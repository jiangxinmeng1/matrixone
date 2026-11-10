// Exercise a real TN WAL handle closing with an accepted Append in flight.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

type appendBarrierClient struct {
	logservice.Client
	once           *sync.Once
	ready, release string
}

func (c *appendBarrierClient) Append(ctx context.Context, rec logservice.LogRecord) (logservice.Lsn, error) {
	c.once.Do(func() {
		if err := os.WriteFile(c.ready, []byte("ready\n"), 0o644); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(c.release); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	start := time.Now()
	lsn, err := c.Client.Append(ctx, rec)
	fmt.Printf("backend_append lsn=%d elapsed_ms=%.3f err=%v\n", lsn, float64(time.Since(start).Microseconds())/1000, err)
	return lsn, err
}

func main() {
	addresses := flag.String("addresses", "", "dedicated test RPC addresses")
	prefix := flag.String("prefix", "close-probe", "unique payload")
	count := flag.Int("count", 1, "accepted user WAL entries before close")
	appendReady := flag.String("append-ready-file", "", "backend Append barrier ready")
	appendRelease := flag.String("append-release-file", "", "backend Append barrier release")
	closeReady := flag.String("close-ready-file", "", "close started signal")
	flag.Parse()
	if *addresses != "198.18.51.10:58201,198.18.51.11:58211,198.18.51.12:58221" ||
		*count <= 0 || *count > 5000 || *appendReady == "" || *appendRelease == "" || *closeReady == "" {
		panic("use only the isolated WAL test cluster")
	}
	const sid = "tn-wal-close-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cfg := logservice.ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ",")}
	var once sync.Once
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		c, err := logservice.NewClient(ctx, sid, cfg)
		if err != nil {
			return nil, err
		}
		return &appendBarrierClient{Client: c, once: &once, ready: *appendReady, release: *appendRelease}, nil
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil); err != nil {
		panic(err)
	}
	fmt.Printf("replay_done user_lsn=%d checkpointed=%d\n", w.GetLSNWatermark(), w.GetCheckpointed())
	type waitResult struct {
		lsn uint64
		err error
	}
	waitDone := make(chan waitResult, *count)
	for seq := 0; seq < *count; seq++ {
		payload := *prefix
		if *count > 1 {
			payload = fmt.Sprintf("%s/%06d", *prefix, seq)
		}
		e := entry.GetBase()
		e.SetType(entry.IOET_WALEntry_Test)
		if err := e.SetPayload([]byte(payload)); err != nil {
			panic(err)
		}
		lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
		if err != nil {
			panic(err)
		}
		fmt.Printf("accepted lsn=%d payload=%s\n", lsn, payload)
		go func() { waitDone <- waitResult{lsn: lsn, err: e.WaitDone()} }()
	}
	for {
		if _, err := os.Stat(*appendReady); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	closeDone := make(chan error, 1)
	start := time.Now()
	go func() { closeDone <- w.Close() }()
	if err := os.WriteFile(*closeReady, []byte("ready\n"), 0o644); err != nil {
		panic(err)
	}
	cerr := <-closeDone
	fmt.Printf("close_done elapsed_ms=%.3f err=%v\n", float64(time.Since(start).Microseconds())/1000, cerr)
	failed := cerr != nil
	for i := 0; i < *count; i++ {
		result := <-waitDone
		fmt.Printf("wait_done lsn=%d err=%v\n", result.lsn, result.err)
		failed = failed || result.err != nil
	}
	if failed {
		os.Exit(2)
	}
}
