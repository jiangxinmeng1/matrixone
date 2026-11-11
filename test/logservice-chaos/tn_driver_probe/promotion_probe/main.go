// Exercise the TN WAL replay-forever to replay-for-write handoff against LogService.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

func main() {
	addresses := flag.String("addresses", "", "isolated cluster RPC addresses")
	prefix := flag.String("prefix", "promotion", "unique user WAL payload prefix")
	readyFile := flag.String("ready-file", "", "created after replay applies ready-lsn")
	readyLSN := flag.Uint64("ready-lsn", 1, "create ready file after replay applies at least this user LSN")
	promoteFile := flag.String("promote-file", "", "create to promote replay to write mode")
	count := flag.Int("count", 100, "number of user WAL records after promotion")
	flag.Parse()
	if *addresses != "198.18.51.10:60201,198.18.51.11:60211,198.18.51.12:60221" || *readyFile == "" || *promoteFile == "" || *count <= 0 || *count > 1000 {
		panic("use only isolated TN WAL test cluster")
	}
	const sid = "tn-wal-promotion-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cfg := logservice.ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ",")}
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		return logservice.NewClient(ctx, sid, cfg)
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close:", err)
			os.Exit(2)
		}
	}()
	var mode atomic.Int32
	mode.Store(int32(driver.ReplayMode_ReplayForever))
	var ready sync.Once
	var lastApplied atomic.Uint64
	promoteDone := make(chan struct{})
	go func() {
		defer close(promoteDone)
		for {
			if _, err := os.Stat(*promoteFile); err == nil {
				mode.Store(int32(driver.ReplayMode_ReplayForWrite))
				fmt.Println("promotion_requested")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	start := time.Now()
	if err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		if group == wal.GroupUserTxn {
			lastApplied.Store(lsn)
			if lsn >= *readyLSN {
				ready.Do(func() {
					if err := os.WriteFile(*readyFile, []byte(fmt.Sprintf("replayed_lsn=%d\n", lsn)), 0o644); err != nil {
						panic(err)
					}
				})
			}
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode(mode.Load()) }, nil); err != nil {
		panic(fmt.Sprintf("replay failed after %s: %v", time.Since(start), err))
	}
	<-promoteDone
	fmt.Printf("promotion_done elapsed_ms=%.3f user_lsn=%d checkpointed=%d replay_last=%d\n", float64(time.Since(start).Microseconds())/1000, w.GetLSNWatermark(), w.GetCheckpointed(), lastApplied.Load())
	for seq := 0; seq < *count; seq++ {
		payload := fmt.Sprintf("%s/%06d", *prefix, seq)
		e := entry.GetBase()
		e.SetType(entry.IOET_WALEntry_Test)
		if err := e.SetPayload([]byte(payload)); err != nil {
			panic(err)
		}
		start := time.Now()
		lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
		if err == nil {
			err = e.WaitDone()
		}
		if err != nil {
			panic(fmt.Sprintf("append seq=%d lsn=%d: %v", seq, lsn, err))
		}
		fmt.Printf("ack lsn=%d payload=%s elapsed_ms=%.3f\n", lsn, payload, float64(time.Since(start).Microseconds())/1000)
	}
	latest := w.GetLSNWatermark()
	if latest <= 20 {
		return
	}
	end := latest - 20
	cp, err := w.RangeCheckpoint(1, end, "probe-checkpoint/"+*prefix)
	if err == nil {
		err = cp.WaitDone()
	}
	if err != nil {
		panic(fmt.Sprintf("checkpoint end=%d: %v", end, err))
	}
	fmt.Printf("checkpoint_ack end_lsn=%d\n", end)
}
