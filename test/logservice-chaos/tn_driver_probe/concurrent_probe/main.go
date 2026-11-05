// A single TN WAL handle with concurrent appenders for isolated LogService tests.
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
	addresses := flag.String("addresses", "", "three dedicated test LogService RPC addresses")
	prefix := flag.String("prefix", "concurrent", "unique payload prefix")
	workers := flag.Int("workers", 32, "concurrent appenders")
	perWorker := flag.Int("per-worker", 30, "entries per appender")
	delay := flag.Duration("delay", 20*time.Millisecond, "delay between entries per appender")
	flag.Parse()
	if *addresses != "127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221" ||
		*workers <= 0 || *workers > 100 || *perWorker <= 0 || *delay < 0 {
		panic("invalid isolated cluster or workload settings")
	}

	const sid = "tn-wal-concurrent-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	clientCfg := logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ","),
	}
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return logservice.NewClient(ctx, sid, clientCfg)
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "wal close:", err)
			os.Exit(1)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		if group == wal.GroupUserTxn {
			fmt.Printf("replay lsn=%d payload=%s\n", lsn, payload)
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil); err != nil {
		panic(err)
	}
	fmt.Printf("ready lsn=%d checkpointed=%d\n", w.GetLSNWatermark(), w.GetCheckpointed())

	var acked atomic.Int64
	var wg sync.WaitGroup
	errors := make(chan error, *workers)
	start := make(chan struct{})
	for worker := 0; worker < *workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for seq := 0; seq < *perWorker; seq++ {
				payload := fmt.Sprintf("%s/%02d/%04d", *prefix, worker, seq)
				e := entry.GetBase()
				e.SetType(entry.IOET_WALEntry_Test)
				if err := e.SetPayload([]byte(payload)); err != nil {
					errors <- err
					return
				}
				began := time.Now()
				lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
				if err == nil {
					err = e.WaitDone()
				}
				if err != nil {
					errors <- fmt.Errorf("worker=%d seq=%d lsn=%d: %w", worker, seq, lsn, err)
					return
				}
				count := acked.Add(1)
				fmt.Printf("ack count=%d lsn=%d payload=%s elapsed_ms=%.3f\n",
					count, lsn, payload, float64(time.Since(began).Microseconds())/1000)
				if *delay > 0 {
					time.Sleep(*delay)
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		fmt.Fprintln(os.Stderr, "append error:", err)
	}
	fmt.Printf("summary acked=%d expected=%d user_lsn=%d\n", acked.Load(), *workers**perWorker, w.GetLSNWatermark())
	if acked.Load() != int64(*workers**perWorker) {
		os.Exit(2)
	}
}
