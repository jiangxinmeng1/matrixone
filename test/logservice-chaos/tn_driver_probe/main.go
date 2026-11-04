// A standalone TN WAL driver probe for the isolated LogService chaos cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
)

func main() {
	var addresses, mode, prefix, progressFile, stopFile string
	var count, truncateEvery, truncateLag int
	flag.StringVar(&addresses, "addresses", "", "three comma-separated LogService addresses")
	flag.StringVar(&mode, "mode", "cycle", "cycle or replay")
	flag.StringVar(&prefix, "prefix", "driver-probe", "payload prefix")
	flag.StringVar(&progressFile, "progress-file", "", "atomic acknowledged DSN progress file")
	flag.StringVar(&stopFile, "stop-file", "", "stop after current acknowledged entry")
	flag.IntVar(&count, "count", 0, "number of entries; zero means until stop file exists")
	flag.IntVar(&truncateEvery, "truncate-every", 0, "request driver DSN truncate every N acks")
	flag.IntVar(&truncateLag, "truncate-lag", 50, "keep at least this many acknowledged DSNs")
	flag.Parse()
	if addresses != "127.0.0.1:57401,127.0.0.1:57411,127.0.0.1:57421" {
		panic("use only the dedicated three-node driver test cluster")
	}
	if mode != "cycle" && mode != "replay" {
		panic("invalid mode")
	}
	if count < 0 || truncateEvery < 0 || truncateLag < 1 {
		panic("invalid count or truncate setting")
	}

	const sid = "tn-driver-chaos"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	clientCfg := &logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100,
		ServiceAddresses: strings.Split(addresses, ","),
	}
	cfg := logservicedriver.NewConfig(sid,
		logservicedriver.WithConfigOptClientConfig(sid, clientCfg),
		logservicedriver.WithConfigOptMaxClient(2),
		logservicedriver.WithConfigOptClientBufSize(1<<20),
	)
	d := logservicedriver.NewLogServiceDriver(&cfg)
	defer func() {
		if err := d.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "driver close:", err)
			os.Exit(1)
		}
	}()

	var replayCount int
	var previousDSN uint64
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	replayMode := driver.ReplayMode_ReplayForRead
	if mode == "cycle" {
		replayMode = driver.ReplayMode_ReplayForWrite
	}
	if err := d.Replay(ctx,
		func(e *entry.Entry) driver.ReplayEntryState {
			payload := string(e.Entry.GetPayload())
			if e.DSN <= previousDSN || !strings.Contains(payload, "/") {
				panic(fmt.Sprintf("invalid replay entry dsn=%d previous=%d payload=%q", e.DSN, previousDSN, payload))
			}
			previousDSN = e.DSN
			replayCount++
			fmt.Printf("replay dsn=%d payload=%s\n", e.DSN, payload)
			return driver.RE_Nomal
		},
		func() driver.ReplayMode { return replayMode }, nil,
	); err != nil {
		panic(fmt.Sprintf("driver replay: %v", err))
	}
	fmt.Printf("replay_done count=%d max_dsn=%d driver_dsn=%d\n", replayCount, previousDSN, d.GetDSN())
	if mode == "replay" {
		return
	}

	for seq := 0; count == 0 || seq < count; seq++ {
		if stopFile != "" {
			if _, err := os.Stat(stopFile); err == nil {
				fmt.Printf("summary cycles=%d last_dsn=%d\n", seq, d.GetDSN())
				return
			}
		}
		payload := fmt.Sprintf("%s/%06d", prefix, seq)
		e := entry.MockEntryWithPayload([]byte(payload))
		start := time.Now()
		if err := d.Append(e); err != nil {
			panic(fmt.Sprintf("append enqueue seq=%d: %v", seq, err))
		}
		if err := e.WaitDone(); err != nil {
			panic(fmt.Sprintf("append completion seq=%d dsn=%d: %v", seq, e.DSN, err))
		}
		dsn := e.DSN
		e.Entry.Free()
		fmt.Printf("ack seq=%d dsn=%d payload=%s elapsed_ms=%.3f\n",
			seq, dsn, payload, float64(time.Since(start).Microseconds())/1000)
		if progressFile != "" {
			tmp := progressFile + ".tmp"
			if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d %d\n", seq, dsn)), 0o644); err != nil {
				panic(err)
			}
			if err := os.Rename(tmp, progressFile); err != nil {
				panic(err)
			}
		}
		if truncateEvery > 0 && (seq+1)%truncateEvery == 0 && dsn > uint64(truncateLag) {
			target := dsn - uint64(truncateLag)
			if err := d.Truncate(target); err != nil {
				panic(fmt.Sprintf("truncate dsn=%d: %v", target, err))
			}
			fmt.Printf("truncate_intent dsn=%d\n", target)
		}
	}
	fmt.Printf("summary cycles=%d last_dsn=%d\n", count, d.GetDSN())
}
