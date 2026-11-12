// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A small, direct Logservice probe for the isolated three-node fault test.
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
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func main() {
	runtime.SetupServiceBasedRuntime("logservice-chaos", runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime("logservice-chaos", runtime.DefaultRuntime())
	var addresses, operation, prefix, progressFile, stopFile, readReadyFile, readReleaseFile string
	var count, pauseReadSeq int
	var first, lsn, shard, checkpointedDSN uint64
	var tryTimeout, maxRetryWindow time.Duration
	flag.StringVar(&addresses, "addresses", "127.0.0.1:32001", "comma-separated Logservice RPC addresses")
	flag.StringVar(&operation, "op", "status", "status, append, append-wal, load, cycle, read, truncate, wal-truncate, wal-state, truncated, or latest")
	flag.StringVar(&prefix, "prefix", "probe", "unique append payload prefix")
	flag.IntVar(&count, "count", 1, "append count")
	flag.Uint64Var(&first, "first", 1, "first LSN for read")
	flag.Uint64Var(&lsn, "lsn", 0, "LSN for truncate")
	flag.Uint64Var(&checkpointedDSN, "checkpointed-dsn", 0, "durable TN DSN for WAL truncate")
	flag.Uint64Var(&shard, "shard", 1, "shard ID for status")
	flag.DurationVar(&tryTimeout, "try-timeout", time.Minute, "per-attempt timeout for load, matching MO WAL default")
	flag.DurationVar(&maxRetryWindow, "retry-window", 3*time.Minute, "retry continuation window for load, matching MO WAL default")
	flag.StringVar(&progressFile, "progress-file", "", "atomically updated verified LSN for cycle")
	flag.StringVar(&stopFile, "stop-file", "", "stop cycle after the current verified record when this file exists")
	flag.IntVar(&pauseReadSeq, "pause-read-seq", -1, "cycle sequence to pause after Append and before Read")
	flag.StringVar(&readReadyFile, "read-ready-file", "", "write the acknowledged LSN here before a paused Read")
	flag.StringVar(&readReleaseFile, "read-release-file", "", "resume the paused Read when this file exists")
	flag.Parse()
	targets := strings.Split(addresses, ",")
	if operation == "status" {
		for _, address := range targets {
			info, ok, err := logservice.GetShardInfo("logservice-chaos", address, shard)
			fmt.Printf("address=%s ok=%v leader=%d replicas=%v err=%v\n", address, ok, info.ReplicaID, info.Replicas, err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := logservice.NewClient(ctx, "logservice-chaos", logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: targets,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer client.Close()
	switch operation {
	case "append", "append-wal":
		for i := 0; i < count; i++ {
			payload := []byte(fmt.Sprintf("%s/%06d", prefix, i))
			record := pb.NewUserLogRecord(100, len(payload))
			record.SetPayload(payload)
			requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
			var index uint64
			var appendErr error
			if operation == "append-wal" {
				walClient, ok := client.(interface {
					AppendWAL(context.Context, logservice.LogRecord) (uint64, error)
				})
				if !ok {
					panic("LogService client does not implement AppendWAL")
				}
				index, appendErr = walClient.AppendWAL(requestCtx, record)
			} else {
				index, appendErr = client.Append(requestCtx, record)
			}
			requestCancel()
			fmt.Printf("append payload=%s lsn=%d err=%v\n", payload, index, appendErr)
			if appendErr != nil {
				os.Exit(1)
			}
		}
	case "load":
		for i := 0; i < count; i++ {
			payload := []byte(fmt.Sprintf("%s/%06d", prefix, i))
			record := pb.NewUserLogRecord(100, len(payload))
			record.SetPayload(payload)
			started := time.Now()
			for attempt := 1; ; attempt++ {
				attemptStarted := time.Now()
				requestCtx, requestCancel := context.WithTimeout(context.Background(), tryTimeout)
				index, appendErr := client.Append(requestCtx, record)
				requestCancel()
				fmt.Printf("try time=%s payload=%s attempt=%d latency_ms=%.3f lsn=%d err=%q\n",
					time.Now().Format(time.RFC3339Nano), payload, attempt,
					float64(time.Since(attemptStarted).Microseconds())/1000, index, fmt.Sprint(appendErr))
				if appendErr == nil {
					fmt.Printf("ack time=%s payload=%s lsn=%d attempts=%d total_ms=%.3f\n",
						time.Now().Format(time.RFC3339Nano), payload, index, attempt,
						float64(time.Since(started).Microseconds())/1000)
					break
				}
				if time.Since(started) > maxRetryWindow {
					fmt.Printf("final_error time=%s payload=%s attempts=%d total_ms=%.3f err=%q\n",
						time.Now().Format(time.RFC3339Nano), payload, attempt,
						float64(time.Since(started).Microseconds())/1000, fmt.Sprint(appendErr))
					os.Exit(1)
				}
				interval := 100 * time.Millisecond
				for step := 0; step < attempt && interval < 4*time.Second; step++ {
					interval *= 2
				}
				time.Sleep(interval)
			}
		}
	case "cycle":
		err = runCycle(client, prefix, count, tryTimeout, maxRetryWindow, progressFile, stopFile,
			pauseReadSeq, readReadyFile, readReleaseFile)
		if err != nil {
			fmt.Printf("final_error time=%s err=%q\n", time.Now().Format(time.RFC3339Nano), err.Error())
		}
	case "read":
		for {
			requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
			records, next, readErr := client.Read(requestCtx, first, 1<<20)
			requestCancel()
			if readErr != nil {
				fmt.Fprintln(os.Stderr, "read:", readErr)
				os.Exit(1)
			}
			for _, record := range records {
				if record.Type == pb.UserRecord {
					fmt.Printf("read lsn=%d payload=%s\n", record.Lsn, record.Data[12:])
				}
			}
			if next == first {
				return
			}
			first = next
		}
	case "truncate":
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = client.Truncate(requestCtx, lsn)
		requestCancel()
		fmt.Printf("truncate lsn=%d err=%v\n", lsn, err)
	case "wal-truncate":
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = client.TruncateWAL(requestCtx, lsn, checkpointedDSN)
		requestCancel()
		fmt.Printf("wal-truncate psn=%d checkpointed_dsn=%d err=%v\n", lsn, checkpointedDSN, err)
	case "wal-state":
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		psn, dsn, getErr := client.GetWALTruncate(requestCtx)
		requestCancel()
		fmt.Printf("wal-state psn=%d checkpointed_dsn=%d err=%v\n", psn, dsn, getErr)
		err = getErr
	case "truncated":
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		index, getErr := client.GetTruncatedLsn(requestCtx)
		requestCancel()
		fmt.Printf("truncated=%d err=%v\n", index, getErr)
		err = getErr
	case "latest":
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		index, getErr := client.GetLatestLsn(requestCtx)
		requestCancel()
		fmt.Printf("latest=%d err=%v\n", index, getErr)
		err = getErr
	default:
		err = fmt.Errorf("unknown operation %q", operation)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
