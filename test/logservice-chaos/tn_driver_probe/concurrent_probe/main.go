// A single TN WAL handle with concurrent appenders for isolated LogService tests.
package main

import (
	"context"
	"crypto/sha256"
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

type tracedClient struct {
	logservice.Client
	firstAppend  *atomic.Bool
	firstReady   string
	firstRelease string
}

func (c *tracedClient) Append(ctx context.Context, rec logservice.LogRecord) (logservice.Lsn, error) {
	start := time.Now()
	lsn, err := c.Client.Append(ctx, rec)
	if err != nil || time.Since(start) > 3*time.Second {
		fmt.Printf("backend_append lsn=%d elapsed_ms=%.3f err=%v\n",
			lsn, float64(time.Since(start).Microseconds())/1000, err)
	}
	return lsn, err
}

func (c *tracedClient) AppendWAL(ctx context.Context, rec logservice.LogRecord) (logservice.Lsn, error) {
	if c.firstReady != "" && !c.firstAppend.Swap(true) {
		if err := os.WriteFile(c.firstReady, []byte("ready\n"), 0o644); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(c.firstRelease); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	start := time.Now()
	walClient, ok := c.Client.(interface {
		AppendWAL(context.Context, logservice.LogRecord) (uint64, error)
	})
	if !ok {
		return 0, fmt.Errorf("underlying LogService client does not implement AppendWAL")
	}
	lsn, err := walClient.AppendWAL(ctx, rec)
	if err != nil || time.Since(start) > 3*time.Second {
		fmt.Printf("backend_append_wal lsn=%d elapsed_ms=%.3f err=%v\n",
			lsn, float64(time.Since(start).Microseconds())/1000, err)
	}
	return lsn, err
}

func main() {
	addresses := flag.String("addresses", "", "three dedicated test LogService RPC addresses")
	mode := flag.String("mode", "write", "write or replay")
	prefix := flag.String("prefix", "concurrent", "unique payload prefix")
	largeEvery := flag.Int("large-every", 0, "make every Nth entry per worker large; zero disables")
	largeMiB := flag.Int("large-mib", 0, "size of large payload in MiB")
	workers := flag.Int("workers", 32, "concurrent appenders")
	perWorker := flag.Int("per-worker", 30, "entries per appender")
	delay := flag.Duration("delay", 20*time.Millisecond, "delay between entries per appender")
	checkpointEvery := flag.Int("checkpoint-every-acks", 0, "issue a checkpoint after each N acknowledged entries; zero disables")
	checkpointLag := flag.Uint64("checkpoint-lag", 100, "keep this many contiguous acknowledged user LSNs")
	checkpointFiles := flag.Bool("checkpoint-files", false, "include a GroupFiles marker as TN does")
	startReady := flag.String("start-ready-file", "", "write this file after replay and before worker Append calls")
	startRelease := flag.String("start-release-file", "", "wait for this file before worker Append calls")
	traceBackend := flag.Bool("trace-backend-append", false, "log slow or failed individual LogService Append attempts")
	firstReady := flag.String("first-append-ready-file", "", "signal before the first backend AppendWAL")
	firstRelease := flag.String("first-append-release-file", "", "release the first backend AppendWAL")
	flag.Parse()
	if (*addresses != "127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221" &&
		*addresses != "198.18.51.10:58201,198.18.51.11:58211,198.18.51.12:58221" &&
		*addresses != "198.18.51.10:59601,198.18.51.11:59611,198.18.51.12:59621" &&
		*addresses != "198.18.51.10:59801,198.18.51.11:59811,198.18.51.12:59821" &&
		*addresses != "198.18.51.10:60201,198.18.51.11:60211,198.18.51.12:60221") ||
		(*mode != "write" && *mode != "replay") ||
		*workers <= 0 || *workers > 100 || *perWorker <= 0 || *delay < 0 || *checkpointEvery < 0 ||
		*largeEvery < 0 || *largeMiB < 0 || *largeMiB > 32 || (*largeEvery == 0) != (*largeMiB == 0) ||
		(*startReady == "") != (*startRelease == "") ||
		(*firstReady == "") != (*firstRelease == "") {
		panic("invalid isolated cluster or workload settings")
	}

	const sid = "tn-wal-concurrent-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	clientCfg := logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ","),
	}
	var firstAppend atomic.Bool
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		client, err := logservice.NewClient(ctx, sid, clientCfg)
		if err != nil || (!*traceBackend && *firstReady == "") {
			return client, err
		}
		return &tracedClient{Client: client, firstAppend: &firstAppend, firstReady: *firstReady, firstRelease: *firstRelease}, nil
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
			if *largeEvery > 0 || *mode == "replay" {
				fmt.Printf("replay lsn=%d bytes=%d sha256=%x\n", lsn, len(payload), sha256.Sum256(payload))
			} else {
				fmt.Printf("replay lsn=%d payload=%s\n", lsn, payload)
			}
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil); err != nil {
		panic(err)
	}
	fmt.Printf("ready lsn=%d checkpointed=%d\n", w.GetLSNWatermark(), w.GetCheckpointed())
	if *mode == "replay" {
		return
	}

	var acked atomic.Int64
	var ackMu sync.Mutex
	ackLSNs := make(map[uint64]struct{})
	contiguous := w.GetLSNWatermark()
	workersDone := make(chan struct{})
	checkpointDone := make(chan error, 1)
	if *checkpointEvery > 0 {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			last := w.GetCheckpointed()
			nextCount := int64(*checkpointEvery)
			for {
				finished := false
				select {
				case <-ticker.C:
				case <-workersDone:
					finished = true
				}
				count := acked.Load()
				if count >= nextCount || finished {
					ackMu.Lock()
					end := contiguous
					ackMu.Unlock()
					if end > *checkpointLag {
						end -= *checkpointLag
						if end > last {
							var files []string
							if *checkpointFiles {
								files = []string{fmt.Sprintf("probe-checkpoint/%s/%d", *prefix, end)}
							}
							began := time.Now()
							e, err := w.RangeCheckpoint(1, end, files...)
							if err == nil {
								err = e.WaitDone()
							}
							if err != nil {
								checkpointDone <- fmt.Errorf("checkpoint end=%d: %w", end, err)
								return
							}
							fmt.Printf("checkpoint_ack end_lsn=%d ack_count=%d elapsed_ms=%.3f\n",
								end, count, float64(time.Since(began).Microseconds())/1000)
							last = end
						}
					}
					nextCount = count + int64(*checkpointEvery)
				}
				if finished {
					checkpointDone <- nil
					return
				}
			}
		}()
	}
	var wg sync.WaitGroup
	errors := make(chan error, *workers)
	start := make(chan struct{})
	for worker := 0; worker < *workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for seq := 0; seq < *perWorker; seq++ {
				payload := []byte(fmt.Sprintf("%s/%02d/%04d", *prefix, worker, seq))
				if *largeEvery > 0 && seq%*largeEvery == 0 {
					large := make([]byte, *largeMiB<<20)
					copy(large, payload)
					for i := len(payload); i < len(large); i++ {
						large[i] = byte((i*31 + worker*17 + seq) % 251)
					}
					payload = large
				}
				e := entry.GetBase()
				e.SetType(entry.IOET_WALEntry_Test)
				if err := e.SetPayload(payload); err != nil {
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
				ackMu.Lock()
				ackLSNs[lsn] = struct{}{}
				for {
					if _, ok := ackLSNs[contiguous+1]; !ok {
						break
					}
					contiguous++
					delete(ackLSNs, contiguous)
				}
				ackMu.Unlock()
				count := acked.Add(1)
				if *largeEvery > 0 {
					fmt.Printf("ack count=%d lsn=%d bytes=%d sha256=%x elapsed_ms=%.3f\n",
						count, lsn, len(payload), sha256.Sum256(payload), float64(time.Since(began).Microseconds())/1000)
				} else {
					fmt.Printf("ack count=%d lsn=%d payload=%s elapsed_ms=%.3f\n",
						count, lsn, payload, float64(time.Since(began).Microseconds())/1000)
				}
				if *delay > 0 {
					time.Sleep(*delay)
				}
			}
		}(worker)
	}
	if *startReady != "" {
		if err := os.WriteFile(*startReady, []byte("ready\n"), 0o644); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(*startRelease); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	close(start)
	wg.Wait()
	close(workersDone)
	var checkpointErr error
	if *checkpointEvery > 0 {
		checkpointErr = <-checkpointDone
		if checkpointErr != nil {
			fmt.Fprintln(os.Stderr, "checkpoint error:", checkpointErr)
		}
	}
	close(errors)
	for err := range errors {
		fmt.Fprintln(os.Stderr, "append error:", err)
	}
	fmt.Printf("summary acked=%d expected=%d user_lsn=%d contiguous=%d\n",
		acked.Load(), *workers**perWorker, w.GetLSNWatermark(), contiguous)
	if checkpointErr != nil || acked.Load() != int64(*workers**perWorker) {
		os.Exit(2)
	}
}
