// A standalone probe using the same WAL StoreImpl entry points as TN TAE.
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

type faultHookClient struct {
	logservice.Client
	once            *sync.Once
	ready           string
	release         string
	truncateOnce    *sync.Once
	truncateReady   string
	truncateRelease string
}

func (c *faultHookClient) GetTruncatedLsn(ctx context.Context) (logservice.Lsn, error) {
	lsn, err := c.Client.GetTruncatedLsn(ctx)
	if err == nil && c.ready != "" {
		c.once.Do(func() {
			if err := os.WriteFile(c.ready, []byte(fmt.Sprintf("%d\n", lsn)), 0o644); err != nil {
				panic(err)
			}
			for {
				if _, err := os.Stat(c.release); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	return lsn, err
}

func (c *faultHookClient) Truncate(ctx context.Context, lsn logservice.Lsn) error {
	if c.truncateReady != "" {
		c.truncateOnce.Do(func() {
			if err := os.WriteFile(c.truncateReady, []byte(fmt.Sprintf("%d\n", lsn)), 0o644); err != nil {
				panic(err)
			}
			for {
				if _, err := os.Stat(c.truncateRelease); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	return c.Client.Truncate(ctx, lsn)
}

func main() {
	var addresses, mode, prefix, readyFile, releaseFile, progressFile string
	var checkpointReadyFile, checkpointReleaseFile string
	var replayReadyFile, replayReleaseFile string
	var readReadyFile, readReleaseFile string
	var truncateReadyFile, truncateReleaseFile string
	var count, pauseBeforeSeq, checkpointEvery, pauseAtCheckpoint int
	var checkpointLag uint64
	var writeDelay, replayTimeout time.Duration
	var checkpointFiles bool
	flag.StringVar(&addresses, "addresses", "", "three test LogService addresses")
	flag.StringVar(&mode, "mode", "cycle", "cycle or replay")
	flag.StringVar(&prefix, "prefix", "wal-probe", "unique payload prefix")
	flag.StringVar(&readyFile, "pause-ready-file", "", "write this file before selected AppendEntry")
	flag.StringVar(&releaseFile, "pause-release-file", "", "wait for this file before selected AppendEntry")
	flag.StringVar(&progressFile, "progress-file", "", "atomic last acknowledged seq and LSN")
	flag.IntVar(&count, "count", 0, "number of user entries; zero means continue until killed")
	flag.IntVar(&pauseBeforeSeq, "pause-before-seq", -1, "pause before this AppendEntry")
	flag.Uint64Var(&checkpointLag, "checkpoint-lag", 0, "checkpoint through user LSN latest minus lag")
	flag.IntVar(&checkpointEvery, "checkpoint-every", 0, "checkpoint after every N acknowledged user entries")
	flag.IntVar(&pauseAtCheckpoint, "pause-at-checkpoint", -1, "pause after dispatching checkpoint number N, zero-based")
	flag.StringVar(&checkpointReadyFile, "checkpoint-ready-file", "", "write this file after checkpoint dispatch")
	flag.StringVar(&checkpointReleaseFile, "checkpoint-release-file", "", "wait for this file before checkpoint WaitDone")
	flag.StringVar(&replayReadyFile, "replay-ready-file", "", "write this file after client pool creation, before replay")
	flag.StringVar(&replayReleaseFile, "replay-release-file", "", "wait for this file before replay")
	flag.StringVar(&readReadyFile, "read-ready-file", "", "write this file after first successful GetTruncatedLsn, before replay Read")
	flag.StringVar(&readReleaseFile, "read-release-file", "", "wait for this file before replay Read")
	flag.StringVar(&truncateReadyFile, "truncate-ready-file", "", "write this file before the first physical Truncate RPC")
	flag.StringVar(&truncateReleaseFile, "truncate-release-file", "", "wait for this file before the first physical Truncate RPC")
	flag.DurationVar(&replayTimeout, "replay-timeout", 3*time.Minute, "overall replay context deadline")
	flag.DurationVar(&writeDelay, "write-delay", 0, "delay after each acknowledged user entry")
	flag.BoolVar(&checkpointFiles, "checkpoint-files", false, "append a GroupFiles marker using the TAE checkpoint WAL form")
	flag.Parse()
	if addresses != "127.0.0.1:58001,127.0.0.1:58011,127.0.0.1:58021" &&
		addresses != "127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221" &&
		addresses != "198.18.51.10:58201,198.18.51.11:58211,198.18.51.12:58221" {
		panic("use only the dedicated WAL StoreImpl test cluster")
	}
	if mode != "cycle" && mode != "replay" {
		panic("invalid mode")
	}
	if count < 0 || pauseBeforeSeq < -1 || checkpointEvery < 0 || pauseAtCheckpoint < -1 || writeDelay < 0 || replayTimeout <= 0 ||
		(pauseBeforeSeq >= 0 && (readyFile == "" || releaseFile == "")) ||
		(pauseAtCheckpoint >= 0 && (checkpointReadyFile == "" || checkpointReleaseFile == "")) ||
		((replayReadyFile == "") != (replayReleaseFile == "")) ||
		((readReadyFile == "") != (readReleaseFile == "")) ||
		((truncateReadyFile == "") != (truncateReleaseFile == "")) {
		panic("invalid count or pause settings")
	}

	const sid = "tn-wal-store-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	clientCfg := logservice.ClientConfig{
		LogShardID:       1,
		TNReplicaID:      100,
		ServiceAddresses: strings.Split(addresses, ","),
	}
	var readHookOnce sync.Once
	var truncateHookOnce sync.Once
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		client, err := logservice.NewClient(ctx, sid, clientCfg)
		if err != nil || (readReadyFile == "" && truncateReadyFile == "") {
			return client, err
		}
		return &faultHookClient{
			Client: client, once: &readHookOnce, ready: readReadyFile, release: readReleaseFile,
			truncateOnce: &truncateHookOnce, truncateReady: truncateReadyFile, truncateRelease: truncateReleaseFile,
		}, nil
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "wal close:", err)
			os.Exit(1)
		}
	}()

	if replayReadyFile != "" {
		if err := os.WriteFile(replayReadyFile, []byte("ready\n"), 0o644); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(replayReleaseFile); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	replayStart := time.Now()
	if err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		if group == wal.GroupUserTxn {
			fmt.Printf("replay lsn=%d payload=%s\n", lsn, payload)
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil); err != nil {
		panic(fmt.Sprintf("wal replay after %s: %v", time.Since(replayStart), err))
	}
	fmt.Printf("replay_done user_lsn=%d checkpointed=%d truncated_dsn=%d elapsed_ms=%.3f\n",
		w.GetLSNWatermark(), w.GetCheckpointed(), w.GetTruncated(), float64(time.Since(replayStart).Microseconds())/1000)
	if mode == "replay" {
		return
	}

	checkpointNumber := 0
	checkpoint := func() {
		latest := w.GetLSNWatermark()
		if latest <= checkpointLag {
			return
		}
		end := latest - checkpointLag
		var files []string
		if checkpointFiles {
			files = []string{fmt.Sprintf("probe-checkpoint/%s/%d", prefix, end)}
		}
		e, err := w.RangeCheckpoint(1, end, files...)
		if err != nil {
			panic(fmt.Sprintf("range checkpoint %d: %v", end, err))
		}
		if checkpointNumber == pauseAtCheckpoint {
			if err := os.WriteFile(checkpointReadyFile, []byte(fmt.Sprintf("%d\n", end)), 0o644); err != nil {
				panic(err)
			}
			for {
				if _, err := os.Stat(checkpointReleaseFile); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		checkpointNumber++
		if err := e.WaitDone(); err != nil {
			panic(fmt.Sprintf("checkpoint completion %d: %v", end, err))
		}
		fmt.Printf("checkpoint_ack end_lsn=%d checkpointed=%d truncated_dsn=%d\n",
			end, w.GetCheckpointed(), w.GetTruncated())
	}
	for seq := 0; count == 0 || seq < count; seq++ {
		if seq == pauseBeforeSeq {
			if err := os.WriteFile(readyFile, []byte(fmt.Sprintf("%d %d\n", seq, w.GetLSNWatermark())), 0o644); err != nil {
				panic(err)
			}
			for {
				if _, err := os.Stat(releaseFile); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		payload := fmt.Sprintf("%s/%06d", prefix, seq)
		e := entry.GetBase()
		e.SetType(entry.IOET_WALEntry_Test)
		if err := e.SetPayload([]byte(payload)); err != nil {
			panic(err)
		}
		start := time.Now()
		lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
		if err != nil {
			panic(fmt.Sprintf("wal append seq=%d: %v", seq, err))
		}
		if err := e.WaitDone(); err != nil {
			panic(fmt.Sprintf("wal completion seq=%d lsn=%d: %v", seq, lsn, err))
		}
		fmt.Printf("ack seq=%d lsn=%d payload=%s elapsed_ms=%.3f\n",
			seq, lsn, payload, float64(time.Since(start).Microseconds())/1000)
		if progressFile != "" {
			tmp := progressFile + ".tmp"
			if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d %d\n", seq, lsn)), 0o644); err != nil {
				panic(err)
			}
			if err := os.Rename(tmp, progressFile); err != nil {
				panic(err)
			}
		}
		if checkpointEvery > 0 && (seq+1)%checkpointEvery == 0 {
			checkpoint()
		}
		if writeDelay > 0 {
			time.Sleep(writeDelay)
		}
	}
	if checkpointLag > 0 && checkpointEvery == 0 {
		checkpoint()
	}
	fmt.Printf("summary cycles=%d user_lsn=%d\n", count, w.GetLSNWatermark())
}
