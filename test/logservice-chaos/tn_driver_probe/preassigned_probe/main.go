// Test the production TN WAL handle with all user LSNs assigned before backend RPCs are released.
package main

import (
	"bytes"
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
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

type gatedClient struct {
	logservice.Client
	enabled      *atomic.Bool
	calls        *atomic.Uint64
	gate         <-chan struct{}
	firstReady   string
	firstRelease string
}

func recordUserLSNs(rec logservice.LogRecord) []uint64 {
	if len(rec.Data) < 12+logservicedriver.EmptyLogEntrySize {
		return nil
	}
	batch := logservicedriver.LogEntry(rec.Data[12:])
	if batch.GetCmdType() != uint16(logservicedriver.Cmd_Normal) {
		return nil
	}
	var lsns []uint64
	for i := 0; i < int(batch.GetEntryCount()); i++ {
		e := driverEntry.NewEmptyEntry()
		if _, err := e.UnmarshalBinary(batch.GetEntry(i)); err != nil {
			panic(err)
		}
		if e.Info.Group == wal.GroupUserTxn {
			lsns = append(lsns, e.Info.GroupLSN)
		}
	}
	return lsns
}

func (c *gatedClient) AppendWAL(ctx context.Context, rec logservice.LogRecord) (uint64, error) {
	active := c.enabled.Load()
	call := uint64(0)
	if active {
		call = c.calls.Add(1)
		<-c.gate
		if call == 1 && c.firstReady != "" {
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
	}
	backend, ok := c.Client.(interface {
		AppendWAL(context.Context, logservice.LogRecord) (uint64, error)
	})
	if !ok {
		return 0, fmt.Errorf("underlying client does not implement AppendWAL")
	}
	started := time.Now()
	psn, err := backend.AppendWAL(ctx, rec)
	if active {
		fmt.Printf("backend_done call=%d psn=%d lsns=%v elapsed_ms=%.3f err=%v\n",
			call, psn, recordUserLSNs(rec), float64(time.Since(started).Microseconds())/1000, err)
	}
	return psn, err
}

func main() {
	addresses := flag.String("addresses", "", "isolated three-node LogService addresses")
	mode := flag.String("mode", "replay", "write, replay, checkpoint, or scan")
	prefix := flag.String("prefix", "preassigned", "payload prefix to verify")
	count := flag.Int("count", 64, "number of sequentially preassigned user WAL entries")
	sizeMiB := flag.Int("size-mib", 3, "size per user WAL entry in MiB")
	assignedReady := flag.String("assigned-ready-file", "", "signal after all user LSNs are assigned")
	assignedRelease := flag.String("assigned-release-file", "", "release all backend RPCs")
	firstReady := flag.String("first-ready-file", "", "signal when the first backend call is held")
	firstRelease := flag.String("first-release-file", "", "release the first backend call")
	checkpointThrough := flag.Uint64("checkpoint-through", 0, "checkpoint through this user LSN")
	scanMin := flag.Uint64("scan-min", 0, "first physical LSN to print in scan mode")
	scanMax := flag.Uint64("scan-max", 0, "last physical LSN to print in scan mode")
	flag.Parse()
	if *addresses != "198.18.51.10:60201,198.18.51.11:60211,198.18.51.12:60221" ||
		(*mode != "write" && *mode != "replay" && *mode != "checkpoint" && *mode != "scan") ||
		*count <= 0 || *count > 96 || *sizeMiB <= 0 || *sizeMiB > 4 ||
		(*assignedReady == "") != (*assignedRelease == "") ||
		(*firstReady == "") != (*firstRelease == "") ||
		(*mode == "checkpoint" && *checkpointThrough == 0) ||
		(*scanMin == 0) != (*scanMax == 0) ||
		(*scanMax != 0 && *scanMax < *scanMin) {
		panic("invalid isolated WAL probe parameters")
	}
	const sid = "tn-preassigned-wal-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	config := logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ","),
	}
	if *mode == "scan" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		client, err := logservice.NewClient(ctx, sid, config)
		cancel()
		if err != nil {
			panic(err)
		}
		defer client.Close()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		first, err := client.GetTruncatedLsn(ctx)
		cancel()
		if err != nil {
			panic(err)
		}
		first++
		var total, mismatch uint64
		for {
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			records, next, err := client.Read(ctx, first, 4<<20)
			cancel()
			if err != nil {
				panic(err)
			}
			for i, record := range records {
				total++
				computed := first + uint64(i)
				if *scanMin != 0 && record.Lsn >= *scanMin && record.Lsn <= *scanMax {
					fmt.Printf("psn_record batch_first=%d index=%d computed=%d actual=%d type=%d",
						first, i, computed, record.Lsn, record.Type)
					if len(record.Data) >= 12+logservicedriver.EmptyLogEntrySize {
						batch := logservicedriver.LogEntry(record.Data[12:])
						fmt.Printf(" cmd=%d start_dsn=%d entries=%d", batch.GetCmdType(), batch.GetStartDSN(), batch.GetEntryCount())
					}
					fmt.Println()
				}
				if record.Lsn != computed {
					mismatch++
					if mismatch <= 20 {
						fmt.Printf("psn_mismatch batch_first=%d index=%d computed=%d actual=%d type=%d\n",
							first, i, computed, record.Lsn, record.Type)
					}
				}
			}
			if next == first {
				fmt.Printf("scan_done records=%d psn_mismatch=%d\n", total, mismatch)
				return
			}
			first = next
		}
	}
	gate := make(chan struct{})
	var enabled atomic.Bool
	var calls atomic.Uint64
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		client, err := logservice.NewClient(ctx, sid, config)
		if err != nil {
			return nil, err
		}
		return &gatedClient{Client: client, enabled: &enabled, calls: &calls, gate: gate,
			firstReady: *firstReady, firstRelease: *firstRelease}, nil
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "wal close:", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	seenPrefix := []byte(*prefix + "/")
	var replayed uint64
	err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		if group == wal.GroupUserTxn && bytes.HasPrefix(payload, seenPrefix) {
			replayed++
			fmt.Printf("replay lsn=%d bytes=%d sha256=%x\n", lsn, len(payload), sha256.Sum256(payload))
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("replay_done count=%d user_lsn=%d checkpointed=%d\n",
		replayed, w.GetLSNWatermark(), w.GetCheckpointed())
	if *mode == "replay" {
		return
	}
	if *mode == "checkpoint" {
		if *checkpointThrough > w.GetLSNWatermark() {
			panic("checkpoint beyond user watermark")
		}
		e, err := w.RangeCheckpoint(1, *checkpointThrough, fmt.Sprintf("preassigned/%s/checkpoint/%d", *prefix, *checkpointThrough))
		if err != nil {
			panic(err)
		}
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		fmt.Printf("checkpoint_ack through=%d checkpointed=%d\n", *checkpointThrough, w.GetCheckpointed())
		time.Sleep(3 * time.Second)
		return
	}
	enabled.Store(true)
	type assigned struct {
		lsn     uint64
		payload []byte
		e       entry.Entry
	}
	entries := make([]assigned, 0, *count)
	for i := 0; i < *count; i++ {
		payload := make([]byte, *sizeMiB<<20)
		header := fmt.Sprintf("%s/%06d", *prefix, i)
		copy(payload, header)
		for j := len(header); j < len(payload); j++ {
			payload[j] = byte((j*31 + i*17) % 251)
		}
		e := entry.GetBase()
		e.SetType(entry.IOET_WALEntry_Test)
		if err := e.SetPayload(payload); err != nil {
			panic(err)
		}
		lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
		if err != nil {
			panic(err)
		}
		entries = append(entries, assigned{lsn: lsn, payload: payload, e: e})
		fmt.Printf("assigned seq=%d lsn=%d bytes=%d sha256=%x\n", i, lsn, len(payload), sha256.Sum256(payload))
	}
	fmt.Printf("all_assigned count=%d first=%d last=%d backend_done=0\n",
		len(entries), entries[0].lsn, entries[len(entries)-1].lsn)
	if *assignedReady != "" {
		if err := os.WriteFile(*assignedReady, []byte("ready\n"), 0o644); err != nil {
			panic(err)
		}
		for {
			if _, err := os.Stat(*assignedRelease); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	close(gate)
	var wg sync.WaitGroup
	var failed atomic.Bool
	for _, a := range entries {
		wg.Add(1)
		go func(a assigned) {
			defer wg.Done()
			err := a.e.WaitDone()
			if err != nil {
				failed.Store(true)
			}
			fmt.Printf("ack lsn=%d bytes=%d sha256=%x err=%v\n",
				a.lsn, len(a.payload), sha256.Sum256(a.payload), err)
		}(a)
	}
	wg.Wait()
	fmt.Printf("write_done count=%d user_lsn=%d backend_calls=%d\n", len(entries), w.GetLSNWatermark(), calls.Load())
	if failed.Load() {
		panic("one or more assigned WAL entries did not commit")
	}
}
