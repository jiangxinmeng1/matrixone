// A bounded large-record probe through TN's actual WAL handle.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

func main() {
	addresses := flag.String("addresses", "", "three test addresses")
	mode := flag.String("mode", "replay", "write, replay, or checkpoint")
	sizeMiB := flag.Int("size-mib", 0, "payload size in MiB, up to 95")
	tag := flag.String("tag", "large-wal", "record tag")
	checkpointThrough := flag.Uint64("checkpoint-through", 0, "last checkpointed user LSN")
	flag.Parse()
	if *addresses != "127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221" ||
		(*mode != "write" && *mode != "replay" && *mode != "checkpoint") ||
		*sizeMiB < 0 || *sizeMiB > 95 {
		panic("invalid dedicated-cluster arguments")
	}
	const sid = "large-tn-wal-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cfg := logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100,
		ServiceAddresses: strings.Split(*addresses, ","),
	}
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return logservice.NewClient(ctx, sid, cfg)
	}
	w := wal.NewLogserviceHandle(logservicedriver.LogServiceClientFactory(factory), 0)
	defer func() {
		if err := w.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close:", err)
			os.Exit(1)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	count := 0
	started := time.Now()
	err := w.Replay(ctx, func(group uint32, lsn uint64, payload []byte, _ uint16, _ any) driver.ReplayEntryState {
		if group == wal.GroupUserTxn && len(payload) > 1<<20 {
			count++
			fmt.Printf("large_replay lsn=%d bytes=%d sha256=%x header=%q\n",
				lsn, len(payload), sha256.Sum256(payload), payload[:min(len(payload), 32)])
		}
		return driver.RE_Nomal
	}, func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite }, nil)
	if err != nil {
		panic(fmt.Sprintf("replay after %s: %v", time.Since(started), err))
	}
	fmt.Printf("replay_done count=%d watermark=%d checkpointed=%d elapsed=%s\n",
		count, w.GetLSNWatermark(), w.GetCheckpointed(), time.Since(started))
	if *mode == "replay" {
		return
	}
	if *mode == "checkpoint" {
		if *checkpointThrough == 0 || *checkpointThrough > w.GetLSNWatermark() {
			panic("invalid checkpoint watermark")
		}
		e, err := w.RangeCheckpoint(1, *checkpointThrough,
			fmt.Sprintf("large-wal-probe/checkpoint/%d", *checkpointThrough))
		if err != nil {
			panic(err)
		}
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		fmt.Printf("checkpoint_ack through=%d checkpointed=%d\n",
			*checkpointThrough, w.GetCheckpointed())
		return
	}
	size := *sizeMiB << 20
	payload := make([]byte, size)
	copy(payload, *tag)
	for i := len(*tag); i < len(payload); i++ {
		payload[i] = byte((i*31 + 17) % 251)
	}
	e := entry.GetBase()
	e.SetType(entry.IOET_WALEntry_Test)
	if err := e.SetPayload(payload); err != nil {
		panic(err)
	}
	started = time.Now()
	lsn, err := w.AppendEntry(wal.GroupUserTxn, e)
	if err != nil {
		panic(err)
	}
	if err := e.WaitDone(); err != nil {
		panic(fmt.Sprintf("wait lsn=%d after %s: %v", lsn, time.Since(started), err))
	}
	fmt.Printf("ack lsn=%d bytes=%d sha256=%x elapsed=%s\n",
		lsn, len(payload), sha256.Sum256(payload), time.Since(started))
}
