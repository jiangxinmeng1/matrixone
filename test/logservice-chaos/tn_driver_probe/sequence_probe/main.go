// Inspect physical PSN and logical DSN/LSN order before a checkpoint truncation.
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
)

func main() {
	addresses := flag.String("addresses", "", "dedicated test RPC addresses")
	flag.Parse()
	if *addresses != "127.0.0.1:58201,127.0.0.1:58211,127.0.0.1:58221" {
		panic("use only the dedicated WAL test cluster")
	}
	const sid = "tn-wal-sequence-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := logservice.NewClient(ctx, sid, logservice.ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(*addresses, ","),
	})
	cancel()
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	first, err := client.GetTruncatedLsn(ctx)
	cancel()
	if err != nil {
		panic(err)
	}
	first++
	for {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		records, next, err := client.Read(ctx, first, 1<<20)
		cancel()
		if err != nil {
			panic(err)
		}
		for _, record := range records {
			if record.Type != pb.UserRecord {
				continue
			}
			if len(record.Data) < 12+logservicedriver.EmptyLogEntrySize {
				continue
			}
			logEntry := logservicedriver.LogEntry(record.Data[12:])
			if logEntry.GetCmdType() != uint16(logservicedriver.Cmd_Normal) {
				continue
			}
			for i := 0; i < int(logEntry.GetEntryCount()); i++ {
				e := entry.NewEmptyEntry()
				if _, err := e.UnmarshalBinary(logEntry.GetEntry(i)); err != nil {
					panic(err)
				}
				if e.Info.Group == wal.GroupUserTxn {
					fmt.Printf("record psn=%d dsn=%d lsn=%d payload=%s\n", record.Lsn, e.DSN, e.Info.GroupLSN, e.Entry.GetPayload())
				}
			}
		}
		if next == first {
			return
		}
		first = next
	}
}
