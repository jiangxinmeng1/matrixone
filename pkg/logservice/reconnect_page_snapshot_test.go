package logservice

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestPaginatedWALReadAcrossSameTNReconnectsAndSnapshotImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	s, err := NewServiceWithRetry(func() Config { return cfg }, newFS(), nil,
		WithBackendFilter(func(morpc.Message, string) bool { return true }))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, s.store.startHAKeeperReplica(1,
		map[uint64]dragonboat.Target{1: s.ID()}, false))
	require.NoError(t, s.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: s.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := NewClient(ctx, "", ClientConfig{
		LogShardID: 1, TNReplicaID: 100,
		ServiceAddresses: []string{cfg.LogServiceServiceAddr()},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	managed := c.(*managedClient)
	appendPhase := func(phase int) map[uint64][]byte {
		written := make(map[uint64][]byte)
		for i := 0; i < 20; i++ {
			managed.resetClient()
			require.NoError(t, managed.prepareClient(ctx))
			if i%4 != 0 {
				continue
			}
			value := fmt.Sprintf("phase-%d-wal-%02d", phase, i)
			rec := c.GetLogRecord(len(value))
			copy(rec.Payload(), value)
			lsn, err := managed.AppendWAL(ctx, rec)
			require.NoError(t, err)
			written[lsn] = bytes.Clone(rec.Data)
		}
		return written
	}
	checkPages := func(first uint64, written map[uint64][]byte) {
		seen := make(map[uint64]bool)
		cursor := first
		for i := 0; i < 500; i++ {
			page, next, err := c.Read(ctx, cursor, 1)
			require.NoError(t, err)
			for _, rec := range page {
				if want, ok := written[rec.Lsn]; ok {
					require.Equal(t, pb.UserRecord, rec.Type)
					require.Equal(t, want, rec.Data)
					seen[rec.Lsn] = true
				}
			}
			if next == cursor {
				require.Len(t, seen, len(written))
				return
			}
			require.Greater(t, next, cursor)
			cursor = next
		}
		t.Fatal("paginated Read did not reach the end of the log")
	}
	before := appendPhase(1)
	var firstWAL uint64
	for lsn := range before {
		if firstWAL == 0 || lsn < firstWAL {
			firstWAL = lsn
		}
	}
	checkPages(1, before)
	require.NoError(t, c.Truncate(ctx, firstWAL))
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	index := s.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	after := appendPhase(2)
	checkPages(index+1, after)
	require.NoError(t, c.TruncateWAL(ctx, index, 1))
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	require.GreaterOrEqual(t, s.store.shardSnapshotInfo.getTruncatedLsn(1), index)
	checkPages(index+1, after)
}
