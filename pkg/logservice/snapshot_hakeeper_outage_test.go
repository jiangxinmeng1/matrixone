package logservice

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/stretchr/testify/require"
)

func TestSnapshotImportResumesAfterHAKeeperReplicaRestarts(t *testing.T) {
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
	appendOne := func(value string) (uint64, []byte) {
		rec := c.GetLogRecord(len(value))
		copy(rec.Payload(), value)
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		return lsn, bytes.Clone(rec.Data)
	}
	for i := 0; i < 10; i++ {
		appendOne("before-hakeeper-outage")
	}
	require.NoError(t, c.Truncate(ctx, 4))
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	index := s.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	later, laterData := appendOne("during-hakeeper-outage")
	require.Greater(t, later, index)
	require.NoError(t, c.TruncateWAL(ctx, index, 1))

	require.NoError(t, s.store.stopReplica(hakeeper.DefaultHAKeeperShardID, 1))
	outageLSN, outageData := appendOne("write-while-hakeeper-stopped")
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	require.GreaterOrEqual(t, s.store.snapshotMgr.Count(1, 1), 1)
	require.Less(t, s.store.shardSnapshotInfo.getTruncatedLsn(1), index)
	checkRecord := func(lsn uint64, data []byte) {
		records, _, err := c.Read(ctx, lsn, 1<<20)
		require.NoError(t, err)
		for _, record := range records {
			if record.Lsn == lsn {
				require.Equal(t, data, record.Data)
				return
			}
		}
		t.Fatalf("confirmed WAL %d missing", lsn)
	}
	checkRecord(later, laterData)
	checkRecord(outageLSN, outageData)
	require.NoError(t, s.store.startHAKeeperReplica(1,
		map[uint64]dragonboat.Target{1: s.ID()}, false))
	require.Eventually(t, func() bool {
		err := s.store.processShardTruncateLog(ctx, 1)
		return err == nil && s.store.shardSnapshotInfo.getTruncatedLsn(1) >= index
	}, 10*time.Second, 100*time.Millisecond)
	dir, importedIndex := s.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Empty(t, dir)
	require.Zero(t, importedIndex)
	psn, dsn, err := c.GetWALTruncate(ctx)
	require.NoError(t, err)
	require.Equal(t, index, psn)
	require.Equal(t, uint64(1), dsn)
	checkRecord(later, laterData)
	checkRecord(outageLSN, outageData)
	appendOne("after-hakeeper-restart")
}
