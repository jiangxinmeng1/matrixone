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

func TestSnapshotImportCrashBeforeCandidateRemoval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	files := newFS()
	newService := func() *Service {
		s, err := NewServiceWithRetry(func() Config { return cfg }, files, nil,
			WithBackendFilter(func(morpc.Message, string) bool { return true }))
		require.NoError(t, err)
		return s
	}
	s := newService()
	defer func() {
		if s != nil {
			require.NoError(t, s.Close())
		}
	}()
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
	defer func() {
		if c != nil {
			require.NoError(t, c.Close())
		}
	}()
	for i := 0; i < 10; i++ {
		_, err := c.Append(ctx, c.GetLogRecord(8))
		require.NoError(t, err)
	}
	require.NoError(t, c.Truncate(ctx, 4))
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	index := s.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	const laterValue = "retained-after-import"
	later := c.GetLogRecord(len(laterValue))
	copy(later.Payload(), laterValue)
	laterLSN, err := c.Append(ctx, later)
	require.NoError(t, err)
	require.NoError(t, c.TruncateWAL(ctx, index, 1))
	dir, selected := s.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Equal(t, index, selected)
	// Stop after Dragonboat has committed the import, before LogService calls
	// snapshotMgr.Remove and forwardTruncate.
	require.NoError(t, s.store.nh.SyncRequestImportSnapshot(ctx, 1, 1, dir))
	require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
	require.NoError(t, c.Close())
	c = nil
	require.NoError(t, s.Close())
	s = nil

	s = newService()
	require.Eventually(t, func() bool {
		_, _, ok, err := s.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
	if s.store.getReplicaID(hakeeper.DefaultHAKeeperShardID) <= 0 {
		require.NoError(t, s.store.startHAKeeperReplica(1,
			map[uint64]dragonboat.Target{1: s.ID()}, false))
	}
	firstErr := s.store.processShardTruncateLog(ctx, 1)
	require.NoError(t, firstErr)
	require.GreaterOrEqual(t, s.store.shardSnapshotInfo.getTruncatedLsn(1), index)
	_, staleIndex := s.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Zero(t, staleIndex)
	secondErr := s.store.processShardTruncateLog(ctx, 1)
	require.NoError(t, secondErr)
	records, _, readErr := s.store.queryLog(ctx, 1, laterLSN, 1<<20)
	require.NoError(t, readErr)
	found := false
	for _, record := range records {
		if record.Lsn == laterLSN {
			require.True(t, bytes.Equal(later.Data, record.Data))
			found = true
		}
	}
	require.True(t, found)
}
