package logservice

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestImportSnapshotExportedBeforeReplicaAddition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := make([]bool, 3)
	defer func() {
		for i, s := range services {
			if !closed[i] {
				require.NoError(t, s.Close())
			}
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID()}
	require.NoError(t, s1.store.startReplica(1, 1, peers, false))
	require.NoError(t, s2.store.startReplica(1, 2, peers, false))
	require.Eventually(t, func() bool {
		id, _, ok, err := s1.store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0
	}, 10*time.Second, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	require.NoError(t, s1.store.getOrExtendTNLease(ctx, 1, 100))
	appendOne := func(s *Service, i int) (uint64, []byte) {
		cmd := getTestAppendCmd(100, []byte(fmt.Sprintf("wal-%04d", i)))
		lsn, err := s.store.append(ctx, 1, cmd, true)
		require.NoError(t, err)
		return lsn, cmd
	}
	for i := 0; i < 10; i++ {
		appendOne(s1, i)
	}
	require.NoError(t, s1.store.truncateLog(ctx, 1, 4))
	require.NoError(t, s1.store.processShardTruncateLog(ctx, 1))
	index := s1.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	written := make(map[uint64][]byte)
	for i := 10; i < 20; i++ {
		lsn, cmd := appendOne(s1, i)
		written[lsn] = cmd
	}
	m, err := s1.store.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, s1.store.nh.SyncRequestAddReplica(ctx, 1, 3, s3.ID(), m.ConfigChangeID))
	require.NoError(t, s3.store.startReplica(1, 3, nil, true))
	require.Eventually(t, func() bool {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		state, err := s3.store.getWALTruncate(attempt, 1)
		return err == nil && state.psn == 4
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, s1.store.truncateWAL(ctx, 1, index, 10))
	require.Eventually(t, func() bool {
		_ = s1.store.processShardTruncateLog(ctx, 1)
		return s1.store.snapshotMgr.Count(1, 1) == 0 &&
			s1.store.shardSnapshotInfo.getTruncatedLsn(1) >= index
	}, 10*time.Second, 100*time.Millisecond)
	for _, s := range services {
		m, err := s.store.nh.SyncGetShardMembership(ctx, 1)
		require.NoError(t, err)
		require.Contains(t, m.Nodes, uint64(3), "new member disappeared after old snapshot import")
	}
	require.NoError(t, s2.Close())
	closed[1] = true
	require.Eventually(t, func() bool {
		id, _, ok, err := s1.store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0 && id != 2
	}, 10*time.Second, 20*time.Millisecond)
	for i := 20; i < 30; i++ {
		lsn, cmd := appendOne(s1, i)
		written[lsn] = cmd
	}
	for _, s := range []*Service{s1, s3} {
		records, _, err := s.store.queryLog(ctx, 1, index+1, math.MaxUint64)
		require.NoError(t, err)
		missing := make(map[uint64][]byte, len(written))
		for lsn, payload := range written {
			missing[lsn] = payload
		}
		for _, record := range records {
			if want, ok := missing[record.Lsn]; ok {
				require.True(t, bytes.Equal(want, record.Data), "WAL %d changed", record.Lsn)
				delete(missing, record.Lsn)
			}
		}
		require.Empty(t, missing, "acknowledged WAL missing")
	}
}
