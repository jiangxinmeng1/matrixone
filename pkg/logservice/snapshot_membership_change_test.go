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

func TestImportSnapshotExportedBeforeReplicaRemoval(t *testing.T) {
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
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID(), 3: s3.ID()}
	for i, s := range services {
		require.NoError(t, s.store.startReplica(1, uint64(i+1), peers, false))
	}
	var leader uint64
	require.Eventually(t, func() bool {
		id, _, ok, err := s1.store.nh.GetLeaderID(1)
		leader = id
		return err == nil && ok && id != 0
	}, 10*time.Second, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	survivor := int(leader % 3)
	require.NoError(t, services[survivor].store.processShardTruncateLog(ctx, 1))
	index := services[survivor].store.snapshotMgr.NewestIndex(1, uint64(survivor+1))
	require.NotZero(t, index)
	require.NoError(t, services[int(leader-1)].Close())
	closed[int(leader-1)] = true
	require.Eventually(t, func() bool {
		id, _, ok, err := services[survivor].store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0 && id != leader
	}, 10*time.Second, 20*time.Millisecond)
	membership, err := services[survivor].store.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, services[survivor].store.nh.SyncRequestDeleteReplica(
		ctx, 1, leader, membership.ConfigChangeID))
	written := make(map[uint64][]byte)
	for i := 10; i < 20; i++ {
		lsn, cmd := appendOne(services[survivor], i)
		written[lsn] = cmd
	}
	require.NoError(t, services[survivor].store.truncateWAL(ctx, 1, index, 10))
	require.Eventually(t, func() bool {
		_ = services[survivor].store.processShardTruncateLog(ctx, 1)
		return services[survivor].store.snapshotMgr.Count(1, uint64(survivor+1)) == 0 &&
			services[survivor].store.shardSnapshotInfo.getTruncatedLsn(1) >= index
	}, 10*time.Second, 100*time.Millisecond)
	for i, s := range services {
		if closed[i] {
			continue
		}
		m, err := s.store.nh.SyncGetShardMembership(ctx, 1)
		require.NoError(t, err)
		require.NotContains(t, m.Nodes, leader, "removed replica reappeared on node %d", i+1)
		records, _, err := s.store.queryLog(ctx, 1, index+1, math.MaxUint64)
		require.NoError(t, err)
		missing := make(map[uint64][]byte, len(written))
		for lsn, data := range written {
			missing[lsn] = data
		}
		for _, record := range records {
			if want, ok := missing[record.Lsn]; ok {
				require.True(t, bytes.Equal(want, record.Data), "WAL %d changed on node %d", record.Lsn, i+1)
				delete(missing, record.Lsn)
			}
		}
		require.Empty(t, missing, "WAL missing on node %d", i+1)
	}
}
