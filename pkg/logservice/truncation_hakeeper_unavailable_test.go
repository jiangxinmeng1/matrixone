package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestTruncationWaitsForHAKeeperWithFullSnapshotQuota(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		s.store.cfg.MaxExportedSnapshot = 1
		resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{ShardID: 1, TNID: 100}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		for i := 0; i < 20; i++ {
			resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND,
				LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{ShardID: 1, Lsn: 4}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
		firstIndex := s.store.snapshotMgr.NewestIndex(1, 1)
		require.Zero(t, s.store.shardSnapshotInfo.getTruncatedLsn(1))
		for i := 0; i < 10; i++ {
			resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND,
				LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{ShardID: 1, Lsn: firstIndex + 1}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		dir, eligibleIndex := s.store.snapshotMgr.EvalImportSnapshot(1, 1, firstIndex+1)
		require.NotEmpty(t, dir)
		require.Equal(t, firstIndex, eligibleIndex)
		for i := 0; i < 3; i++ {
			require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
			require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
			require.Equal(t, firstIndex, s.store.snapshotMgr.NewestIndex(1, 1))
			require.Zero(t, s.store.shardSnapshotInfo.getTruncatedLsn(1))
		}
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND,
			LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		peers := map[uint64]dragonboat.Target{1: s.ID()}
		require.NoError(t, s.store.startHAKeeperReplica(1, peers, false))
		require.Eventually(t, func() bool {
			if err := s.store.processShardTruncateLog(ctx, 1); err != nil {
				return false
			}
			return s.store.shardSnapshotInfo.getTruncatedLsn(1) >= firstIndex
		}, 5*time.Second, 50*time.Millisecond)
		require.Zero(t, s.store.snapshotMgr.Count(1, 1))
	})
}
