package logservice

import (
	"context"
	"testing"
	"time"

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
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{ShardID: 1, Lsn: 6}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		for i := 0; i < 3; i++ {
			require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
			require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
			require.Equal(t, firstIndex, s.store.snapshotMgr.NewestIndex(1, 1))
			require.Zero(t, s.store.shardSnapshotInfo.getTruncatedLsn(1))
		}
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND,
			LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	})
}
