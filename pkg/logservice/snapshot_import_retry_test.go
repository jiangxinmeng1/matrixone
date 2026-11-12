// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestImportSnapshotRetryAfterCleanupFailure(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		peers := map[uint64]dragonboat.Target{1: s.ID()}
		require.NoError(t, s.store.startHAKeeperReplica(1, peers, false))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		const shardID, replicaID, tnID = uint64(1), uint64(1), uint64(100)
		connect := pb.Request{Method: pb.CONNECT_RO, LogRequest: pb.LogRequest{ShardID: shardID, TNID: tnID}}
		require.Equal(t, uint32(moerr.Ok), s.handleConnect(ctx, connect).ErrorCode)
		appendTen := func() {
			for i := 0; i < 10; i++ {
				cmd := getTestAppendCmd(tnID, make([]byte, 8))
				req := pb.Request{Method: pb.APPEND, LogRequest: pb.LogRequest{ShardID: shardID}}
				require.Equal(t, uint32(moerr.Ok), s.handleAppend(ctx, req, cmd).ErrorCode)
			}
		}
		truncate := func(lsn uint64) {
			req := pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: shardID, Lsn: lsn}}
			require.Equal(t, uint32(moerr.Ok), s.handleTruncate(ctx, req).ErrorCode)
		}
		appendTen()
		truncate(4)
		require.NoError(t, s.store.processShardTruncateLog(ctx, shardID))
		require.Equal(t, 1, s.store.snapshotMgr.Count(shardID, replicaID))
		appendTen()
		truncate(15)

		nid := nodeID{shardID: shardID, replicaID: replicaID}
		item := s.store.snapshotMgr.snapshots[nid].items[0]
		originalFS := item.fs
		// The import completes in Dragonboat; only removal of the exported
		// source directory fails. The next truncation tick must retry cleanup.
		item.fs = vfs.Wrap(originalFS, vfs.OnIndex(0, vfs.OpWrite))
		firstErr := s.store.processShardTruncateLog(ctx, shardID)
		if s.store.snapshotMgr.Count(shardID, replicaID) == 1 {
			item.fs = originalFS
		}
		require.Error(t, firstErr, "injected exported-snapshot cleanup must fail")
		require.Equal(t, 1, s.store.snapshotMgr.Count(shardID, replicaID))
		t.Logf("first import completed but cleanup failed: %v", firstErr)
		require.NoError(t, s.store.processShardTruncateLog(ctx, shardID), "retry must finish cleanup after import succeeded")
		require.Zero(t, s.store.snapshotMgr.Count(shardID, replicaID))
	}
	runServiceTest(t, false, true, fn)
}
