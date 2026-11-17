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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestExportedSnapshotSurvivesRestartBeforeWALTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	files := newFS()
	start := func() *Service {
		s, err := NewServiceWithRetry(func() Config { return cfg }, files, nil,
			WithBackendFilter(func(morpc.Message, string) bool { return true }))
		require.NoError(t, err)
		return s
	}
	service := start()
	defer func() {
		if service != nil {
			require.NoError(t, service.Close())
		}
	}()
	require.NoError(t, service.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: service.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitLeader := func() {
		require.Eventually(t, func() bool {
			_, _, ok, err := service.store.nh.GetLeaderID(1)
			return err == nil && ok
		}, 5*time.Second, 10*time.Millisecond)
	}
	waitLeader()
	const tnID = uint64(100)
	connect := service.handleConnect(ctx, pb.Request{Method: pb.CONNECT,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID}})
	require.Equal(t, uint32(moerr.Ok), connect.ErrorCode)
	appendOne := func(i int) (uint64, []byte) {
		cmd := getTestAppendCmd(tnID, []byte(fmt.Sprintf("wal-%04d", i)))
		resp := service.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
		return resp.LogResponse.Lsn, cmd
	}
	for i := 0; i < 10; i++ {
		appendOne(i)
	}
	truncate := service.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: 4}})
	require.Equal(t, uint32(moerr.Ok), truncate.ErrorCode, truncate.ErrorMessage)
	require.NoError(t, service.store.processShardTruncateLog(ctx, 1))
	index := service.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	written := make(map[uint64][]byte)
	for i := 10; i < 20; i++ {
		lsn, cmd := appendOne(i)
		written[lsn] = cmd
	}

	// The export is only a disk candidate. Restart before the TN checkpoint
	// advances the WAL truncation watermark and before Dragonboat imports it.
	require.NoError(t, service.Close())
	service = nil
	service = start()
	waitLeader()
	require.Equal(t, index, service.store.snapshotMgr.NewestIndex(1, 1))
	before, err := service.store.getWALTruncate(ctx, 1)
	require.NoError(t, err)
	require.Zero(t, before.dsn)
	resp := service.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: index, CheckpointedDSN: 10}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
	dir, imported := service.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Equal(t, index, imported)
	require.NoError(t, service.store.importSnapshot(ctx, 1, 1, imported, dir))
	require.NoError(t, service.Close())
	service = nil
	service = start()
	waitLeader()
	state, err := service.store.getWALTruncate(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, walTruncateState{psn: index, dsn: 10}, state)
	records, _, err := service.store.queryLog(ctx, 1, index+1, math.MaxUint64)
	require.NoError(t, err)
	for _, record := range records {
		if want, ok := written[record.Lsn]; ok {
			require.True(t, bytes.Equal(want, record.Data), "WAL %d changed", record.Lsn)
			delete(written, record.Lsn)
		}
	}
	require.Empty(t, written, "acknowledged WAL missing after two restarts")
	appendOne(20)
}
