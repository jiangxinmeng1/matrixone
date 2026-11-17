package logservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
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

var errExportSnapshotRemovalOnce = errors.New("injected exported snapshot removal failure")

type failExportSnapshotRemovalOnceFS struct {
	vfs.FS
	target string
	fail   atomic.Bool
}

func (fs *failExportSnapshotRemovalOnceFS) RemoveAll(name string) error {
	if name == fs.target && fs.fail.CompareAndSwap(true, false) {
		return errExportSnapshotRemovalOnce
	}
	return fs.FS.RemoveAll(name)
}

func TestImportSnapshotRetriesAfterExportCleanupFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	fs := &failExportSnapshotRemovalOnceFS{FS: cfg.FS}
	cfg.FS = fs
	defer vfs.ReportLeakedFD(fs, t)
	s, err := NewServiceWithRetry(func() Config { return cfg }, newFS(), nil,
		WithBackendFilter(func(morpc.Message, string) bool { return true }))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, s.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: s.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const tnID = uint64(100)
	connect := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID}})
	require.Equal(t, uint32(moerr.Ok), connect.ErrorCode, connect.ErrorMessage)
	appendOne := func(i int) (uint64, []byte) {
		cmd := getTestAppendCmd(tnID, []byte(fmt.Sprintf("wal-%04d", i)))
		resp := s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
		return resp.LogResponse.Lsn, cmd
	}
	for i := 0; i < 10; i++ {
		appendOne(i)
	}
	truncate := s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: 4}})
	require.Equal(t, uint32(moerr.Ok), truncate.ErrorCode, truncate.ErrorMessage)
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	index := s.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	written := make(map[uint64][]byte)
	for i := 10; i < 20; i++ {
		lsn, cmd := appendOne(i)
		written[lsn] = cmd
	}
	resp := s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: index, CheckpointedDSN: 10}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
	dir, imported := s.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Equal(t, index, imported)
	fs.target = dir
	fs.fail.Store(true)
	err = s.store.importSnapshot(ctx, 1, 1, imported, dir)
	require.ErrorIs(t, err, errExportSnapshotRemovalOnce)
	require.False(t, fs.fail.Load(), "injection did not reach exported snapshot cleanup")
	require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
	reader, err := s.store.nh.GetLogReader(1)
	require.NoError(t, err)
	require.Equal(t, index, reader.Snapshot().Index,
		"Dragonboat import must have committed before the cleanup failure")
	require.NoError(t, s.store.importSnapshot(ctx, 1, 1, imported, dir))
	require.Zero(t, s.store.snapshotMgr.Count(1, 1))
	records, _, err := s.store.queryLog(ctx, 1, index+1, math.MaxUint64)
	require.NoError(t, err)
	for _, record := range records {
		if want, ok := written[record.Lsn]; ok {
			require.True(t, bytes.Equal(want, record.Data), "WAL %d changed", record.Lsn)
			delete(written, record.Lsn)
		}
	}
	require.Empty(t, written)
	appendOne(20)
}
