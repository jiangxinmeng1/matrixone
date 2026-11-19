package logservice

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestReadRetainedWALWhileImportingSnapshot(t *testing.T) {
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	s, err := NewServiceWithRetry(func() Config { return cfg }, newFS(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, s.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: s.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		_, _, ok, err := s.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)

	const tnID = uint64(100)
	resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	appendWAL := func(i int) (uint64, []byte) {
		cmd := getTestAppendCmd(tnID, []byte(fmt.Sprintf("read-import-%04d", i)))
		r := s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
		require.Equal(t, uint32(moerr.Ok), r.ErrorCode, r.ErrorMessage)
		return r.LogResponse.Lsn, cmd
	}
	for i := 0; i < 12; i++ {
		appendWAL(i)
	}
	resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID, Lsn: 5}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	index := s.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)

	want := make(map[uint64][]byte)
	var first uint64
	for i := 12; i < 24; i++ {
		lsn, data := appendWAL(i)
		if first == 0 {
			first = lsn
		}
		want[lsn] = data
	}
	resp = s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID, Lsn: index, CheckpointedDSN: 12}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
	dir, selected := s.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Equal(t, index, selected)

	var wg sync.WaitGroup
	var importErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		importErr = s.store.importSnapshot(ctx, 1, 1, index, dir)
	}()
	for i := 0; i < 100; i++ {
		records, _, err := s.store.queryLog(ctx, 1, first, math.MaxUint64)
		require.NoError(t, err, "read iteration %d", i)
		seen := make(map[uint64]bool)
		for _, record := range records {
			if data, ok := want[record.Lsn]; ok {
				require.Equal(t, pb.UserRecord, record.Type)
				require.True(t, bytes.Equal(data, record.Data))
				seen[record.Lsn] = true
			}
		}
		require.Len(t, seen, len(want), "read iteration %d", i)
	}
	wg.Wait()
	require.NoError(t, importErr)
}
