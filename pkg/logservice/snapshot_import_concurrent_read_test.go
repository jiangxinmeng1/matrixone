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
	files := newFS()
	newService := func() (*Service, error) {
		return NewServiceWithRetry(func() Config { return cfg }, files, nil)
	}
	s, err := newService()
	require.NoError(t, err)
	defer func() {
		if s != nil {
			require.NoError(t, s.Close())
		}
	}()
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

	// The first imported snapshot must release its quota and allow another
	// export/import cycle while later acknowledged WAL remains readable.
	require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
	secondIndex := s.store.snapshotMgr.NewestIndex(1, 1)
	require.Greater(t, secondIndex, index)
	secondWant := make(map[uint64][]byte)
	var secondFirst uint64
	for i := 24; i < 36; i++ {
		lsn, data := appendWAL(i)
		if secondFirst == 0 {
			secondFirst = lsn
		}
		secondWant[lsn] = data
	}
	resp = s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID, Lsn: secondIndex, CheckpointedDSN: 24}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
	secondDir, selected := s.store.snapshotMgr.EvalImportSnapshot(1, 1, secondIndex)
	require.Equal(t, secondIndex, selected)
	require.NoError(t, s.store.importSnapshot(ctx, 1, 1, secondIndex, secondDir))
	verifySecond := func(service *Service) {
		records, _, err := service.store.queryLog(ctx, 1, secondFirst, math.MaxUint64)
		require.NoError(t, err)
		seen := make(map[uint64]bool)
		for _, record := range records {
			if data, ok := secondWant[record.Lsn]; ok {
				require.Equal(t, pb.UserRecord, record.Type)
				require.True(t, bytes.Equal(data, record.Data))
				seen[record.Lsn] = true
			}
		}
		require.Len(t, seen, len(secondWant))
	}
	verifySecond(s)
	require.NoError(t, s.Close())
	s = nil
	s, err = newService()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, _, ok, err := s.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)
	verifySecond(s)
}
