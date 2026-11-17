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
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestImportedSnapshotRetainsLaterWALAcrossServiceRecreation(t *testing.T) {
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
	service := newService()
	defer func() {
		if service != nil {
			require.NoError(t, service.Close())
		}
	}()
	require.NoError(t, service.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: service.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		_, _, ok, err := service.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)
	const tnID = uint64(100)
	resp := service.handleConnect(ctx, pb.Request{Method: pb.CONNECT,
		LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	appendOne := func(s *Service, i int) (uint64, []byte) {
		cmd := getTestAppendCmd(tnID, []byte(fmt.Sprintf("wal-%04d", i)))
		resp := s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode, resp.ErrorMessage)
		return resp.LogResponse.Lsn, cmd
	}
	for i := 0; i < 10; i++ {
		appendOne(service, i)
	}
	resp = service.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: 4}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	require.NoError(t, service.store.processShardTruncateLog(ctx, 1))
	index := service.store.snapshotMgr.NewestIndex(1, 1)
	require.NotZero(t, index)
	written := make(map[uint64][]byte)
	var laterPSN uint64
	for i := 10; i < 20; i++ {
		lsn, cmd := appendOne(service, i)
		if laterPSN == 0 {
			laterPSN = lsn
		}
		written[lsn] = cmd
	}
	resp = service.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
		LogRequest: pb.LogRequest{ShardID: 1, Lsn: index, CheckpointedDSN: 10}})
	require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	dir, importedIndex := service.store.snapshotMgr.EvalImportSnapshot(1, 1, index)
	require.Equal(t, index, importedIndex)
	var wg sync.WaitGroup
	var importErr error
	var laterTruncate pb.Response
	wg.Add(2)
	go func() {
		defer wg.Done()
		importErr = service.store.importSnapshot(ctx, 1, 1, index, dir)
	}()
	go func() {
		defer wg.Done()
		laterTruncate = service.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL,
			LogRequest: pb.LogRequest{ShardID: 1, Lsn: laterPSN, CheckpointedDSN: 11}})
	}()
	wg.Wait()
	require.NoError(t, importErr)
	require.Equal(t, uint32(moerr.Ok), laterTruncate.ErrorCode, laterTruncate.ErrorMessage)
	before, err := service.store.getWALTruncate(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, walTruncateState{psn: laterPSN, dsn: 11}, before)
	require.NoError(t, service.Close())
	service = nil

	service = newService()
	require.Eventually(t, func() bool {
		_, _, ok, err := service.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)
	after, err := service.store.getWALTruncate(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, before, after, "WAL truncate checkpoint changed after snapshot import and restart")
	records, _, err := service.store.queryLog(ctx, 1, index+1, math.MaxUint64)
	require.NoError(t, err)
	for _, record := range records {
		if want, ok := written[record.Lsn]; ok {
			require.True(t, bytes.Equal(want, record.Data), "record %d changed after restart", record.Lsn)
			delete(written, record.Lsn)
		}
	}
	require.Empty(t, written, "acknowledged WAL missing after snapshot import and service restart")
	appendOne(service, 20)
}
