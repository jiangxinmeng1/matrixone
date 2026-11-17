package logservice

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestImportExportedSnapshotWhileAppendingWAL(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		const tnID = uint64(100)
		connect := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT, LogRequest: pb.LogRequest{ShardID: 1, TNID: tnID}})
		require.Equal(t, uint32(moerr.Ok), connect.ErrorCode)
		appendOne := func(i int) (uint64, []byte, error) {
			cmd := getTestAppendCmd(tnID, []byte(fmt.Sprintf("wal-%04d", i)))
			resp := s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL, LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
			if resp.ErrorCode != uint32(moerr.Ok) {
				return 0, nil, fmt.Errorf("append %d: %d %s", i, resp.ErrorCode, resp.ErrorMessage)
			}
			return resp.LogResponse.Lsn, cmd, nil
		}
		for i := 0; i < 10; i++ {
			_, _, err := appendOne(i)
			require.NoError(t, err)
		}
		resp := s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, Lsn: 4}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		snapshotIndex := s.store.snapshotMgr.NewestIndex(1, 1)
		require.NotZero(t, snapshotIndex)
		type written struct {
			lsn uint64
			cmd []byte
		}
		var mu sync.Mutex
		acknowledged := make([]written, 0, 110)
		for i := 10; i < 20; i++ {
			lsn, cmd, err := appendOne(i)
			require.NoError(t, err)
			acknowledged = append(acknowledged, written{lsn, cmd})
		}
		resp = s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL, LogRequest: pb.LogRequest{ShardID: 1, Lsn: snapshotIndex, CheckpointedDSN: 10}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		dir, index := s.store.snapshotMgr.EvalImportSnapshot(1, 1, snapshotIndex)
		require.Equal(t, snapshotIndex, index)

		firstWrite := make(chan struct{})
		writeErr := make(chan error, 1)
		go func() {
			for i := 20; i < 120; i++ {
				lsn, cmd, err := appendOne(i)
				if err != nil {
					writeErr <- err
					return
				}
				mu.Lock()
				acknowledged = append(acknowledged, written{lsn, cmd})
				mu.Unlock()
				if i == 20 {
					close(firstWrite)
				}
				time.Sleep(time.Millisecond)
			}
			writeErr <- nil
		}()
		select {
		case <-firstWrite:
		case err := <-writeErr:
			require.NoError(t, err)
			t.Fatal("writer stopped before first append")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		importErr := s.store.importSnapshot(ctx, 1, 1, index, dir)
		writerErr := <-writeErr
		require.NoError(t, importErr)
		require.NoError(t, writerErr)

		checkReadable := func() {
			records, _, err := s.store.queryLog(ctx, 1, snapshotIndex+1, math.MaxUint64)
			require.NoError(t, err)
			byLSN := make(map[uint64][]byte, len(records))
			for _, record := range records {
				if record.Type == pb.UserRecord {
					byLSN[record.Lsn] = record.Data
				}
			}
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, acknowledged, 110)
			for _, write := range acknowledged {
				require.True(t, bytes.Equal(write.cmd, byLSN[write.lsn]), "acknowledged LSN %d was not readable after import", write.lsn)
			}
		}
		checkReadable()
	})
}
