package logservice

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

func TestStoppedReplicaRecoversWALAfterLateSnapshotExport(t *testing.T) {
	cfg := getStoreTestConfig()
	cfg.DisableWorkers = true
	cfg.TruncateInterval.Duration = time.Hour
	cfg.HAKeeperTruncateInterval.Duration = time.Hour
	fs := &pausedExportRenameFS{
		FS: cfg.FS, entered: make(chan struct{}), release: make(chan struct{}),
	}
	cfg.FS = fs
	l, err := getTestStore(func() Config { return cfg }, true, nil)
	require.NoError(t, err)
	defer func() {
		fs.resume()
		if l != nil {
			require.NoError(t, l.close())
		}
		vfs.ReportLeakedFD(fs, t)
	}()
	require.NoError(t, l.startHAKeeperReplica(1,
		map[uint64]dragonboat.Target{1: l.nh.ID()}, false))
	const shardID, replicaID = uint64(1), uint64(2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, l.getOrExtendTNLease(ctx, shardID, 100))
	want := make(map[uint64][]byte)
	for i := 0; i < 12; i++ {
		cmd := getTestAppendCmd(100, []byte(fmt.Sprintf("late-export-wal-%d", i)))
		lsn, err := l.append(ctx, shardID, cmd, true)
		require.NoError(t, err)
		want[lsn] = cmd
	}
	exportCtx := &controlledDeadlineContext{
		Context: context.Background(), deadline: time.Now().Add(30 * time.Second),
		done: make(chan struct{}),
	}
	defer exportCtx.expire()
	result := make(chan error, 1)
	go func() { result <- l.exportSnapshot(exportCtx, shardID, replicaID) }()
	select {
	case <-fs.entered:
	case err := <-result:
		t.Fatalf("export returned before finalization: %v", err)
	case <-ctx.Done():
		t.Fatal("export did not reach finalization")
	}
	require.NoError(t, l.stopReplica(shardID, replicaID))
	select {
	case err := <-result:
		require.ErrorIs(t, err, dragonboat.ErrShardClosed)
	case <-ctx.Done():
		t.Fatal("stopped export did not return")
	}
	fs.resume()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	require.Eventually(t, func() bool {
		names, err := fs.List(l.snapshotMgr.exportPath(shardID, replicaID))
		if err != nil {
			return false
		}
		for _, name := range names {
			var index uint64
			count, err := fmt.Sscanf(name, snapshotPathPattern, &index)
			if err != nil || count != 1 || name != fmt.Sprintf(snapshotPathPattern, index) {
				continue
			}
			files, err := fs.List(l.snapshotMgr.snapshotPath(nid, snapshotIndex(index)))
			if err == nil && len(files) == 2 {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "stopped export must finish after its request returned")
	require.NoError(t, l.close())
	l = nil
	l, err = newLogStoreWithRetry(func() Config { return cfg }, nil, nil,
		runtime.DefaultRuntime(), newFS())
	require.NoError(t, err)
	require.NoError(t, l.loadMetadata())
	require.NoError(t, l.startReplicas(ctx))
	require.Eventually(t, func() bool {
		_, _, ok, err := l.nh.GetLeaderID(shardID)
		return err == nil && ok
	}, 5*time.Second, 10*time.Millisecond)
	for lsn, data := range want {
		records, _, err := l.queryLog(ctx, shardID, lsn, 1<<20)
		require.NoError(t, err)
		found := false
		for _, record := range records {
			if record.Lsn == lsn {
				require.True(t, bytes.Equal(data, record.Data))
				found = true
			}
		}
		require.True(t, found, "acknowledged WAL %d missing after replica restart", lsn)
	}
	snapshotIndex := l.snapshotMgr.NewestIndex(shardID, replicaID)
	require.NotZero(t, snapshotIndex, "restart must load the late completed export")
	later := getTestAppendCmd(100, []byte("wal-after-restart"))
	laterLSN, err := l.append(ctx, shardID, later, true)
	require.NoError(t, err)
	require.Greater(t, laterLSN, snapshotIndex)
	require.NoError(t, l.truncateWAL(ctx, shardID, snapshotIndex, 12, 100))
	require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
	require.Zero(t, l.snapshotMgr.Count(shardID, replicaID))
	records, _, err := l.queryLog(ctx, shardID, laterLSN, 1<<20)
	require.NoError(t, err)
	foundLater := false
	for _, record := range records {
		if record.Lsn == laterLSN {
			require.True(t, bytes.Equal(later, record.Data))
			foundLater = true
		}
	}
	require.True(t, foundLater)
}
