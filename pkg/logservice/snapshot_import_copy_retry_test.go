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
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

var errImportCopyInterrupted = errors.New("injected snapshot import copy failure")

type interruptedImportCopyFS struct {
	vfs.FS
	armed      atomic.Bool
	failAtSync bool
	path       atomic.Value
}

type interruptedImportCopyFile struct {
	vfs.File
	failAtSync bool
}

func (f *interruptedImportCopyFile) Write(p []byte) (int, error) {
	if f.failAtSync {
		return f.File.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := f.File.Write(p[:1])
	if err != nil {
		return n, err
	}
	return n, errImportCopyInterrupted
}

func (f *interruptedImportCopyFile) Sync() error {
	if f.failAtSync {
		return errImportCopyInterrupted
	}
	return f.File.Sync()
}

func (fs *interruptedImportCopyFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if strings.Contains(name, ".generating/") &&
		!strings.Contains(name, "exported-snapshot") &&
		fs.armed.CompareAndSwap(true, false) {
		fs.path.Store(name)
		return &interruptedImportCopyFile{File: f, failAtSync: fs.failAtSync}, nil
	}
	return f, nil
}

func TestImportSnapshotRetryAfterPartialCopy(t *testing.T) {
	for _, failAtSync := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			name := "write-error/same-process"
			if failAtSync {
				name = "sync-error/same-process"
			}
			if restart {
				name = strings.Replace(name, "same-process", "original-disk-restart", 1)
			}
			t.Run(name, func(t *testing.T) {
				testImportSnapshotRetryAfterPartialCopy(t, restart, failAtSync)
			})
		}
	}
}

func testImportSnapshotRetryAfterPartialCopy(t *testing.T, restart, failAtSync bool) {
	cfg := getServiceTestConfig()
	underlying := cfg.FS
	fs := &interruptedImportCopyFS{FS: underlying, failAtSync: failAtSync}
	cfg.FS = fs
	defer vfs.ReportLeakedFD(underlying, t)
	newService := func() (*Service, error) {
		return NewServiceWithRetry(func() Config { return cfg }, newFS(), nil,
			WithBackendFilter(func(morpc.Message, string) bool { return true }))
	}
	s, err := newService()
	require.NoError(t, err)
	defer func() {
		if s != nil {
			require.NoError(t, s.Close())
		}
	}()

	const shardID, replicaID, tnID = uint64(1), uint64(1), uint64(100)
	peers := map[uint64]dragonboat.Target{replicaID: s.ID()}
	require.NoError(t, s.store.startReplica(shardID, replicaID, peers, false))
	require.NoError(t, s.store.startHAKeeperReplica(replicaID, peers, false))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	waitLeader := func() {
		for {
			_, _, ok, err := s.store.nh.GetLeaderID(shardID)
			require.NoError(t, err)
			if ok {
				return
			}
			require.NoError(t, ctx.Err())
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitLeader()
	connect := pb.Request{Method: pb.CONNECT_RO, LogRequest: pb.LogRequest{ShardID: shardID, TNID: tnID}}
	require.Equal(t, uint32(moerr.Ok), s.handleConnect(ctx, connect).ErrorCode)
	appendTen := func() {
		for i := 0; i < 10; i++ {
			cmd := getTestAppendCmd(tnID, []byte("payload"))
			req := pb.Request{Method: pb.APPEND, LogRequest: pb.LogRequest{ShardID: shardID}}
			require.Equal(t, uint32(moerr.Ok), s.handleAppend(ctx, req, cmd).ErrorCode)
		}
	}
	appendTen()
	require.NoError(t, s.store.truncateWAL(ctx, shardID, 4, 1))
	require.NoError(t, s.store.processShardTruncateLog(ctx, shardID))
	require.Equal(t, 1, s.store.snapshotMgr.Count(shardID, replicaID))
	appendTen()
	require.NoError(t, s.store.truncateWAL(ctx, shardID, 15, 2))

	fs.armed.Store(true)
	firstErr := s.store.processShardTruncateLog(ctx, shardID)
	require.ErrorIs(t, firstErr, errImportCopyInterrupted)
	require.NotNil(t, fs.path.Load(), "must fail while copying into Dragonboat's destination snapshot directory")
	partialPath := fs.path.Load().(string)
	partial, err := underlying.Stat(partialPath)
	require.NoError(t, err)
	if failAtSync {
		require.Greater(t, partial.Size(), int64(1))
	} else {
		require.Equal(t, int64(1), partial.Size())
	}
	require.Equal(t, 1, s.store.snapshotMgr.Count(shardID, replicaID))
	state, err := s.store.getWALTruncate(ctx, shardID)
	require.NoError(t, err)
	require.Equal(t, walTruncateState{15, 2}, state)
	if restart {
		require.NoError(t, s.Close())
		s = nil
		persisted, err := underlying.Stat(partialPath)
		require.NoError(t, err, "partial snapshot copy must survive process stop")
		require.Equal(t, partial.Size(), persisted.Size())
		s, err = newService()
		require.NoError(t, err)
		waitLeader()
		require.Equal(t, 1, s.store.snapshotMgr.Count(shardID, replicaID))
	}

	require.NoError(t, s.store.processShardTruncateLog(ctx, shardID))
	require.Zero(t, s.store.snapshotMgr.Count(shardID, replicaID))
	_, err = underlying.Stat(partialPath)
	require.True(t, os.IsNotExist(err), "successful retry must remove the partial import copy")
	state, err = s.store.getWALTruncate(ctx, shardID)
	require.NoError(t, err)
	require.Equal(t, walTruncateState{15, 2}, state)
}
