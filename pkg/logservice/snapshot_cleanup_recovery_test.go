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
	"os"
	"testing"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotPartialCleanupReloadsRemainingCandidates(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(2)
	cfg := &Config{FS: vfs.NewStrictMem(), SnapshotExportDir: "/exported"}
	mgr := newSnapshotManager(cfg)
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	for _, index := range []snapshotIndex{100, 200, 300} {
		testPrepareSnapshot(t, mgr, nid, index)
	}
	require.NoError(t, mgr.Init(shardID, replicaID))
	require.Equal(t, 3, mgr.Count(shardID, replicaID))
	// The first directory is deleted, but deletion of the second fails.
	mgr.snapshots[nid].items[1].fs = vfs.Wrap(cfg.FS, vfs.OnIndex(0, vfs.OpWrite))
	require.Error(t, mgr.Remove(shardID, replicaID, 250))
	require.Equal(t, 2, mgr.Count(shardID, replicaID))
	_, err := cfg.FS.Stat(mgr.snapshotPath(nid, 100))
	require.True(t, os.IsNotExist(err))
	_, err = cfg.FS.Stat(mgr.snapshotPath(nid, 200))
	require.NoError(t, err)

	// A fresh manager represents process restart: reconstruct candidates
	// from the surviving on-disk exports, then finish the same cleanup.
	restarted := newSnapshotManager(cfg)
	require.NoError(t, restarted.Init(shardID, replicaID))
	require.Equal(t, 2, restarted.Count(shardID, replicaID))
	dir, index := restarted.EvalImportSnapshot(shardID, replicaID, 250)
	require.Equal(t, uint64(200), index)
	require.Equal(t, restarted.snapshotPath(nid, 200), dir)
	require.NoError(t, restarted.Remove(shardID, replicaID, 250))
	require.Equal(t, 1, restarted.Count(shardID, replicaID))
	dir, index = restarted.EvalImportSnapshot(shardID, replicaID, 999)
	require.Equal(t, uint64(300), index)
	require.Equal(t, restarted.snapshotPath(nid, 300), dir)
}
