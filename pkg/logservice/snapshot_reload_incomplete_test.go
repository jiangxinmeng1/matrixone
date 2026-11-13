// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"testing"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotReloadSkipsIncompleteAndKeepsComplete(t *testing.T) {
	cfg := &Config{FS: vfs.Default, SnapshotExportDir: t.TempDir()}
	nid := nodeID{shardID: 1, replicaID: 1}
	first := newSnapshotManager(cfg)
	testPrepareSnapshot(t, first, nid, 100)
	partial := first.snapshotPath(nid, 200)
	require.NoError(t, cfg.FS.MkdirAll(partial, defaultExportedDirMode))
	f, err := cfg.FS.Create(cfg.FS.PathJoin(partial, "snapshot.metadata"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	restarted := newSnapshotManager(cfg)
	require.NoError(t, restarted.ReconcileInterruptedExport(1, 1))
	require.NoError(t, restarted.Init(1, 1))
	require.Equal(t, 1, restarted.Count(1, 1))
	require.Equal(t, uint64(100), restarted.NewestIndex(1, 1))
	_, err = cfg.FS.Stat(partial)
	require.NoError(t, err, "partial directory remains on disk")

	testPrepareSnapshot(t, restarted, nid, 300)
	require.NoError(t, restarted.Add(1, 1, 300))
	require.Equal(t, 2, restarted.Count(1, 1))

	f, err = cfg.FS.Create(cfg.FS.PathJoin(partial, "snapshot-00000000000000C8.gbsnap"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	restartedAgain := newSnapshotManager(cfg)
	require.NoError(t, restartedAgain.Init(1, 1))
	require.Equal(t, 3, restartedAgain.Count(1, 1))
	require.Equal(t, uint64(300), restartedAgain.NewestIndex(1, 1))
}
