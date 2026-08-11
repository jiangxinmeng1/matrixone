// Copyright 2021 Matrix Origin
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

package updates

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutationControllerAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mc := NewAppendMVCCHandle(obj)

	nodeCnt := 10000
	rowsPerNode := uint32(5)
	//ts := uint64(2)
	//ts = 4

	ts := types.NextGlobalTsForTest()
	ts = ts.Next()
	ts = ts.Next()
	//queries := make([]uint64, 0)
	//queries = append(queries, ts-1)
	queries := make([]types.TS, 0)
	queries = append(queries, ts.Prev())

	for i := 0; i < nodeCnt; i++ {
		txn := mockTxn()
		txn.CommitTS = ts
		txn.PrepareTS = ts
		node, _ := mc.AddAppendNodeLocked(txn, rowsPerNode*uint32(i), rowsPerNode*(uint32(i)+1))
		err := node.ApplyCommit(txn.ID)
		assert.Nil(t, err)
		//queries = append(queries, ts+1)
		queries = append(queries, ts.Next())
		//ts += 2
		ts = ts.Next()
		ts = ts.Next()
	}

	st := time.Now()
	for i, qts := range queries {
		selection, ok, _ := mc.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(qts))
		if i == 0 {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, uint32(i)*rowsPerNode, selection.MaxRow)
		}
	}
	t.Logf("%s -- %d ops", time.Since(st), len(queries))
}

// AppendNode Start Prepare End Aborted
// a1 1,1,1 false
// a2 1,3,5 false
// a3 1,4,4 false
// a4 1,5,5 true
func TestGetVisibleRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	n := NewAppendMVCCHandle(obj)
	an1, _ := n.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = types.BuildTS(1, 0)
	an1.Prepare = types.BuildTS(1, 0)
	an1.End = types.BuildTS(1, 0)
	an2, _ := n.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = types.BuildTS(1, 0)
	an2.Prepare = types.BuildTS(3, 0)
	an2.End = types.BuildTS(5, 0)
	an3, _ := n.AddAppendNodeLocked(nil, 2, 3)
	an3.Start = types.BuildTS(1, 0)
	an3.Prepare = types.BuildTS(4, 0)
	an3.End = types.BuildTS(4, 0)
	an4, _ := n.AddAppendNodeLocked(nil, 3, 4)
	an4.Start = types.BuildTS(1, 0)
	an4.Prepare = types.BuildTS(5, 0)
	an4.End = types.BuildTS(5, 0)
	an4.Aborted = true

	// ts=1 maxrow=1, holes={}
	selection, visible, err := n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(1, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), selection.MaxRow)
	assert.True(t, visible)
	assert.Equal(t, 0, selection.Holes.GetCardinality())

	// ts=4 maxrow=3, holes={1}
	selection, visible, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(4, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), selection.MaxRow)
	assert.True(t, visible)
	assert.Equal(t, 1, selection.Holes.GetCardinality())
	assert.True(t, selection.Holes.Contains(1))

	// ts=5 maxrow=3, holes={}
	selection, visible, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(5, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), selection.MaxRow)
	assert.True(t, visible)
	assert.Equal(t, 0, selection.Holes.GetCardinality())

}

func TestPrepareRollbackKeepsAppendRangeAsHole(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	handle := NewAppendMVCCHandle(obj)

	node, _ := handle.AddAppendNodeLocked(nil, 2, 5)
	require.NoError(t, node.PrepareRollback())
	require.True(t, node.IsAborted())
	require.Same(t, node, handle.GetAppendNodeByRowLocked(3))
	require.Equal(t, uint32(5), handle.GetTotalRow())
}

func TestAppendMVCCSelectionsWithUnorderedTimestamps(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	handle := NewAppendMVCCHandle(obj)

	// Physical order is T1 then T2, while prepare and commit order is T2 then T1.
	t1, _ := handle.AddAppendNodeLocked(nil, 0, 2)
	t1.Start = types.BuildTS(1, 0)
	t1.Prepare = types.BuildTS(5, 0)
	t1.End = types.BuildTS(10, 0)
	t2, _ := handle.AddAppendNodeLocked(nil, 2, 4)
	t2.Start = types.BuildTS(2, 0)
	t2.Prepare = types.BuildTS(3, 0)
	t2.End = types.BuildTS(4, 0)

	prepared := handle.GetRowSelectionByTSLocked(types.BuildTS(3, 0))
	require.Equal(t, uint32(4), prepared.MaxRow)
	require.False(t, prepared.Contains(0))
	require.False(t, prepared.Contains(1))
	require.True(t, prepared.Contains(2))
	require.True(t, prepared.Contains(3))

	inRange := handle.GetRowSelectionInRangeLocked(types.BuildTS(3, 0), types.BuildTS(3, 0))
	require.Equal(t, uint32(2), inRange.MinRow)
	require.Equal(t, uint32(4), inRange.MaxRow)
	require.True(t, inRange.Contains(2))

	visible, ok, err := handle.GetVisibleRowLocked(
		context.Background(), MockTxnWithStartTS(types.BuildTS(4, 0)),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(4), visible.MaxRow)
	require.False(t, visible.Contains(0))
	require.True(t, visible.Contains(2))

	handle.RLock()
	collected, commitTS, abort := handle.CollectAppendLocked(
		types.BuildTS(3, 0), types.BuildTS(3, 0), common.DefaultAllocator,
	)
	handle.RUnlock()
	require.Equal(t, inRange, collected)
	require.NotNil(t, commitTS)
	require.NotNil(t, abort)
	defer commitTS.Close()
	defer abort.Close()
	require.Equal(t, 2, commitTS.Length())
	require.Equal(t, types.BuildTS(4, 0), commitTS.Get(0).(types.TS))

	require.False(t, handle.AllAppendsCommittedBeforeLocked(types.BuildTS(6, 0)))
	require.True(t, handle.AllAppendsCommittedBeforeLocked(types.BuildTS(11, 0)))
	require.Equal(t, types.BuildTS(5, 0), handle.GetLatestAppendPrepareTSLocked())
}

func TestAllAppendsCommittedChecksEveryPhysicalNode(t *testing.T) {
	handle := NewAppendMVCCHandle(nil)
	txn := mockTxn()
	txn.PrepareTS = types.BuildTS(3, 0)
	txn.State = txnif.TxnStatePreparing
	handle.OnReplayAppendNode(NewAppendNode(txn, 0, 1, false, handle))
	committed := NewAppendNode(nil, 1, 2, false, handle)
	committed.Prepare = types.BuildTS(2, 0)
	committed.End = types.BuildTS(2, 0)
	handle.OnReplayAppendNode(committed)

	require.False(t, handle.allAppendsCommittedLocked())
	var found []*AppendNode
	require.True(t, handle.CollectUncommittedANodesPreparedBeforeLocked(
		types.BuildTS(4, 0), func(node *AppendNode) { found = append(found, node) },
	))
	require.Len(t, found, 1)
}

func BenchmarkAppendMVCCRowSelection(b *testing.B) {
	for _, nodeCount := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("nodes-%d", nodeCount), func(b *testing.B) {
			handle := NewAppendMVCCHandle(nil)
			for i := 0; i < nodeCount; i++ {
				node := NewAppendNode(nil, uint32(i), uint32(i+1), false, handle)
				node.Prepare = types.BuildTS(int64(nodeCount-i), 0)
				handle.OnReplayAppendNode(node)
			}
			query := types.BuildTS(int64(nodeCount/2+1), 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				selection := handle.GetRowSelectionByTSLocked(query)
				if selection.IsEmpty() {
					b.Fatal("expected selected rows")
				}
			}
		})
	}
}
