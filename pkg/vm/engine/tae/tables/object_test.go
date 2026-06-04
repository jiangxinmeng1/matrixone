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

package tables

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	api "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetActiveRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	ts1 := types.BuildTS(1, 0)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mvcc := updates.NewAppendMVCCHandle(obj)
	// blk := &dataBlock{
	// 	mvcc: mvcc,
	// }
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)
	mnode := &memoryNode{
		object: b,
	}
	blk := &aobject{baseObject: b}

	mnode.Ref()
	n := NewNode(mnode)
	blk.node.Store(n)

	// appendnode1 [0,1)
	an1, _ := mvcc.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = ts1
	an1.Prepare = ts1
	an1.End = ts1

	// appendnode1 [1,2)
	an2, _ := mvcc.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = ts1
	an2.Prepare = ts1
	an2.End = ts1

	// index uint8(1)-0,1
	vec := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
	vec.Append(int8(1), false)
	vec.Append(int8(1), false)
	idx := indexwrapper.NewMutIndex(types.T_int8.ToType())
	err := idx.BatchUpsert(vec.GetDownstreamVector(), 0)
	assert.NoError(t, err)
	blk.node.Load().MustMNode().pkIndex = idx
}

func TestApplyAppendLockedPadsMissingColumnsForUpgradedSchema(t *testing.T) {
	defer testutils.AfterTest(t)()

	oldSchema := catalog.MockSchema(2, 0)
	newSchema := oldSchema.Clone()
	require.NoError(t, newSchema.ApplyAlterTable(
		api.NewAddColumnReq(0, 0, "added_flag", types.NewProtoType(types.T_int8), 2),
	))

	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(oldSchema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)

	mnode := &memoryNode{
		object:      b,
		writeSchema: newSchema,
	}

	bat := containers.BuildBatch(
		oldSchema.AllNames(),
		oldSchema.AllTypes(),
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer bat.Close()
	for _, vec := range bat.Vecs {
		vec.Append(nil, true)
	}

	from, err := mnode.ApplyAppendLocked(bat)
	require.NoError(t, err)
	require.Equal(t, 0, from)

	for _, vec := range mnode.data.Vecs {
		require.Equal(t, bat.Length(), vec.Length())
	}
	addedVec := mnode.data.GetVectorByName("added_flag")
	require.Equal(t, bat.Length(), addedVec.Length())
	require.True(t, addedVec.IsNull(0))

	pool := containers.NewVectorPool("upgrade-compat", 4, containers.WithMPool(common.DefaultAllocator))
	require.NotPanics(t, func() {
		win := mnode.data.CloneWindowWithPool(0, bat.Length(), pool)
		win.Close()
	})
}

func TestOverwriteAtLockedPadsAndWritesAtOffset(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(2, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)

	mnode := &memoryNode{
		object:      b,
		writeSchema: schema,
	}
	bat := catalog.MockBatch(schema, 2)
	defer bat.Close()

	from, err := mnode.OverwriteAtLocked(bat, 3)
	require.NoError(t, err)
	require.Equal(t, 3, from)
	require.Equal(t, 5, mnode.data.Length())

	for _, vec := range mnode.data.Vecs {
		for i := 0; i < 3; i++ {
			require.True(t, vec.IsNull(i))
		}
	}
	for srcPos, attr := range bat.Attrs {
		dest := mnode.data.GetVectorByName(attr)
		for i := 0; i < bat.Length(); i++ {
			require.Equal(t, bat.Vecs[srcPos].Get(i), dest.Get(3+i))
			require.Equal(t, bat.Vecs[srcPos].IsNull(i), dest.IsNull(3+i))
		}
	}
}

func TestSharedAppenderConcurrentPrepareAllocatesDisjointRows(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 100
	c, _, tableData := newSharedAppendTestTable(t, schema)
	defer c.Close()

	const txnCnt = 32
	type prepareResult struct {
		startRow  uint32
		allocated uint32
		err       error
	}
	results := make(chan prepareResult, txnCnt)
	var wg sync.WaitGroup
	for i := 0; i < txnCnt; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			appender, _, startRow, allocated, _, err := tableData.PrepareSharedAppend(
				false,
				schema,
				updates.MockTxnWithStartTS(types.BuildTS(int64(i+1), 0)),
				1,
				false,
			)
			if appender != nil {
				defer appender.Close()
			}
			results <- prepareResult{startRow: startRow, allocated: allocated, err: err}
		}(i)
	}
	wg.Wait()
	close(results)

	seen := make(map[uint32]struct{}, txnCnt)
	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, uint32(1), result.allocated)
		require.NotContains(t, seen, result.startRow)
		seen[result.startRow] = struct{}{}
	}
	require.Len(t, seen, txnCnt)
}

func TestSharedAppenderOutOfOrderApplyWritesAtDestRowsAndPKIndex(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 10
	c, _, tableData := newSharedAppendTestTable(t, schema)
	defer c.Close()

	type preparedAppend struct {
		appender data.ObjectAppender
		startRow uint32
		rows     uint32
		bat      *containers.Batch
		base     int32
	}
	prepares := make([]preparedAppend, 3)
	for i, base := range []int32{10, 20, 30} {
		appender, _, startRow, allocated, _, err := tableData.PrepareSharedAppend(
			false,
			schema,
			updates.MockTxnWithStartTS(types.BuildTS(int64(i+1), 0)),
			2,
			false,
		)
		require.NoError(t, err)
		require.Equal(t, uint32(2), allocated)
		bat := mockInt32BatchWithBase(schema, 2, base)
		prepares[i] = preparedAppend{
			appender: appender,
			startRow: startRow,
			rows:     allocated,
			bat:      bat,
			base:     base,
		}
		defer appender.Close()
		defer bat.Close()
	}

	for _, idx := range []int{2, 0, 1} {
		_, err := prepares[idx].appender.ApplyAppendAt(prepares[idx].bat, nil, prepares[idx].startRow)
		require.NoError(t, err)
	}

	obj := prepares[0].appender.GetMeta().(*catalog.ObjectEntry).GetObjectData().(*aobject)
	pinned := obj.PinNode()
	defer pinned.Unref()
	mnode := pinned.MustMNode()
	pkVec := mnode.data.Vecs[schema.GetSingleSortKeyIdx()]
	for _, prepared := range prepares {
		for i := 0; i < int(prepared.rows); i++ {
			row := int(prepared.startRow) + i
			require.Equal(t, prepared.base+int32(i), pkVec.Get(row))
			require.False(t, pkVec.IsNull(row))
		}
	}

	rows, err := mnode.pkIndex.GetActiveRow(int32(30))
	require.NoError(t, err)
	require.Equal(t, []uint32{prepares[2].startRow}, rows)
}

func TestSharedAppenderRefPreventsCompact(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(2, 0)
	c, _, tableData := newSharedAppendTestTable(t, schema)
	defer c.Close()

	appender, _, _, allocated, _, err := tableData.PrepareSharedAppend(
		false,
		schema,
		updates.MockTxnWithStartTS(types.BuildTS(1, 0)),
		1,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(1), allocated)
	obj := appender.GetMeta().(*catalog.ObjectEntry).GetObjectData().(*aobject)
	require.Greater(t, obj.RefCount(), int64(0))

	ok, reason := obj.PrepareCompactInfo()
	require.False(t, ok)
	require.Contains(t, reason, "refcount")

	appender.Close()
	require.Equal(t, int64(0), obj.RefCount())
}

func TestSharedAppenderLargeBatchSpansObjects(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 3
	c, _, tableData := newSharedAppendTestTable(t, schema)
	defer c.Close()

	remaining := uint32(8)
	expectedAllocs := []uint32{3, 3, 2}
	var allocs []uint32
	objects := make(map[objectio.ObjectId]struct{})
	for remaining > 0 {
		appender, _, _, allocated, _, err := tableData.PrepareSharedAppend(
			false,
			schema,
			updates.MockTxnWithStartTS(types.BuildTS(int64(len(allocs)+1), 0)),
			remaining,
			false,
		)
		require.NoError(t, err)
		allocs = append(allocs, allocated)
		objects[*appender.GetMeta().(*catalog.ObjectEntry).ID()] = struct{}{}
		appender.Close()
		remaining -= allocated
	}
	require.Equal(t, expectedAllocs, allocs)
	require.Len(t, objects, 3)
}

func newSharedAppendTestTable(t *testing.T, schema *catalog.Schema) (*catalog.Catalog, *catalog.TableEntry, *dataTable) {
	rt := dbutils.NewRuntime()
	factory := NewDataFactory(rt, "")
	c := catalog.MockCatalog(factory)
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, factory.MakeTableFactory())
	require.NoError(t, err)
	return c, table, table.GetTableData().(*dataTable)
}

func mockInt32BatchWithBase(schema *catalog.Schema, rows int, base int32) *containers.Batch {
	bat := catalog.MockBatch(schema, rows)
	for _, vec := range bat.Vecs {
		if vec.GetType().Oid != types.T_int32 {
			continue
		}
		for i := 0; i < rows; i++ {
			vec.Update(i, base+int32(i), false)
		}
	}
	return bat
}
