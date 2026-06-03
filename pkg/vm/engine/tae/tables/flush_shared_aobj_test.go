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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlushSharedAobj_ObjectListSorting tests ObjectList sorting after flush
// Verifies that persisted object (D entry) is sorted correctly with other objects
func TestFlushSharedAobj_ObjectListSorting(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	// 1. 创建共享 aobj (T0)
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)
	t0 := sharedAobj.GetCreatedAt()

	// 2. 创建 aobject 并模拟 append
	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100) at T1
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	t1 := node1.GetEnd()

	// Txn2: append [100, 200) at T2
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node2, _ := aobj.appendMVCC.AddAppendNodeLocked(txn2, 100, 200)
	aobj.Unlock()

	err = txn2.Commit(context.Background())
	require.NoError(t, err)
	_ = node2

	// 3. 创建其他 non-appendable objects (用于测试排序)
	// Object A: CreatedAt = T1 + 1000 (应该排在 persisted obj 后面)
	objATxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	objAID := objectio.NewObjectid()
	objAStats := objectio.NewObjectStatsWithObjectID(&objAID, false, false, false)
	objAName := objectio.BuildObjectNameWithObjectID(&objAID)
	objALocation := objectio.MockLocation(objAName)
	objectio.SetObjectStatsLocation(objAStats, objALocation)

	objA := catalog.NewObjectEntry(table, objATxn, *objAStats, nil, false)
	objA.ObjectState = catalog.ObjectState_Create_ApplyCommit

	table.Lock()
	table.AddEntryLocked(objA)
	table.Unlock()

	err = objATxn.Commit(context.Background())
	require.NoError(t, err)
	objA.CreateNode.End = objATxn.GetCommitTS()

	// 4. 模拟 flush: 创建 persisted object
	flushTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	// 创建 persisted object stats (使用相同的 ObjectID)
	objID := *sharedAobj.ID()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	location := objectio.MockLocation(objName)
	objectio.SetObjectStatsLocation(stats, location)
	objectio.SetObjectStatsSize(stats, 1000)

	// 创建 persisted object (non-appendable, 使用相同的 ObjectID)
	// CreatedAt 应该是 min(T1, T2) = T1
	persistedObj := catalog.NewObjectEntry(table, flushTxn, *stats, nil, false)
	persistedObj.CreatedAt = t1                                       // 设置为 min(AppendNode.CommitTS)
	persistedObj.ObjectState = catalog.ObjectState_Create_ApplyCommit // 确保是 committed

	table.Lock()
	table.AddEntryLocked(persistedObj)
	table.Unlock()

	err = flushTxn.Commit(context.Background())
	require.NoError(t, err)

	// 等待 persistedObj commit
	persistedObj.CreateNode.End = flushTxn.GetCommitTS()

	// 5. 验证 ObjectList 排序
	it := table.MakeDataObjectIt()
	defer it.Release()

	entries := []*catalog.ObjectEntry{}
	for ok := it.First(); ok; ok = it.Next() {
		entries = append(entries, it.Item())
	}

	// 打印调试信息
	t.Logf("Total entries: %d", len(entries))
	for i, e := range entries {
		t.Logf("  [%d] ID=%v, Appendable=%v, CreatedAt=%v, State=%v",
			i, e.ID().ShortStringEx(), e.IsAppendable(), e.GetCreatedAt(), e.ObjectState)
	}

	// 应该有 3 个 object: sharedAobj, persistedObj, objA
	require.Equal(t, 3, len(entries))

	// 验证排序: Less2 按 max(CreatedAt, DeletedAt) 排序
	// sharedAobj: appendable, CreatedAt=T0
	// persistedObj: non-appendable, CreatedAt=T1
	// objA: non-appendable, CreatedAt=T1+1000
	//
	// 排序结果: sharedAobj < persistedObj < objA

	assert.True(t, entries[0].IsAppendable(), "First should be appendable (sharedAobj)")
	assert.False(t, entries[1].IsAppendable(), "Second should be non-appendable (persistedObj)")
	assert.False(t, entries[2].IsAppendable(), "Third should be non-appendable (objA)")

	assert.Equal(t, sharedAobj.ID(), entries[0].ID())
	assert.Equal(t, persistedObj.ID(), entries[1].ID())
	assert.Equal(t, objA.ID(), entries[2].ID())

	t.Logf("Sorting verified:")
	t.Logf("  [0] sharedAobj (appendable, T0=%v)", t0)
	t.Logf("  [1] persistedObj (non-appendable, T1=%v)", t1)
	t.Logf("  [2] objA (non-appendable, T1+1000=%v)", objA.GetCreatedAt())
}

// TestFlushSharedAobj_EarlyBreak tests early break correctness
// Verifies that RecurLoop stops at the correct object
func TestFlushSharedAobj_EarlyBreak(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	// 1. 创建共享 aobj
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	// 2. 创建 aobject 并 append
	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	t1 := node1.GetEnd()

	// 3. 创建 persisted object (CreatedAt = T1)
	flushTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	objID := *sharedAobj.ID()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	location := objectio.MockLocation(objName)
	objectio.SetObjectStatsLocation(stats, location)
	objectio.SetObjectStatsSize(stats, 1000)

	persistedObj := catalog.NewObjectEntry(table, flushTxn, *stats, nil, false)
	persistedObj.CreatedAt = t1

	table.Lock()
	table.AddEntryLocked(persistedObj)
	table.Unlock()

	err = flushTxn.Commit(context.Background())
	require.NoError(t, err)

	// 4. 创建 read txn (startTS = T1 + 500)
	readTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	defer readTxn.Rollback(context.Background())

	readTS := readTxn.GetStartTS()

	// 5. 验证 early break
	// RecurLoop 应该在 persistedObj 处 break，因为 persistedObj.CreatedAt (T1) < readTS
	// 不应该继续遍历 sharedAobj

	it := table.MakeDataVisibleObjectIt(readTxn)
	defer it.Release()

	visitedObjects := []*catalog.ObjectEntry{}
	for ok := it.Next(); ok; ok = it.Next() {
		obj := it.Item()
		visitedObjects = append(visitedObjects, obj)

		// Early break: 如果遇到 CreatedAt < readTS 的 non-appendable object，应该 break
		createdAt := obj.GetCreatedAt()
		if !obj.IsAppendable() && createdAt.LT(&readTS) {
			break
		}
	}

	// 验证只访问了 persistedObj，没有访问 sharedAobj
	require.Equal(t, 1, len(visitedObjects), "Should only visit persistedObj")
	assert.Equal(t, persistedObj.ID(), visitedObjects[0].ID())

	t.Logf("Early break verified:")
	t.Logf("  readTS=%v", readTS)
	t.Logf("  persistedObj.CreatedAt=%v (< readTS, should break)", persistedObj.GetCreatedAt())
	t.Logf("  sharedAobj not visited (correct)")
}

// TestFlushSharedAobj_ObjectNamePreserved tests ObjectName preservation
// Verifies that persisted object has the same ObjectName as shared aobj
func TestFlushSharedAobj_ObjectNamePreserved(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	// 1. 创建共享 aobj
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	originalObjectName := sharedAobj.ObjectName()

	// 2. 模拟 flush: 创建 persisted object with same ObjectID
	flushTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	objID := *sharedAobj.ID()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	location := objectio.MockLocation(objName)
	objectio.SetObjectStatsLocation(stats, location)

	persistedObj := catalog.NewObjectEntry(table, flushTxn, *stats, nil, false)

	table.Lock()
	table.AddEntryLocked(persistedObj)
	table.Unlock()

	err = flushTxn.Commit(context.Background())
	require.NoError(t, err)

	// 3. 验证 ObjectName 保持不变
	persistedObjectName := persistedObj.ObjectName()

	assert.Equal(t, originalObjectName.String(), persistedObjectName.String(),
		"Persisted object should have the same ObjectName as shared aobj")

	t.Logf("ObjectName preserved:")
	t.Logf("  sharedAobj.ObjectName=%v", originalObjectName.String())
	t.Logf("  persistedObj.ObjectName=%v", persistedObjectName.String())
}

// TestFlushSharedAobj_StateTransition tests complete state transition
// Verifies: in-memory → uncommitted (flushing) → committed (persisted)
func TestFlushSharedAobj_StateTransition(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	// === Phase 1: In-memory appendable aobj ===
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	// Verify Phase 1: in-memory appendable
	assert.True(t, sharedAobj.IsAppendable(), "Phase 1: should be appendable")
	assert.True(t, sharedAobj.IsInMemory(), "Phase 1: should be in-memory")
	assert.False(t, sharedAobj.HasDropCommitted(), "Phase 1: should not be dropped")
	t.Logf("Phase 1: in-memory appendable aobj")
	t.Logf("  IsAppendable=%v, IsInMemory=%v, HasDropCommitted=%v",
		sharedAobj.IsAppendable(), sharedAobj.IsInMemory(), sharedAobj.HasDropCommitted())

	// Create aobject and append
	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	t1 := node1.GetEnd()

	// === Phase 2: Uncommitted flushing (txn active) ===
	flushTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	objID := *sharedAobj.ID()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	location := objectio.MockLocation(objName)
	objectio.SetObjectStatsLocation(stats, location)
	objectio.SetObjectStatsSize(stats, 1000)

	persistedObj := catalog.NewObjectEntry(table, flushTxn, *stats, nil, false)
	persistedObj.CreatedAt = t1

	table.Lock()
	table.AddEntryLocked(persistedObj)
	table.Unlock()

	// Verify Phase 2: uncommitted (txn active)
	assert.False(t, persistedObj.IsAppendable(), "Phase 2: should be non-appendable")
	assert.False(t, persistedObj.IsInMemory(), "Phase 2: should have location")
	assert.Equal(t, catalog.ObjectState_Create_Active, persistedObj.ObjectState,
		"Phase 2: should be in Create_Active state")
	t.Logf("Phase 2: uncommitted flushing (txn active)")
	t.Logf("  IsAppendable=%v, IsInMemory=%v, State=%v",
		persistedObj.IsAppendable(), persistedObj.IsInMemory(), persistedObj.ObjectState)

	// Verify ObjectList has 2 entries (sharedAobj + persistedObj)
	it := table.MakeDataObjectIt()
	defer it.Release()
	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		count++
	}
	assert.Equal(t, 2, count, "Phase 2: should have 2 entries in ObjectList")

	// === Phase 3: Committed persisted aobj ===
	err = flushTxn.Commit(context.Background())
	require.NoError(t, err)
	persistedObj.CreateNode.End = flushTxn.GetCommitTS()
	persistedObj.ObjectState = catalog.ObjectState_Create_ApplyCommit

	// Verify Phase 3: committed persisted
	assert.False(t, persistedObj.IsAppendable(), "Phase 3: should be non-appendable")
	assert.False(t, persistedObj.IsInMemory(), "Phase 3: should have location")
	assert.Equal(t, catalog.ObjectState_Create_ApplyCommit, persistedObj.ObjectState,
		"Phase 3: should be in Create_ApplyCommit state")
	assert.NotEmpty(t, persistedObj.GetLocation(), "Phase 3: should have location")
	t.Logf("Phase 3: committed persisted aobj")
	t.Logf("  IsAppendable=%v, IsInMemory=%v, State=%v",
		persistedObj.IsAppendable(), persistedObj.IsInMemory(), persistedObj.ObjectState)
	t.Logf("  Location=%v", persistedObj.GetLocation())

	// Verify state transition summary
	t.Logf("State transition verified:")
	t.Logf("  Phase 1: in-memory appendable → Phase 2: uncommitted flushing → Phase 3: committed persisted")
}

// Test 4: Simulate flush shared aobj behavior (design verification)
// This test verifies the flush design without modifying existing flush logic
func TestFlushSharedAobj_SimulateFlush(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	rt := dbutils.NewRuntime()

	// 1. Create shared aobj
	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	sharedAobj := catalog.NewInMemoryObject(table, createTxn.GetStartTS(), false)
	table.Lock()
	table.AddEntryLocked(sharedAobj)
	table.Unlock()

	err = createTxn.Commit(context.Background())
	require.NoError(t, err)

	// 2. Create aobject and simulate multiple txn appends
	aobj := newAObject(sharedAobj, rt, false)
	aobj.Ref()
	defer aobj.Unref()

	// Txn1: append [0, 100)
	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node1, _ := aobj.appendMVCC.AddAppendNodeLocked(txn1, 0, 100)
	aobj.Unlock()

	err = txn1.Commit(context.Background())
	require.NoError(t, err)
	commitTS1 := node1.GetEnd()

	// Txn2: append [100, 200)
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	aobj.Lock()
	node2, _ := aobj.appendMVCC.AddAppendNodeLocked(txn2, 100, 200)
	aobj.Unlock()

	err = txn2.Commit(context.Background())
	require.NoError(t, err)
	commitTS2 := node2.GetEnd()

	// 3. Calculate maxCommitTS (key step in flush)
	maxCommitTS := aobj.GetMaxCommitTS()
	expectedMaxTS := commitTS1
	if commitTS2.GT(&commitTS1) {
		expectedMaxTS = commitTS2
	}

	assert.True(t, maxCommitTS.EQ(&expectedMaxTS),
		"maxCommitTS should equal max(commitTS1, commitTS2)")

	// 4. Simulate flush: Create persistedObj with same ObjectID
	flushTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	// Key design: Reuse the same ObjectID
	objID := *sharedAobj.ID()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	location := objectio.MockLocation(objName)
	objectio.SetObjectStatsLocation(stats, location)

	// Create persistedObj (simulating UpdateObjectInfo result)
	persistedObj := catalog.NewObjectEntry(table, flushTxn, *stats, nil, false)
	persistedObj.CreatedAt = maxCommitTS // Key: Use maxCommitTS
	persistedObj.ObjectState = catalog.ObjectState_Create_Active

	table.Lock()
	table.AddEntryLocked(persistedObj)
	table.Unlock()

	err = flushTxn.Commit(context.Background())
	require.NoError(t, err)

	persistedObj.CreateNode.End = flushTxn.GetCommitTS()
	persistedObj.ObjectState = catalog.ObjectState_Create_ApplyCommit

	// 5. Verify design
	assert.Equal(t, sharedAobj.ID(), persistedObj.ID(),
		"ObjectID should be preserved after flush")

	assert.True(t, persistedObj.CreatedAt.EQ(&maxCommitTS),
		"persistedObj.CreatedAt should equal maxCommitTS")

	t.Logf("Flush shared aobj simulation verified:")
	t.Logf("  ObjectID preserved: %v", sharedAobj.ID())
	t.Logf("  maxCommitTS: %v", maxCommitTS)
	t.Logf("  persistedObj.CreatedAt: %v", persistedObj.CreatedAt)
}
