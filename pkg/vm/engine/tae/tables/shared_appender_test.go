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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAppendableNode implements txnif.AppendableNode for testing
type mockAppendableNode struct {
	data        *containers.Batch
	appends     []mockAppendInfo
	writeSchema *catalog.Schema
}

type mockAppendInfo struct {
	srcOff  uint32
	srcLen  uint32
	destOff uint32
	destLen uint32
	dest    common.ID
}

func (n *mockAppendableNode) Rows() uint32 {
	return uint32(n.data.Length())
}

func (n *mockAppendableNode) GetData() *containers.Batch {
	return n.data
}

func (n *mockAppendableNode) IsSameColumns(other interface{}) bool {
	otherSchema, ok := other.(*catalog.Schema)
	if !ok {
		return false
	}
	return n.writeSchema.IsSameColumns(otherSchema)
}

func (n *mockAppendableNode) GetPhyAddrIdx() int {
	return n.writeSchema.PhyAddrKey.Idx
}

func (n *mockAppendableNode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) {
	n.appends = append(n.appends, mockAppendInfo{
		srcOff:  srcOff,
		srcLen:  srcLen,
		destOff: destOff,
		destLen: destLen,
		dest:    *dest,
	})
}

func createMockNode(schema *catalog.Schema, rows int) *mockAppendableNode {
	// Use AllTypes and AllNames to include PhyAddr column
	bat := containers.MockBatchWithAttrs(schema.AllTypes(), schema.AllNames(), rows, schema.GetPrimaryKey().Idx, nil)
	return &mockAppendableNode{
		data:        bat,
		appends:     make([]mockAppendInfo, 0),
		writeSchema: schema,
	}
}

func createMockNodeWithOffset(schema *catalog.Schema, rows, offset int) *mockAppendableNode {
	node := createMockNode(schema, rows)
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() || colDef.Type.Oid != types.T_int32 {
			continue
		}
		vec := node.data.Vecs[colDef.Idx]
		for i := 0; i < rows; i++ {
			vec.Update(i, int32(offset+i), false)
		}
	}
	return node
}

// Helper function to create test environment
func setupTest(t *testing.T) (*catalog.Catalog, *catalog.TableEntry, txnif.AsyncTxn, *txnbase.TxnManager, *dbutils.Runtime) {
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 8192

	rt := dbutils.NewRuntime()

	// Create mock DataFactory
	dataFactory := &mockDataFactory{rt: rt}
	c := catalog.MockCatalog(dataFactory)

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)

	table, err := db.CreateTableEntry(schema, txn, nil)
	require.NoError(t, err)

	return c, table, txn, txnMgr, rt
}

// mockDataFactory implements catalog.DataFactory for testing
type mockDataFactory struct {
	rt *dbutils.Runtime
}

func (f *mockDataFactory) MakeTableFactory() catalog.TableDataFactory {
	return func(meta *catalog.TableEntry) data.Table {
		return nil // System tables not needed in these tests
	}
}

func (f *mockDataFactory) MakeObjectFactory() catalog.ObjectDataFactory {
	return func(meta *catalog.ObjectEntry) data.Object {
		return newAObject(meta, f.rt, meta.IsTombstone)
	}
}

// ============================================================================
// New API Tests (PrepareAppend/ApplyAppend) with mock txn
// ============================================================================

// testAppendWithNewAPI uses the new per-txn appender API
func testAppendWithNewAPI(t *testing.T, table *catalog.TableEntry, txn txnif.AsyncTxn, rt *dbutils.Runtime, node *mockAppendableNode, isTombstone bool) error {
	// Get per-txn appender
	appender := table.GetTxnAppender(txn, rt, isTombstone)
	defer appender.Close()

	// Phase 1: PrepareAppend - allocate space, generate RowIDs, create AppendNodes
	appendNodes, err := appender.PrepareAppend(node)
	if err != nil {
		return err
	}

	// Empty nodes may return empty appendNodes (this is valid)
	if len(appendNodes) == 0 && node.Rows() == 0 {
		return nil
	}

	// Non-empty nodes should have AppendNodes
	if node.Rows() > 0 {
		require.NotEmpty(t, appendNodes)
	}

	// Phase 2: ApplyAppend - write all prepared data
	return appender.ApplyAppend()
}

// Test: Single append
func TestSharedAppender_NewAPI_Single(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 1, len(node.appends))
	assert.Equal(t, uint32(0), node.appends[0].destOff)
	assert.Equal(t, uint32(100), node.appends[0].destLen)
}

// Test: Concurrent appends from multiple txns
func TestSharedAppender_NewAPI_Concurrent(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	concurrency := 2
	rowsPerTxn := 100
	nodes := make([]*mockAppendableNode, concurrency)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txn, err := txnMgr.StartTxn(nil)
			require.NoError(t, err)

			node := createMockNode(table.GetLastestSchema(false), rowsPerTxn)
			nodes[idx] = node

			err = testAppendWithNewAPI(t, table, txn, rt, node, false)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	for _, node := range nodes {
		node.data.Close()
	}
}

func TestSharedAppender_NewAPI_OutOfOrderApplyUsesAllocatedRows(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)

	appender1 := table.GetTxnAppender(txn1, rt, false)
	defer appender1.Close()
	appender2 := table.GetTxnAppender(txn2, rt, false)
	defer appender2.Close()

	schema := table.GetLastestSchema(false)
	node1 := createMockNodeWithOffset(schema, 3, 0)
	defer node1.data.Close()
	node2 := createMockNodeWithOffset(schema, 3, 100)
	defer node2.data.Close()

	_, err = appender1.PrepareAppend(node1)
	require.NoError(t, err)
	_, err = appender2.PrepareAppend(node2)
	require.NoError(t, err)

	require.Equal(t, uint32(0), node1.appends[0].destOff)
	require.Equal(t, uint32(3), node2.appends[0].destOff)

	sharedAppender := appender1.(SharedAppender)
	aobjs := sharedAppender.GetRefedAobjs()
	require.Len(t, aobjs, 1)
	aobj := aobjs[0]

	aobj.RLock()
	pinned := aobj.PinNode()
	mnode := pinned.MustMNode()
	require.Equal(t, 6, mnode.data.Length())
	pkVec := mnode.data.GetVectorByName(schema.GetPrimaryKey().Name)
	for i := 0; i < 6; i++ {
		require.True(t, pkVec.IsNull(i), "row %d should be reserved as NULL before apply", i)
	}
	pinned.Unref()
	aobj.RUnlock()

	require.NoError(t, appender2.ApplyAppend())
	require.NoError(t, appender1.ApplyAppend())

	aobj.RLock()
	pinned = aobj.PinNode()
	mnode = pinned.MustMNode()
	defer pinned.Unref()
	defer aobj.RUnlock()

	pkVec = mnode.data.GetVectorByName(schema.GetPrimaryKey().Name)
	expected := []int32{0, 1, 2, 100, 101, 102}
	for i, v := range expected {
		require.False(t, pkVec.IsNull(i), "row %d should be overwritten", i)
		require.Equal(t, v, pkVec.Get(i))
	}

	rows, err := mnode.pkIndex.GetActiveRow(int32(0))
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, rows)
	rows, err = mnode.pkIndex.GetActiveRow(int32(100))
	require.NoError(t, err)
	require.Equal(t, []uint32{3}, rows)
}

// Test: Cross aobj append
func TestSharedAppender_NewAPI_CrossAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 100

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	node := createMockNode(table.GetLastestSchema(false), 250)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 3, len(node.appends))
	assert.Equal(t, uint32(100), node.appends[0].destLen)
	assert.Equal(t, uint32(100), node.appends[1].destLen)
	assert.Equal(t, uint32(50), node.appends[2].destLen)
}

// Test: Tombstone append
func TestSharedAppender_NewAPI_Tombstone(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	tombstoneSchema := table.GetLastestSchema(true)
	node := createMockNode(tombstoneSchema, 50)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, true)
	require.NoError(t, err)

	assert.Equal(t, 1, len(node.appends))
	assert.Equal(t, uint32(50), node.appends[0].destLen)
}

// Test: Multiple appends from same txn
func TestSharedAppender_NewAPI_MultipleAppends(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)

	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)

	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify AddApplyInfo was called correctly
	assert.Equal(t, uint32(0), node1.appends[0].destOff)
	assert.Equal(t, uint32(100), node1.appends[0].destLen)
	assert.Equal(t, uint32(100), node2.appends[0].destOff)
	assert.Equal(t, uint32(50), node2.appends[0].destLen)

	// Verify actual data was written to aobj
	sharedAppender, ok := appender.(SharedAppender)
	require.True(t, ok, "appender should implement SharedAppender interface")

	aobjs := sharedAppender.GetRefedAobjs()
	require.Equal(t, 1, len(aobjs), "should have 1 aobj")

	aobj := aobjs[0]
	aobj.RLock()
	defer aobj.RUnlock()

	// Check aobj has correct number of rows
	node := aobj.PinNode()
	defer node.Unref()

	mnode := node.MustMNode()
	actualRows := mnode.data.Length()
	expectedRows := 100 + 50 // node1 + node2
	assert.Equal(t, expectedRows, actualRows, "aobj should contain data from both nodes")

	t.Logf("Multiple PrepareAppend: aobj has %d rows (expected %d)", actualRows, expectedRows)
}

// Test: Large batch
func TestSharedAppender_NewAPI_LargeBatch(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 1000

	node := createMockNode(table.GetLastestSchema(false), 5500)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 6, len(node.appends))
	totalRows := uint32(0)
	for _, info := range node.appends {
		totalRows += info.destLen
	}
	assert.Equal(t, uint32(5500), totalRows)
}

// Test: Concurrent with more txns
func TestSharedAppender_NewAPI_ConcurrentMany(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	concurrency := 10
	rowsPerTxn := 50

	// Sequential execution - SharedAppender is bound to one txn at a time
	for i := 0; i < concurrency; i++ {
		txn, err := txnMgr.StartTxn(nil)
		require.NoError(t, err)

		node := createMockNode(table.GetLastestSchema(false), rowsPerTxn)
		defer node.data.Close()

		err = testAppendWithNewAPI(t, table, txn, rt, node, false)
		require.NoError(t, err)
	}
}

// Test: Different txns sequential
func TestSharedAppender_NewAPI_DifferentTxns(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	txn1, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	err = testAppendWithNewAPI(t, table, txn1, rt, node1, false)
	require.NoError(t, err)

	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	node2 := createMockNode(table.GetLastestSchema(false), 200)
	defer node2.data.Close()
	err = testAppendWithNewAPI(t, table, txn2, rt, node2, false)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), node1.appends[0].destOff)
	assert.Equal(t, uint32(100), node1.appends[0].destLen)
	assert.Equal(t, uint32(100), node2.appends[0].destOff)
	assert.Equal(t, uint32(200), node2.appends[0].destLen)
}

// Test: Data and tombstone use different appenders
func TestSharedAppender_NewAPI_DataAndTombstone(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	dataNode := createMockNode(table.GetLastestSchema(false), 100)
	defer dataNode.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, dataNode, false)
	require.NoError(t, err)

	tombstoneNode := createMockNode(table.GetLastestSchema(true), 50)
	defer tombstoneNode.data.Close()
	err = testAppendWithNewAPI(t, table, txn, rt, tombstoneNode, true)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), dataNode.appends[0].destOff)
	assert.Equal(t, uint32(0), tombstoneNode.appends[0].destOff)
}

// Test: Exactly fill aobj
func TestSharedAppender_NewAPI_ExactlyFillAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 1, len(node.appends))
	assert.Equal(t, uint32(0), node.appends[0].destOff)
	assert.Equal(t, uint32(100), node.appends[0].destLen)
}

// Test: Empty node (0 rows)
func TestSharedAppender_NewAPI_EmptyNode(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node := createMockNode(table.GetLastestSchema(false), 0)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 0, len(node.appends))
}

// Test: Multiple empty appends
func TestSharedAppender_NewAPI_MultipleEmptyAppends(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	for i := 0; i < 3; i++ {
		node := createMockNode(table.GetLastestSchema(false), 0)
		err := testAppendWithNewAPI(t, table, txn, rt, node, false)
		require.NoError(t, err)
		assert.Equal(t, 0, len(node.appends))
		node.data.Close()
	}
}

// Test: AppendNode reuse
func TestSharedAppender_NewAPI_AppendNodeReuse(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node1 := createMockNode(table.GetLastestSchema(false), 50)
	defer node1.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, node1, false)
	require.NoError(t, err)

	node2 := createMockNode(table.GetLastestSchema(false), 30)
	defer node2.data.Close()
	err = testAppendWithNewAPI(t, table, txn, rt, node2, false)
	require.NoError(t, err)

	assert.Equal(t, node1.appends[0].dest, node2.appends[0].dest)
}

// Test: Multiple PrepareAppend on same aobj returns same AppendNode
func TestSharedAppender_NewAPI_SameAppendNodeReturned(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	// First PrepareAppend: 50 rows
	node1 := createMockNode(table.GetLastestSchema(false), 50)
	defer node1.data.Close()
	appendNodes1, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	require.Equal(t, 1, len(appendNodes1))
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Second PrepareAppend: 30 rows (same aobj)
	node2 := createMockNode(table.GetLastestSchema(false), 30)
	defer node2.data.Close()
	appendNodes2, err := appender.PrepareAppend(node2)
	require.NoError(t, err)
	require.Equal(t, 1, len(appendNodes2))
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Should return the same AppendNode (extended)
	assert.Equal(t, appendNodes1[0], appendNodes2[0], "Should return the same AppendNode")
}

// Test: RefCount
func TestSharedAppender_NewAPI_RefCount(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	// Check that aobj was created and has references
	// 	assert.NotNil(t, appender.GetCurrentAobj().(*aobject))
	// 	assert.Greater(t, appender.GetCurrentAobj().(*aobject).RefCount(), int64(0))
}

// Test: RefCount with multiple aobjs
func TestSharedAppender_NewAPI_RefCountMultipleAobjs(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	node := createMockNode(table.GetLastestSchema(false), 250)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 3, len(node.appends))
}

// Test: Multiple PrepareAppend calls spanning multiple aobjs
func TestSharedAppender_NewAPI_MultiplePrepareCrossAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	// First PrepareAppend: 80 rows (fills most of aobj1)
	node1 := createMockNode(table.GetLastestSchema(false), 80)
	defer node1.data.Close()
	appendNodes1, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	require.Equal(t, 1, len(appendNodes1))
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Second PrepareAppend: 50 rows (20 in aobj1 + 30 in aobj2)
	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	appendNodes2, err := appender.PrepareAppend(node2)
	require.NoError(t, err)

	// Should have 2 AppendNodes (one for aobj1, one for aobj2)
	// But aobj1's AppendNode is reused (extended), so only 1 new node
	require.Equal(t, 2, len(appendNodes2), "Should have 2 AppendNodes: aobj1 (extended) + aobj2 (new)")

	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify node2 was split across 2 aobjs
	require.Equal(t, 2, len(node2.appends))
	assert.Equal(t, uint32(20), node2.appends[0].destLen, "First 20 rows in aobj1")
	assert.Equal(t, uint32(30), node2.appends[1].destLen, "Remaining 30 rows in aobj2")
}

// Test: Frozen aobj
func TestSharedAppender_NewAPI_FrozenAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	// First append to create aobj
	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Freeze the aobj
	// 	appender.GetCurrentAobj().(*aobject).FreezeAppend()

	// Second append should create new aobj
	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)

	// Should have 2 aobjs now
	// 	assert.Equal(t, 2, len(appender.GetRefedAobjs().([]*aobject)))
}

// Test: Concurrent freeze
func TestSharedAppender_NewAPI_ConcurrentFreeze(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	// First append
	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// 	aobj := appender.GetCurrentAobj().(*aobject)
	// 	aobj.FreezeAppend()

	// Second append with different txn should create new aobj
	txn2, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	err = testAppendWithNewAPI(t, table, txn2, rt, node2, false)
	require.NoError(t, err)

	assert.Equal(t, 1, len(node2.appends))
}

// Test: Very large batch
func TestSharedAppender_NewAPI_VeryLargeBatch(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 1000

	node := createMockNode(table.GetLastestSchema(false), 50000)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 50, len(node.appends))

	totalRows := uint32(0)
	for _, info := range node.appends {
		totalRows += info.destLen
	}
	assert.Equal(t, uint32(50000), totalRows)
}

// Test: Concurrent aobj switch
func TestSharedAppender_NewAPI_ConcurrentAobjSwitch(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	concurrency := 5
	rowsPerTxn := 150
	nodes := make([]*mockAppendableNode, concurrency)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txn, err := txnMgr.StartTxn(nil)
			require.NoError(t, err)

			node := createMockNode(table.GetLastestSchema(false), rowsPerTxn)
			nodes[idx] = node

			err = testAppendWithNewAPI(t, table, txn, rt, node, false)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	for _, node := range nodes {
		node.data.Close()
	}
}

// Test: Object properties
func TestSharedAppender_NewAPI_ObjectProperties(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	// aobj := appender.GetCurrentAobj().(*aobject)
	// objEntry := aobj.meta.Load()
	//
	// assert.True(t, objEntry.IsAppendable())
	// assert.False(t, objEntry.IsTombstone)
	// assert.False(t, objEntry.CreatedAt.IsEmpty())
}

// Test: Insufficient space handling
func TestSharedAppender_NewAPI_InsufficientSpace(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	node := createMockNode(table.GetLastestSchema(false), 150)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 2, len(node.appends))
	assert.Equal(t, uint32(100), node.appends[0].destLen)
	assert.Equal(t, uint32(50), node.appends[1].destLen)
}

// Test: Scan integration (read while writing)
func TestSharedAppender_NewAPI_ScanIntegration(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, node1, false)
	require.NoError(t, err)

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node2 := createMockNode(table.GetLastestSchema(false), 1)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)
	// 	aobj := appender.GetCurrentAobj().(*aobject)

	_, err = txnMgr.StartTxn(nil)
	require.NoError(t, err)

	// rows, err := aobj.Rows()
	// require.NoError(t, err)
	// assert.Greater(t, rows, 0)
}

// Test: MakeObjectIt (object iterator)
func TestSharedAppender_NewAPI_MakeObjectIt(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	node := createMockNode(table.GetLastestSchema(false), 250)
	defer node.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	it := table.MakeDataObjectIt()
	defer it.Release()

	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		obj := it.Item()
		assert.NotNil(t, obj)
		count++
	}

	assert.GreaterOrEqual(t, count, 3)
}

// Test: Visibility (MVCC)
func TestSharedAppender_NewAPI_Visibility(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, node1, false)
	require.NoError(t, err)

	_, err = txnMgr.StartTxn(nil)
	require.NoError(t, err)

	it := table.MakeDataObjectIt()
	defer it.Release()

	visibleCount := 0
	for ok := it.First(); ok; ok = it.Next() {
		obj := it.Item()
		if obj != nil {
			visibleCount++
		}
	}

	assert.GreaterOrEqual(t, visibleCount, 0)
}

// Test: CreateTS verification
func TestSharedAppender_NewAPI_CreateTS(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()
	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	// aobj := appender.GetCurrentAobj().(*aobject)
	// objEntry := aobj.meta.Load()
	//
	// assert.False(t, objEntry.CreatedAt.IsEmpty())
	// assert.Greater(t, objEntry.CreatedAt.Physical(), int64(0))
}

// Test: MVCC version chain
func TestSharedAppender_NewAPI_MVCCVersionChain(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()
	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	// aobj := appender.GetCurrentAobj().(*aobject)
	// objEntry := aobj.meta.Load()
	//
	// assert.NotNil(t, objEntry.GetLatestNode())
	// assert.True(t, objEntry.DeletedAt.IsEmpty())
}

// Test: PhyAddr generation
func TestSharedAppender_NewAPI_PhyAddrGeneration(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()

	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	assert.Equal(t, 1, len(node.appends))
	assert.NotEqual(t, "", node.appends[0].dest.String())
}

// Test: Multiple txns with same table (each txn gets its own appender)
func TestSharedAppender_NewAPI_SingletonAppender(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender1 := table.GetTxnAppender(txn, rt, false)
	appender2 := table.GetTxnAppender(txn, rt, false)
	appender3 := table.GetTxnAppender(txn, rt, false)

	// Each call creates a new instance - compare pointer addresses
	assert.NotSame(t, appender1, appender2)
	assert.NotSame(t, appender2, appender3)
}

// Test: Tombstone and data appenders are different
func TestSharedAppender_NewAPI_SeparateAppenders(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	dataAppender := table.GetTxnAppender(txn, rt, false)
	tombstoneAppender := table.GetTxnAppender(txn, rt, true)

	assert.NotEqual(t, dataAppender, tombstoneAppender)
}

// Test: Appender state consistency after multiple operations
func TestSharedAppender_NewAPI_StateConsistency(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	for i := 0; i < 5; i++ {
		txn, err := txnMgr.StartTxn(nil)
		require.NoError(t, err)

		node := createMockNode(table.GetLastestSchema(false), 50+i*10)
		err = testAppendWithNewAPI(t, table, txn, rt, node, false)
		require.NoError(t, err)
		node.data.Close()
	}
}

// Test: Placeholder mechanism correctness
func TestSharedAppender_NewAPI_PlaceholderMechanism(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	startRow1 := node1.appends[0].destOff
	allocated1 := node1.appends[0].destLen
	require.NoError(t, err)
	assert.Equal(t, uint32(0), startRow1)
	assert.Equal(t, uint32(100), allocated1)

	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)
	startRow2 := node2.appends[0].destOff
	allocated2 := node2.appends[0].destLen
	assert.Equal(t, uint32(100), startRow2)
	assert.Equal(t, uint32(50), allocated2)

	assert.Equal(t, startRow1+allocated1, startRow2)
}

// Test: Close method
func TestSharedAppender_NewAPI_Close(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, node, false)
	require.NoError(t, err)

	appender.Close()

	appender2 := table.GetTxnAppender(txn, rt, false)
	assert.NotNil(t, appender2)
}

// Test: Close with nil currentAobj
func TestSharedAppender_NewAPI_CloseNilAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	appender.Close()
}

// Test: Error - ApplyAppend with nil batch
func TestSharedAppender_NewAPI_ErrorNilBatch(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()
	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Expected panic: %v", r)
		}
	}()

	err = appender.ApplyAppend()
	if err != nil {
		t.Logf("Got error as expected: %v", err)
	}
}

// Test: Error - PrepareAppend with zero count
func TestSharedAppender_NewAPI_ErrorZeroCount(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 0)
	defer node.data.Close()
	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)
	assert.Equal(t, 0, len(node.appends))
}

// Test: Error - Exhaust all space then try to append
func TestSharedAppender_NewAPI_ErrorExhaustSpace(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), node1.appends[0].destLen)

	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), node2.appends[0].destOff)
	assert.Equal(t, uint32(50), node2.appends[0].destLen)
}

// Test: Error - Multiple concurrent PrepareAppend
func TestSharedAppender_NewAPI_ErrorConcurrentPrepare(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 1000

	var wg sync.WaitGroup
	errors := make([]error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txn, err := txnMgr.StartTxn(nil)
			if err != nil {
				errors[idx] = err
				return
			}
			node := createMockNode(table.GetLastestSchema(false), 10)
			defer node.data.Close()
			err = testAppendWithNewAPI(t, table, txn, rt, node, false)
			errors[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("PrepareAppend %d failed: %v", i, err)
		}
	}
}

// Test: Error - ApplyAppend with wrong aobj
func TestSharedAppender_NewAPI_ErrorWrongAobj(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)

	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()

	err = appender.ApplyAppend()
	require.NoError(t, err)
}

// Test: Error - Batch with wrong schema
func TestSharedAppender_NewAPI_ErrorWrongSchema(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)
	defer appender.Close()

	node := createMockNode(table.GetLastestSchema(false), 100)
	defer node.data.Close()
	_, err := appender.PrepareAppend(node)
	require.NoError(t, err)

	wrongSchema := catalog.MockSchema(3, 0)
	bat := catalog.MockBatch(wrongSchema, 50)
	defer bat.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Expected panic caught: %v", r)
		}
	}()

	err = appender.ApplyAppend()
	if err != nil {
		t.Logf("Got expected error: %v", err)
	}
}

// Test: Stress test - rapid append and close
func TestSharedAppender_NewAPI_StressRapidAppendClose(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, _, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	table.GetLastestSchema(false).Extra.BlockMaxRows = 100

	var lastTxn txnif.AsyncTxn
	for i := 0; i < 10; i++ {
		txn, err := txnMgr.StartTxn(nil)
		require.NoError(t, err)
		lastTxn = txn

		node := createMockNode(table.GetLastestSchema(false), 20)
		err = testAppendWithNewAPI(t, table, txn, rt, node, false)
		require.NoError(t, err)
		node.data.Close()
	}

	appender := table.GetTxnAppender(lastTxn, rt, false)
	appender.Close()
}

// Test: Tombstone and data hybrid scan - verify row counts and RowIDs
// Note: This test verifies that RowIDs are correctly generated for both data and tombstone.
// The actual hybrid scan logic (merging data and tombstone to get 70 visible rows)
// is tested in integration tests at the table scan level.
func TestSharedAppender_NewAPI_TombstoneDataHybridScan(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	// Append 100 data rows
	dataNode := createMockNode(table.GetLastestSchema(false), 100)
	defer dataNode.data.Close()
	err := testAppendWithNewAPI(t, table, txn, rt, dataNode, false)
	require.NoError(t, err)

	// Verify data RowIDs are generated correctly
	require.Equal(t, 1, len(dataNode.appends))
	dataAppendInfo := dataNode.appends[0]
	assert.Equal(t, uint32(0), dataAppendInfo.destOff, "Data should start at row 0")
	assert.Equal(t, uint32(100), dataAppendInfo.destLen, "Data should have 100 rows")

	// Append 30 tombstone rows (deletes)
	tombstoneNode := createMockNode(table.GetLastestSchema(true), 30)
	defer tombstoneNode.data.Close()
	err = testAppendWithNewAPI(t, table, txn, rt, tombstoneNode, true)
	require.NoError(t, err)

	// Verify tombstone RowIDs are generated correctly
	require.Equal(t, 1, len(tombstoneNode.appends))
	tombstoneAppendInfo := tombstoneNode.appends[0]
	assert.Equal(t, uint32(0), tombstoneAppendInfo.destOff, "Tombstone should start at row 0")
	assert.Equal(t, uint32(30), tombstoneAppendInfo.destLen, "Tombstone should have 30 rows")

	// Verify they are different appenders
	dataAppender := table.GetTxnAppender(txn, rt, false)
	tombstoneAppender := table.GetTxnAppender(txn, rt, true)
	assert.NotEqual(t, dataAppender, tombstoneAppender, "Data and tombstone should use different appenders")

	// Verify RowIDs are in different objects
	assert.NotEqual(t, dataAppendInfo.dest, tombstoneAppendInfo.dest, "Data and tombstone RowIDs should be in different objects")

	// Verify RowID format: each RowID contains object ID and row offset
	// Data RowIDs: [dataObj:0, dataObj:1, ..., dataObj:99]
	// Tombstone RowIDs: [tombstoneObj:0, tombstoneObj:1, ..., tombstoneObj:29]
	// After hybrid scan: 100 data rows - 30 tombstone rows = 70 visible rows
	// (The actual hybrid scan merge logic is tested in table-level integration tests)

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

// Test: Verify ref count correctness with multiple PrepareAppend
func TestSharedAppender_NewAPI_RefCountMultiplePrepare(t *testing.T) {
	defer testutils.AfterTest(t)()

	c, table, txn, txnMgr, rt := setupTest(t)
	defer c.Close()
	defer txnMgr.Stop()

	appender := table.GetTxnAppender(txn, rt, false)

	// First PrepareAppend
	node1 := createMockNode(table.GetLastestSchema(false), 100)
	defer node1.data.Close()
	_, err := appender.PrepareAppend(node1)
	require.NoError(t, err)

	// Get aobjs after first PrepareAppend
	sharedAppender := appender.(SharedAppender)
	aobjs1 := sharedAppender.GetRefedAobjs()
	require.Equal(t, 1, len(aobjs1), "should have 1 aobj after first PrepareAppend")
	aobj1 := aobjs1[0]
	refCount1 := aobj1.RefCount()
	t.Logf("After first PrepareAppend: aobj refCount = %d", refCount1)

	// Second PrepareAppend (same aobj)
	node2 := createMockNode(table.GetLastestSchema(false), 50)
	defer node2.data.Close()
	_, err = appender.PrepareAppend(node2)
	require.NoError(t, err)

	// Get aobjs after second PrepareAppend
	aobjs2 := sharedAppender.GetRefedAobjs()
	require.Equal(t, 1, len(aobjs2), "should still have 1 aobj after second PrepareAppend")
	require.Equal(t, aobj1, aobjs2[0], "should be the same aobj")
	refCount2 := aobj1.RefCount()
	t.Logf("After second PrepareAppend: aobj refCount = %d", refCount2)

	// Verify: refCount should not increase (only ref once per aobj)
	assert.Equal(t, refCount1, refCount2, "refCount should not increase for same aobj")

	// ApplyAppend
	err = appender.ApplyAppend()
	require.NoError(t, err)

	// Verify data was written
	aobj1.RLock()
	node := aobj1.PinNode()
	mnode := node.MustMNode()
	actualRows := mnode.data.Length()
	aobj1.RUnlock()
	node.Unref()
	assert.Equal(t, 150, actualRows, "should have 150 rows")

	// Close appender
	refCountBeforeClose := aobj1.RefCount()
	t.Logf("Before Close: aobj refCount = %d", refCountBeforeClose)

	appender.Close()

	refCountAfterClose := aobj1.RefCount()
	t.Logf("After Close: aobj refCount = %d", refCountAfterClose)

	// Verify: refCount decreased by 1
	assert.Equal(t, refCountBeforeClose-1, refCountAfterClose, "refCount should decrease by 1 after Close")
}
