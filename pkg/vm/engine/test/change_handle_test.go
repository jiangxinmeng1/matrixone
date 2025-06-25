// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

func TestChangesHandle1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	obj := testutil2.GetOneBlockMeta(rel)
	err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle2(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	obj := testutil2.GetOneBlockMeta(rel)
	err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func checkTombstoneBatch(bat *batch.Batch, pkType types.Type, t *testing.T) {
	if bat == nil {
		return
	}
	assert.Equal(t, len(bat.Vecs), 2)
	assert.Equal(t, bat.Vecs[0].GetType().Oid, pkType.Oid)
	assert.Equal(t, bat.Vecs[1].GetType().Oid, types.T_TS)
	assert.Equal(t, bat.Vecs[0].Length(), bat.Vecs[1].Length())
}

func checkInsertBatch(userBatch *containers.Batch, bat *batch.Batch, t *testing.T) {
	if bat == nil {
		return
	}
	length := bat.RowCount()
	assert.Equal(t, len(bat.Vecs), len(userBatch.Vecs)+1) // user rows + committs
	for i, vec := range userBatch.Vecs {
		assert.Equal(t, bat.Vecs[i].GetType().Oid, vec.GetType().Oid)
		assert.Equal(t, bat.Vecs[i].Length(), length)
	}
	assert.Equal(t, bat.Vecs[len(userBatch.Vecs)].GetType().Oid, types.T_TS)
	assert.Equal(t, bat.Vecs[len(userBatch.Vecs)].Length(), length)
}

func TestChangesHandle3(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(23, 9)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 163840)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	iter := rel.MakeObjectIt(false)
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
	}
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, 163820)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows = 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				assert.Equal(t, tombstone.Vecs[0].Length(), 20)
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, totalRows, 163840)
		assert.NoError(t, handle.Close())
	}
}
func TestChangesHandleForCNWrite(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(blockCnt)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 0; idx < blockCnt; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeEngine.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, bat.Length())
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		batchCount := 0
		for {
			data, tombstone, _, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			batchCount++
			assert.NoError(t, err)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 16384)
			data.Clean(mp)
		}
		assert.Equal(t, batchCount, 5)
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle4(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	batchCount := blockCnt * 2
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(batchCount)

	dntxn, dnrel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[0]))
	assert.NoError(t, dntxn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, true)

	dntxn, dnrel = testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[1]))
	assert.NoError(t, dntxn.Commit(ctx))
	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 3; idx < batchCount; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	dntxn, dnrel = testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[2]))
	assert.NoError(t, dntxn.Commit(ctx))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		c := &checkHelper{}
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, _, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			c.check(data, tombstone, t)
			if tombstone != nil {
				tombstone.Clean(mp)
			}
			if data != nil {
				data.Clean(mp)
			}
		}
		c.checkRows(rowsCount, 0, t)
		assert.NoError(t, handle.Close())
	}

}

type checkHelper struct {
	prevDataTS, prevTombstoneTS       types.TS
	totalDataRows, totalTombstoneRows int
}

func (c *checkHelper) check(data, tombstone *batch.Batch, t *testing.T) {
	if data != nil {
		maxTS := types.TS{}
		commitTSVec := data.Vecs[len(data.Vecs)-1]
		commitTSs := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec)
		for _, ts := range commitTSs {
			assert.True(t, ts.GE(&c.prevTombstoneTS))
			if ts.GT(&maxTS) {
				maxTS = ts
			}
		}
		c.prevDataTS = maxTS
		c.totalDataRows += commitTSVec.Length()
	}
	if tombstone != nil {
		maxTS := types.TS{}
		commitTSVec := data.Vecs[len(tombstone.Vecs)-1]
		commitTSs := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec)
		for _, ts := range commitTSs {
			assert.True(t, ts.GT(&c.prevDataTS))
			if ts.GT(&maxTS) {
				maxTS = ts
			}
		}
		c.prevTombstoneTS = maxTS
		c.totalTombstoneRows += commitTSVec.Length()
	}
}

func (c *checkHelper) checkRows(data, tombstone int, t *testing.T) {
	assert.Equal(t, data, c.totalDataRows)
	assert.Equal(t, tombstone, c.totalTombstoneRows)
}

func TestChangesHandle5(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	flushTxn, flushRel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	obj := testutil2.GetOneBlockMeta(flushRel)
	{
		txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		obj := testutil2.GetOneBlockMeta(rel)
		err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}
	task, err := jobs.NewFlushTableTailTask(nil, flushTxn, []*catalog2.ObjectEntry{obj}, nil, taeHandler.GetDB().Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, flushTxn.Commit(context.Background()))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle6(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	flushTxn, flushRel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	obj := testutil2.GetOneBlockMeta(flushRel)
	{
		txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		obj := testutil2.GetOneBlockMeta(rel)
		err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}
	task, err := jobs.NewFlushTableTailTask(nil, flushTxn, []*catalog2.ObjectEntry{obj}, nil, taeHandler.GetDB().Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, flushTxn.Commit(context.Background()))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandleStaleFiles1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, schema.Name)
	obj := testutil2.GetOneBlockMeta(rel)
	txn.Commit(ctx)
	assert.NoError(t, err)

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	fs := taeHandler.GetDB().Runtime.Fs
	deleteFileName := obj.ObjectStats.ObjectName().String()
	err = fs.Delete(ctx, []string{string(deleteFileName)}...)
	assert.NoError(t, err)
	gcTS := taeHandler.GetDB().TxnMgr.Now()
	gcTSFileName := ioutil.EncodeCompactCKPMetadataFullName(
		types.TS{}, gcTS,
	)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs)
	assert.NoError(t, err)
	_, err = writer.Write(containers.ToCNBatch(bat))
	assert.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	assert.NoError(t, err)

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		_, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead))

	}
}
func TestChangesHandleStaleFiles2(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(blockCnt)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 0; idx < blockCnt; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{

		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		{
			txn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
			iter := dnRel.MakeObjectItOnSnap(false)
			objs := make([]*catalog2.ObjectEntry, 0)
			for iter.Next() {
				obj := iter.GetObject().GetMeta().(*catalog2.ObjectEntry)
				if obj.ObjectStats.GetCNCreated() {
					objs = append(objs, obj)
				}
			}
			assert.NoError(t, txn.Commit(ctx))
			fs := taeEngine.GetDB().Runtime.Fs
			for _, obj := range objs {
				deleteFileName := obj.ObjectStats.ObjectName().String()
				err = fs.Delete(ctx, deleteFileName)
				assert.NoError(t, err)
			}
			gcTS := taeEngine.GetDB().TxnMgr.Now()
			gcTSFileName := ioutil.EncodeCompactCKPMetadataFullName(
				types.TS{}, gcTS,
			)
			writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs)
			assert.NoError(t, err)
			_, err = writer.Write(containers.ToCNBatch(bat))
			assert.NoError(t, err)
			_, err = writer.WriteEnd(ctx)
			assert.NoError(t, err)
		}
		data, tombstone, _, err := handle.Next(ctx, mp)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead))
		assert.Nil(t, tombstone)
		assert.Nil(t, data)
	}
}

func TestChangesHandleStaleFiles5(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(23, 9)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 163840)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	iter := rel.MakeObjectIt(false)
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
	}
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		handle.(*logtailreplay.ChangeHandler).LogThreshold = time.Microsecond
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				assert.Equal(t, tombstone.Vecs[0].Length(), 20)
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, totalRows, 163840)
		assert.NoError(t, handle.Close())
	}
}

func TestChangeHandleFilterBatch1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 1)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	assert.NoError(t, txn.Commit(ctx))

	appendFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctx, bat))
		require.Nil(t, txn.Commit(ctx))
	}

	deleteFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		pkVal := bat.Vecs[0].Get(0)
		filter := handle.NewEQFilter(pkVal)
		err = rel.DeleteByFilter(ctx, filter)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}

	appendFn()
	deleteFn()
	ts1 := taeHandler.GetDB().TxnMgr.Now()

	appendFn()
	ts2 := taeHandler.GetDB().TxnMgr.Now()
	deleteFn()
	appendFn()
	ts3 := taeHandler.GetDB().TxnMgr.Now()
	deleteFn()
	ts4 := taeHandler.GetDB().TxnMgr.Now()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, ts1, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Nil(t, data)
			assert.Nil(t, tombstone)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, ts3, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
			assert.NotNil(t, data)
			assert.Equal(t, data.Vecs[0].Length(), 1)
			assert.Nil(t, tombstone)
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, ts2, ts3, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, data)
			assert.Nil(t, tombstone)
			assert.Equal(t, data.Vecs[0].Length(), 1)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, ts2, ts4, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, tombstone)
			assert.Nil(t, data)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangeHandleFilterBatch2(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, -1)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 1)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	assert.NoError(t, txn.Commit(ctx))

	appendFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctx, bat))
		require.Nil(t, txn.Commit(ctx))
	}

	deleteFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		schema := rel.GetMeta().(*catalog2.TableEntry).GetLastestSchemaLocked(false)
		pkIdx := schema.GetPrimaryKey().Idx
		rowIDIdx := schema.GetColIdx(catalog2.PhyAddrColumnName)
		it := rel.MakeObjectIt(false)
		for it.Next() {
			blk := it.GetObject()
			defer blk.Close()
			blkCnt := uint16(blk.BlkCnt())
			for i := uint16(0); i < blkCnt; i++ {
				var view *containers.Batch
				err := blk.HybridScan(context.Background(), &view, i, []int{rowIDIdx, pkIdx}, common.DefaultAllocator)
				assert.NoError(t, err)
				defer view.Close()
				view.Compact()
				err = rel.DeleteByPhyAddrKeys(view.Vecs[0], view.Vecs[1], handle.DT_Normal)
				assert.NoError(t, err)
			}
		}
		err := txn.Commit(context.Background())
		assert.NoError(t, err)
	}

	appendFn()
	deleteFn()
	appendFn()
	deleteFn()
	appendFn()
	deleteFn()
	end := taeHandler.GetDB().TxnMgr.Now()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, end, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
			assert.NotNil(t, tombstone)
			assert.Equal(t, tombstone.Vecs[0].Length(), 3)
			assert.NotNil(t, data)
			assert.Equal(t, data.Vecs[0].Length(), 3)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle7(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, -1)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 8192)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRowCount := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			totalRowCount += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRowCount, 8192*2)
		assert.NoError(t, handle.Close())
	}
}

type idAllocator interface {
	Alloc() uint64
}

func newMoAsyncIndexLogBatch(
	mp *mpool.MPool,
	withRowID bool,
) *batch.Batch {
	bat := batch.New(
		[]string{
			"id",
			"account_id",
			"table_id",
			"index_id",
			"last_sync_txn_ts",
			"err_code",
			"error_msg",
			"info",
			"drop_at",
		},
	)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())   //id
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())  //account_id
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())   //table_id
	bat.Vecs[3] = vector.NewVec(types.T_int32.ToType())   //index_id
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
	bat.Vecs[5] = vector.NewVec(types.T_int32.ToType())   //err_code
	bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
	bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
	bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
	if withRowID {
		bat.Attrs = append(bat.Attrs, objectio.PhysicalAddr_Attr)
		bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_Rowid.ToType())) //row_id
	}
	return bat
}

func getInsertWatermarkFn(
	t *testing.T,
	idAllocator idAllocator,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID int32,
	indexID int32,
) error {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID int32,
		indexID int32,
	) error {
		_, rel, txn, err := de.GetTable(ctx, "mo_catalog", "mo_async_index_log")
		assert.NoError(t, err)
		bat := batch.New(
			[]string{
				"id",
				"account_id",
				"table_id",
				"index_id",
				"last_sync_txn_ts",
				"err_code",
				"error_msg",
				"info",
				"drop_at",
			},
		)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType()) //id
		vector.AppendFixed(bat.Vecs[0], int32(idAllocator.Alloc()), false, mp)
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType()) //account_id
		vector.AppendFixed(bat.Vecs[1], accountID, false, mp)
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType()) //table_id
		vector.AppendFixed(bat.Vecs[2], tableID, false, mp)
		bat.Vecs[3] = vector.NewVec(types.T_int32.ToType()) //index_id
		vector.AppendFixed(bat.Vecs[3], indexID, false, mp)
		bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
		vector.AppendBytes(bat.Vecs[4], []byte(""), false, mp)
		bat.Vecs[5] = vector.NewVec(types.T_int32.ToType()) //err_code
		vector.AppendFixed(bat.Vecs[5], int32(0), false, mp)
		bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
		vector.AppendFixed(bat.Vecs[6], []byte(""), false, mp)
		bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
		vector.AppendBytes(bat.Vecs[7], []byte(""), false, mp)
		bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
		vector.AppendBytes(bat.Vecs[8], []byte(""), false, mp)
		bat.Vecs[9] = vector.NewVec(types.T_bool.ToType()) //pause
		vector.AppendFixed(bat.Vecs[9], false, false, mp)
		bat.SetRowCount(1)

		assert.NoError(t, err)
		err = rel.Write(ctx, bat)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))
		return nil
	}

}

func getFlushWatermarkFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	watermark types.TS,
	pause bool,
	accountID int32,
	indexID int32,
	errorCode int,
	info string,
	errorMsg string,
) error {
	return func(
		ctx context.Context,
		tableID uint64,
		watermark types.TS,
		pause bool,
		accountID int32,
		indexID int32,
		errorCode int,
		info string,
		errorMsg string,
	) error {
		txn, rel, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		deleteRowIDs := make([]types.Rowid, 0)
		deletePks := make([]int32, 0)
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[9])
			pks := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == accountID && indexIDs[i] == indexID {
					deleteRowIDs = append(deleteRowIDs, rowIDs[i])
					deletePks = append(deletePks, pks[i])
				}
			}
		}
		if len(deleteRowIDs) != 1 {
			panic(fmt.Sprintf("logic err: rowCount:%d,tableID:%d,accountID:%d,indexID:%d, ts %v", len(deleteRowIDs), tableID, accountID, indexID, txn.SnapshotTS()))
		}

		deleteBatch := batch.New([]string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr})
		deleteBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		deleteBatch.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		vector.AppendFixed(deleteBatch.Vecs[0], deleteRowIDs[0], false, mp)
		vector.AppendFixed(deleteBatch.Vecs[1], deletePks[0], false, mp)
		deleteBatch.SetRowCount(1)

		_, rel, txn, err = de.GetTable(ctx, "mo_catalog", "mo_async_index_log")
		assert.NoError(t, err)
		assert.NoError(t, rel.Delete(ctx, deleteBatch, catalog.Row_ID))

		bat = batch.New(
			[]string{
				"id",
				"account_id",
				"table_id",
				"index_id",
				"last_sync_txn_ts",
				"err_code",
				"error_msg",
				"info",
				"drop_at",
			},
		)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType()) //id
		vector.AppendFixed(bat.Vecs[0], deletePks[0], false, mp)
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType()) //account_id
		vector.AppendFixed(bat.Vecs[1], accountID, false, mp)
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType()) //table_id
		vector.AppendFixed(bat.Vecs[2], tableID, false, mp)
		bat.Vecs[3] = vector.NewVec(types.T_int32.ToType()) //index_id
		vector.AppendFixed(bat.Vecs[3], indexID, false, mp)
		bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
		vector.AppendBytes(bat.Vecs[4], []byte(watermark.ToString()), false, mp)
		bat.Vecs[5] = vector.NewVec(types.T_int32.ToType()) //err_code
		vector.AppendFixed(bat.Vecs[5], int32(errorCode), false, mp)
		bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
		vector.AppendFixed(bat.Vecs[6], []byte(errorMsg), false, mp)
		bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
		vector.AppendBytes(bat.Vecs[7], []byte(info), false, mp)
		bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
		vector.AppendBytes(bat.Vecs[8], []byte(errorMsg), false, mp)
		bat.Vecs[9] = vector.NewVec(types.T_bool.ToType()) //pause
		vector.AppendFixed(bat.Vecs[9], pause, false, mp)
		bat.SetRowCount(1)

		assert.NoError(t, err)
		err = rel.Write(ctx, bat)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))
		return nil
	}
}
func getReplayFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID uint32,
	indexID int32,
) (
	watermark types.TS,
	errorCode int,
	errorMsg string,
	pause bool,
	err error,
) {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID uint32,
		indexID int32,
	) (
		watermark types.TS,
		errorCode int,
		errorMsg string,
		pause bool,
		err error,
	) {
		txn, _, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		rowcount := 0
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			errors := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[5])
			pauses := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[9])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == int32(accountID) && indexIDs[i] == indexID {
					rowcount++
					watermarkStr := bat.Vecs[4].GetStringAt(i)
					watermark = types.StringToTS(watermarkStr)
					errorCode = int(errors[i])
					errorMsg = bat.Vecs[6].GetStringAt(i)
					pause = pauses[i]
				}
			}
		}
		if rowcount != 1 {
			err = moerr.NewInternalError(ctx, fmt.Sprintf("row count not match, row count %d", rowcount))
			return
		}
		assert.NoError(t, txn.Commit(ctx))
		return watermark, errorCode, errorMsg, pause, err
	}
}

func getDeleteFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID uint32,
	indexID int32,
) (
	err error,
) {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID uint32,
		indexID int32,
	) (
		err error,
	) {
		txn, rel, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		deleteRowIDs := make([]types.Rowid, 0)
		deletePks := make([]int32, 0)
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[9])
			pks := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == int32(accountID) && indexIDs[i] == indexID {
					deleteRowIDs = append(deleteRowIDs, rowIDs[i])
					deletePks = append(deletePks, pks[i])
				}
			}
		}
		if len(deleteRowIDs) != 1 {
			panic(fmt.Sprintf("logic err: rowCount:%d,tableID:%d,accountID:%d,indexID:%d, ts %v", len(deleteRowIDs), tableID, accountID, indexID, txn.SnapshotTS()))
		}

		deleteBatch := batch.New([]string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr})
		deleteBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		deleteBatch.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		vector.AppendFixed(deleteBatch.Vecs[0], deleteRowIDs[0], false, mp)
		vector.AppendFixed(deleteBatch.Vecs[1], deletePks[0], false, mp)
		deleteBatch.SetRowCount(1)

		assert.NoError(t, rel.Delete(ctx, deleteBatch, catalog.Row_ID))
		assert.NoError(t, txn.Commit(ctx))
		return err
	}
}
func getCDCExecutor(
	ctx context.Context,
	t *testing.T,
	idAllocator idAllocator,
	accountID uint32,
	cnTestEngine *testutil.TestDisttaeEngine,
	taeHandler *testutil.TestTxnStorage,
) *frontend.CDCTaskExecutor2 {

	mockSpec := &task.CreateCdcDetails{
		TaskName: "cdc_task",
	}

	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(cnTestEngine.Engine.GetService())
	}

	txnFactory := func() (client.TxnOperator, error) {
		return cnTestEngine.NewTxnOperator(ctx, cnTestEngine.Now())
	}
	sinkerFactory := func(dbName, tableName string, tableDefs []engine.TableDef) (cdc.Sinker, error) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
		return frontend.MockCNSinker(
			ctx,
			func(ctx context.Context) (engine.Relation, client.TxnOperator, error) {
				_, rel, txn, err := cnTestEngine.GetTable(ctx, dbName, tableName)
				return rel, txn, err
			},
			func(ctx context.Context, dstDefs []engine.TableDef) error {
				txn, err := cnTestEngine.NewTxnOperator(ctx, cnTestEngine.Now())
				if err != nil {
					return err
				}
				db, err := cnTestEngine.Engine.Database(ctx, dbName, txn)
				if err != nil {
					return err
				}

				if _, err = db.Relation(ctx, tableName, nil); err == nil {
					return nil
				}

				err = db.Create(ctx, tableName, dstDefs)
				if err != nil {
					return err
				}
				return txn.Commit(ctx)
			},
			tableDefs,
			common.DebugAllocator,
		)
	}
	cdcExecutor := frontend.NewCDCTaskExecutor2(
		ctx,
		uint64(accountID),
		mockSpec,
		ieFactory,
		sinkerFactory,
		txnFactory,
		cnTestEngine.Engine,
		cnTestEngine.Engine.GetService(),
		taeHandler.GetRPCHandle().HandleGetChangedTableList,
		getInsertWatermarkFn(t, idAllocator, cnTestEngine, common.DebugAllocator),
		getFlushWatermarkFn(t, cnTestEngine, common.DebugAllocator),
		getReplayFn(t, cnTestEngine, common.DebugAllocator),
		getDeleteFn(t, cnTestEngine, common.DebugAllocator),
		common.DebugAllocator,
	)
	return cdcExecutor
}

/*
CREATE TABLE mo_async_index_log (

	id INT AUTO_INCREMENT PRIMARY KEY,
	account_id INT NOT NULL,
	table_id INT NOT NULL,
	index_id INT NOT NULL,
	last_sync_txn_ts VARCHAR(32)  NOT NULL,
	err_code INT NOT NULL,
	error_msg VARCHAR(255) NOT NULL,
	info VARCHAR(255) NOT NULL,
	drop_at VARCHAR(32) NULL,
	pause bool NULL,
);
*/
func mock_mo_async_index_log(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	var defs = make([]engine.TableDef, 0)

	addDefFn := func(name string, typ types.Type, idx int) {
		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Type:          typ,
				IsRowId:       false,
				Name:          name,
				ID:            uint64(idx),
				Primary:       name == "id",
				IsHidden:      false,
				Seqnum:        uint16(idx),
				ClusterBy:     false,
				AutoIncrement: name == "id",
			},
		})
	}

	addDefFn("id", types.T_int32.ToType(), 0)
	addDefFn("account_id", types.T_int32.ToType(), 1)
	addDefFn("table_id", types.T_int32.ToType(), 2)
	addDefFn("index_id", types.T_int32.ToType(), 3)
	addDefFn("last_sync_txn_ts", types.T_varchar.ToType(), 4)
	addDefFn("err_code", types.T_int32.ToType(), 5)
	addDefFn("error_msg", types.T_varchar.ToType(), 6)
	addDefFn("info", types.T_varchar.ToType(), 7)
	addDefFn("drop_at", types.T_varchar.ToType(), 8)
	addDefFn("pause", types.T_bool.ToType(), 9)

	defs = append(defs,
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
	)
	dbName := "mo_catalog"
	tableName := "mo_async_index_log"
	var txn client.TxnOperator
	if txn, err = de.NewTxnOperator(ctx, de.Now()); err != nil {
		return
	}

	var database engine.Database
	if database, err = de.Engine.Database(ctx, dbName, txn); err != nil {
		return
	}
	if err = database.Create(ctx, tableName, defs); err != nil {
		return
	}

	if _, err = database.Relation(ctx, tableName, nil); err != nil {
		return
	}

	if err = txn.Commit(ctx); err != nil {
		return
	}
	return
}

func getCDCPitrTablesString(
	srcDB, srcTable string,
	dstDB, dstTable string,
) string {
	table := cdc.PatternTuple{
		Source: cdc.PatternTable{
			Database: srcDB,
			Table:    srcTable,
		},
		Sink: cdc.PatternTable{
			Database: dstDB,
			Table:    dstTable,
		},
	}
	var tablesPatternTuples cdc.PatternTuples
	tablesPatternTuples.Append(&table)
	tableStr, err := cdc.JsonEncode(tablesPatternTuples)
	if err != nil {
		panic(err)
	}
	return tableStr
}

func addCDCTask(
	ctx context.Context,
	exec *frontend.CDCTaskExecutor2,
	srcDB, srcTable string,
	dstDB, dstTable string,
) error {
	err := exec.StartTables(
		ctx,
		frontend.CDCCreateTaskOptions{
			PitrTables: getCDCPitrTablesString(
				srcDB,
				srcTable,
				dstDB,
				dstTable,
			),
		},
	)
	return err
}

func pauseCDCTask(
	ctx context.Context,
	exec *frontend.CDCTaskExecutor2,
	srcDB, srcTable string,
	dstDB, dstTable string,
) error {
	err := exec.PauseTables(
		ctx,
		frontend.CDCCreateTaskOptions{
			PitrTables: getCDCPitrTablesString(
				srcDB,
				srcTable,
				dstDB,
				dstTable,
			),
		},
	)
	return err
}

func dropCDCTask(
	ctx context.Context,
	exec *frontend.CDCTaskExecutor2,
	srcDB, srcTable string,
	dstDB, dstTable string,
) error {
	err := exec.DropTables(
		ctx,
		frontend.CDCCreateTaskOptions{
			PitrTables: getCDCPitrTablesString(
				srcDB,
				srcTable,
				dstDB,
				dstTable,
			),
		},
	)
	return err
}

func TestCDCExecutor(t *testing.T) {
	catalog.SetupDefines("")

	idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_async_index_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(10, 1)
	tableCount := 1
	rowCount := 10

	tableIDs := make([]uint64, 0, tableCount)
	dbNames := make([]string, 0, tableCount)
	srcTables := make([]string, 0, tableCount)
	dstTables := make([]string, 0, tableCount)

	for i := 0; i < tableCount; i++ {
		dbNames = append(dbNames, fmt.Sprintf("db%d", i))
		srcTables = append(srcTables, fmt.Sprintf("src_table%d", i))
		dstTables = append(dstTables, fmt.Sprintf("dst_table%d", i))
	}
	bat := catalog2.MockBatch(schema, rowCount)
	bats := bat.Split(rowCount)

	// create database and table

	for i := 0; i < tableCount; i++ {
		disttaeEngine.CreateDatabaseAndTable(ctxWithTimeout, dbNames[i], srcTables[i], schema)

		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, dbNames[i], srcTables[i])
		require.Nil(t, err)
		tableIDs = append(tableIDs, rel.GetTableID(ctxWithTimeout))
		txn.Commit(ctxWithTimeout)
	}

	appendFn := func(db, table string, rowIdx int) {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, db, table)
		require.Nil(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[rowIdx]))
		require.Nil(t, err)

		txn.Commit(ctxWithTimeout)
	}

	for i := 0; i < tableCount; i++ {
		appendFn(dbNames[i], srcTables[i], 0)
	}

	cdcExecutor := getCDCExecutor(ctxWithTimeout, t, idAllocator, accountId, disttaeEngine, taeHandler)

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	for i := 0; i < tableCount; i++ {
		err := addCDCTask(ctxWithTimeout, cdcExecutor, dbNames[i], srcTables[i], dbNames[i], dstTables[i])
		assert.NoError(t, err)
	}

	for j := 0; j < tableCount; j++ {
		appendFn(dbNames[j], srcTables[j], 1)
	}

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(4000, func() bool {
		for i := 0; i < tableCount; i++ {
			watermark := cdcExecutor.GetWatermark(tableIDs[i])
			if !watermark.GE(&now) {
				return false
			}
		}
		return true
	})

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	for i := 0; i < tableCount; i++ {
		tnTxn, err := taeHandler.GetDB().StartTxn(nil)
		assert.NoError(t, err)
		tnDatabase, err := tnTxn.GetDatabase(dbNames[i])
		assert.NoError(t, err)
		tnRel, err := tnDatabase.GetRelationByName(dstTables[i])
		assert.NoError(t, err)
		testutil2.CheckAllColRowsByScan(t, tnRel, 2, false)
		assert.NoError(t, tnTxn.Commit(ctxWithTimeout))
	}

	for i := 0; i < tableCount; i++ {
		err := pauseCDCTask(ctxWithTimeout, cdcExecutor, dbNames[i], srcTables[i], dbNames[i], dstTables[i])
		assert.NoError(t, err)
	}

	for i := 0; i < tableCount; i++ {
		appendFn(dbNames[i], srcTables[i], 2)
	}

	time.Sleep(time.Second)

	for i := 0; i < tableCount; i++ {
		tnTxn, err := taeHandler.GetDB().StartTxn(nil)
		assert.NoError(t, err)
		tnDatabase, err := tnTxn.GetDatabase(dbNames[i])
		assert.NoError(t, err)
		tnRel, err := tnDatabase.GetRelationByName(dstTables[i])
		assert.NoError(t, err)
		testutil2.CheckAllColRowsByScan(t, tnRel, 2, false)
		assert.NoError(t, tnTxn.Commit(ctxWithTimeout))
	}

	for i := 0; i < tableCount; i++ {
		err := addCDCTask(ctxWithTimeout, cdcExecutor, dbNames[i], srcTables[i], dbNames[i], dstTables[i])
		assert.NoError(t, err)
	}
	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(4000, func() bool {
		for i := 0; i < tableCount; i++ {
			watermark := cdcExecutor.GetWatermark(tableIDs[i])
			if !watermark.GE(&now) {
				return false
			}
		}
		return true
	})

	for i := 0; i < tableCount; i++ {
		tnTxn, err := taeHandler.GetDB().StartTxn(nil)
		assert.NoError(t, err)
		tnDatabase, err := tnTxn.GetDatabase(dbNames[i])
		assert.NoError(t, err)
		tnRel, err := tnDatabase.GetRelationByName(dstTables[i])
		assert.NoError(t, err)
		testutil2.CheckAllColRowsByScan(t, tnRel, 3, false)
		assert.NoError(t, tnTxn.Commit(ctxWithTimeout))
	}

	for i := 0; i < tableCount; i++ {
		err := dropCDCTask(ctxWithTimeout, cdcExecutor, dbNames[i], srcTables[i], dbNames[i], dstTables[i])
		assert.NoError(t, err)
	}
}
