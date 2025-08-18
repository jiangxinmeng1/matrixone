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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCDC1(t *testing.T) {

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

	moIndexDDL := frontend.MoCatalogMoIndexesDDL	
	moCDCTaskDDL := frontend.MoCatalogMoCdcTaskDDL
	moCDCWatermarkDDL := frontend.MoCatalogMoCdcWatermarkDDL
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement()

	_, err := exec.Exec(ctx, moIndexDDL, opts)
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, moCDCTaskDDL, opts)
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, moCDCWatermarkDDL, opts)
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, frontend.MoCatalogMoRolePrivsDDL, opts)
	assert.NoError(t, err)

	schema := catalog2.MockSchemaAll(20, 1)
	schema.Name = tableName
	schema.Relkind = catalog.SystemOrdinaryRel

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	txn, err := taeHandler.GetDB().StartTxn(nil)
	assert.NoError(t, err)
	db, err := testutil2.CreateDatabase2(ctx, txn, databaseName)
	assert.NoError(t, err)
	_, err = testutil2.CreateRelation2(ctx, txn, db, schema)
	require.NoError(t, err)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	tIEFactory1 := func() internalExecutor.InternalExecutor {
		return frontend.NewInternalExecutor("")
	}

	fault.Enable()
	defer fault.Disable()

	pt := cdc.PatternTuple{
		Source: cdc.PatternTable{
			Database: databaseName,
			Table:    tableName,
		},
		Sink: cdc.PatternTable{
			Database: databaseName,
			Table:    tableName,
		},
	}
	tablePT := cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{&pt},
	}
	tableJsone, err := cdc.JsonEncode(tablePT)
	assert.NoError(t, err)

	rmFn, err := objectio.InjectCDCStart(string(tableJsone))
	assert.NoError(t, err)
	defer rmFn()

	cdcExecutor := frontend.NewCDCTaskExecutor(
		logutil.GetGlobalLogger(),
		tIEFactory1(),
		&task.CreateCdcDetails{
			TaskId:   "cdc 1",
			TaskName: "cdc 1",
			Accounts: []*task.Account{
				{
					Id: uint64(accountId),
				},
			},
		},
		"",
		taeHandler.GetDB().Opts.Fs,
		disttaeEngine.GetTxnClient(),
		disttaeEngine.Engine,
		common.DebugAllocator,
	)
	cdcExecutor.SetAR()
	go cdcExecutor.Start(ctx)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	db, err = rel.GetDB()
	assert.NoError(t, err)
	err = testutil2.DropRelation2(ctx, txn, db, tableName)
	assert.NoError(t, err)
	_, err = testutil2.CreateRelation2(ctx, txn, db, schema)
	assert.NoError(t, err)
	require.Nil(t, txn.Commit(ctx))

	time.Sleep(time.Second * 20)
}
