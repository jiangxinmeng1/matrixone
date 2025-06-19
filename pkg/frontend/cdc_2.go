// Copyright 2022 Matrix Origin
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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

type TableInfo_2 struct {
	dbID      uint64
	tableID   uint64
	tableDef  *plan.TableDef
	watermark types.TS
	err       error
}

type CDCTaskExecutor2 struct {
	accountID uint64
	tables    map[uint64]TableInfo_2 // tableID -> table info

	watermark timestamp.Timestamp

	packer *types.Packer
	mp     *mpool.MPool
	spec   *task.CreateCdcDetails

	logger             *zap.Logger
	sqlExecutorFactory func() ie.InternalExecutor
	attachToTask       func(context.Context, uint64, taskservice.ActiveRoutine) error
	cnUUID             string
	ts                 taskservice.TaskService
	fs                 fileservice.FileService
	txnClient          client.TxnClient
	txnEngine          engine.Engine
}

func (exec *CDCTaskExecutor2) Start() {}

func (exec *CDCTaskExecutor2) registerNewTables(
	ctx context.Context,
	opts CDCCreateTaskOptions,
) (err error) {
	// get schema from catalog and update table info

	var tablesPatternTuples cdc.PatternTuples
	cdc.JsonDecode(opts.PitrTables, &tablesPatternTuples)
	for _, table := range tablesPatternTuples.Pts {
		if err = exec.registerTableWithPattern(ctx, table, exec.txnClient, exec.txnEngine); err != nil {
			return err
		}
	}
	// send first task
	return
}

func (exec *CDCTaskExecutor2) registerTableWithPattern(
	ctx context.Context,
	tablePattern *cdc.PatternTuple,
	txnClient client.TxnClient,
	txnEngine engine.Engine,
) error {
	if tablePattern.Source.Database == cdc.CDCPitrGranularity_All ||
		tablePattern.Source.Table == cdc.CDCPitrGranularity_All {
		panic("not support")
		//for each table in mo tables
	}
	return exec.registerTableWithTableName(
		ctx,
		tablePattern.Source.Database,
		tablePattern.Source.Table,
	)

}

func (exec *CDCTaskExecutor2) registerTableWithTableName(
	ctx context.Context,
	dbName, tableName string,
) (err error) {

	txnOp, err := cdc.GetTxnOp(ctx, exec.txnEngine, exec.txnClient, "cdc-handleNewTables")
	if err != nil {
		logutil.Errorf("cdc task %s get txn op failed, err: %v", exec.spec.TaskName, err)
		return
	}
	var db engine.Database
	if db, err = exec.txnEngine.Database(ctx, dbName, txnOp); err != nil {
		return err
	}

	var table engine.Relation
	if table, err = db.Relation(ctx, tableName, nil); err != nil {
		return err
	}

	def := table.CopyTableDef(ctx)

	tableInfo := TableInfo_2{
		dbID:      def.DbId,
		tableID:   def.TblId,
		tableDef:  def,
		watermark: types.TS{},
		err:       nil,
	}
	exec.tables[tableInfo.tableID] = tableInfo

	return nil
}

func (exec *CDCTaskExecutor2) getDirtyTables(
	ctx context.Context,
	service string,
	eng engine.Engine,
) (tables []TableInfo_2, err error) {

	accs := make([]uint64, 0, len(exec.tables))
	dbs := make([]uint64, 0, len(exec.tables))
	tbls := make([]uint64, 0, len(exec.tables))
	ts := make([]timestamp.Timestamp, 0, len(exec.tables))

	tables = make([]TableInfo_2, 0, len(exec.tables))
	for _, t := range exec.tables {
		accs = append(accs, uint64(exec.accountID))
		dbs = append(dbs, t.dbID)
		tbls = append(tbls, t.tableID)
		ts = append(ts, exec.watermark)
	}
	disttae.GetChangedTableList(
		ctx,
		service,
		eng,
		accs,
		dbs,
		tbls,
		ts,
		nil,
		cmd_util.CheckChanged,
		func(
			accountID int64,
			databaseID int64,
			tableID int64,
			tableName string,
			dbName string,
			relKind string,
			pkSequence int,
			snapshot types.TS,
		) {
			tables = append(tables, TableInfo_2{
				dbID:      uint64(databaseID),
				tableID:   uint64(tableID),
				watermark: snapshot,
			})
		},
	)
	return
}

func (exec *CDCTaskExecutor2) getIterationTask(
	ctx context.Context,
	table *TableInfo_2,
) {
	//get rel
	var rel engine.Relation
	//get sinker
	var sinker cdc.Sinker

	from := types.TimestampToTS(exec.watermark)

	//task:
	cdc.CollectChanges_2(
		ctx,
		rel,
		from,
		table.watermark,
		sinker,
		true,
		exec.packer,
		exec.mp,
	)
}
