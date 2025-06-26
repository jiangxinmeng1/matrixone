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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

type CNSinker struct {
	ctx         context.Context
	initTableFn func(context.Context, []engine.TableDef) error
	relFactory  relationFactory
	currentTxn  client.TxnOperator
	currentRel  engine.Relation
	def         []engine.TableDef

	mp *mpool.MPool
}

// TODO stop sinker, drop table
func MockCNSinker(
	ctx context.Context,
	relFactory relationFactory,
	initTableFn func(context.Context, []engine.TableDef) error,
	def []engine.TableDef,
	mp *mpool.MPool,
) (cdc.Sinker, error) {
	return &CNSinker{
		relFactory:  relFactory,
		initTableFn: initTableFn,
		def:         def,
		ctx:         ctx,
		mp:          mp,
	}, nil
}

func (sinker *CNSinker) Run(ctx context.Context, ar *cdc.ActiveRoutine) {
	err := sinker.initTableFn(ctx, sinker.def)
	if err != nil {
		panic(err)
	}
}
func (sinker *CNSinker) Sink(ctx context.Context, data *cdc.DecoderOutput) {
	var initSnapshotSplitTxn bool
	var txn client.TxnOperator
	var rel engine.Relation
	var err error
	if sinker.currentRel == nil {
		initSnapshotSplitTxn = true
		rel, txn, err = sinker.relFactory(ctx)
		if err != nil {
			panic(err)
		}
	} else {
		txn = sinker.currentTxn
		rel = sinker.currentRel
	}
	insertBat := data.GetInsertAtmBatch()
	deleteBat := data.GetDeleteAtmBatch()
	if insertBat != nil {
		insertBat.Vecs[len(insertBat.Vecs)-1].Free(sinker.mp)
		insertBat.Vecs = insertBat.Vecs[:len(insertBat.Vecs)-1]
		err := rel.Write(ctx, insertBat)
		if err != nil {
			panic(err)
		}
	}
	if deleteBat != nil {
		deleteBat.Vecs[len(deleteBat.Vecs)-1].Free(sinker.mp)
		deleteBat.Vecs = deleteBat.Vecs[:len(deleteBat.Vecs)-1]
		err := rel.Delete(ctx, deleteBat, catalog.Row_ID)
		if err != nil {
			panic(err)
		}
	}
	if initSnapshotSplitTxn {
		txn.Commit(ctx)
	}
}
func (sinker *CNSinker) SendBegin() {
	var err error
	sinker.currentRel, sinker.currentTxn, err = sinker.relFactory(sinker.ctx)
	if err != nil {
		panic(err)
	}
}
func (sinker *CNSinker) SendCommit() {
	sinker.currentTxn.Commit(sinker.ctx)
	sinker.currentRel = nil
	sinker.currentTxn = nil
}
func (sinker *CNSinker) SendRollback() {
	sinker.currentTxn.Rollback(sinker.ctx)
	sinker.currentRel = nil
	sinker.currentTxn = nil
}

// SendDummy to guarantee the last sql is sent
func (sinker *CNSinker) SendDummy() {}

// Error must be called after Sink
func (sinker *CNSinker) Error() error {
	return nil
}
func (sinker *CNSinker) ClearError() {}
func (sinker *CNSinker) Reset()      {}
func (sinker *CNSinker) Close()      {}

type TableState int8

const (
	TableState_Invalid TableState = iota
	TableState_Running
	TableState_Finished
)

type relationFactory func(context.Context) (engine.Relation, client.TxnOperator, error)

type replayFn func(
	ctx context.Context, tableID uint64, accountID uint32, indexID int32,
) (watermark types.TS, errorCode int, errorMsg string, err error)

type deleteFn func(ctx context.Context, tableID uint64, accountID uint32, indexID int32) error

type Worker interface {
	Submit(ctx context.Context, task func() error) error
	Stop()
}

type worker struct {
	queue sm.Queue
}

func NewWorker() Worker {
	worker := &worker{}
	worker.queue = sm.NewSafeQueue(10000, 100, worker.onItem)
	worker.queue.Start()
	return worker
}

func (w *worker) Submit(ctx context.Context, task func() error) error {
	_, err := w.queue.Enqueue(task)
	return err
}

func (w *worker) onItem(items ...any) {
	for _, item := range items {
		item.(func() error)()
	}
}

func (w *worker) Stop() {
	w.queue.Stop()
}

type TxnFactory func() (client.TxnOperator, error)

type CDCTaskExecutor2 struct {
	accountID            uint64
	tables               *btree.BTreeG[*TableInfo_2]
	tableMu              sync.RWMutex
	getInsertWatermarkFn func(
		ctx context.Context,
		tableID uint64,
		accountID int32,
		indexID int32,
	) error
	getFlushWatermarkFn func(
		ctx context.Context,
		tableID uint64,
		watermark types.TS,
		accountID int32,
		indexID int32,
		errorCode int,
		info string,
		errorMsg string,
	) error
	replayFn replayFn
	deleteFn deleteFn
	packer   *types.Packer
	mp       *mpool.MPool
	spec     *task.CreateCdcDetails

	logger             *zap.Logger //todo: replace logutil.Infof
	sqlExecutorFactory func() ie.InternalExecutor
	// attachToTask       func(context.Context, uint64, taskservice.ActiveRoutine) error
	cnUUID string
	// ts                 taskservice.TaskService
	// fs                 fileservice.FileService
	txnFactory    func() (client.TxnOperator, error)
	sinkerFactory func(dbName, tableName string, tableDef []engine.TableDef) (cdc.Sinker, error)
	txnEngine     engine.Engine

	rpcHandleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.GetChangedTableListReq,
		resp *cmd_util.GetChangedTableListResp,
	) (func(), error) // for test

	ctx    context.Context
	cancel context.CancelFunc

	worker Worker
	wg     sync.WaitGroup
}

func NewCDCTaskExecutor2(
	ctx context.Context,
	accountID uint64,
	spec *task.CreateCdcDetails,
	sqlExecutorFactory func() ie.InternalExecutor,
	sinkerFactory func(dbName, tableName string, tableDef []engine.TableDef) (cdc.Sinker, error),
	txnFactory TxnFactory,
	txnEngine engine.Engine,
	cdUUID string,
	rpcHandleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.GetChangedTableListReq,
		resp *cmd_util.GetChangedTableListResp,
	) (func(), error),
	getInsertWatermarkFn func(
		ctx context.Context,
		tableID uint64,
		accountID int32,
		indexID int32,
	) error,
	getFlushWatermarkFn func(
		ctx context.Context,
		tableID uint64,
		watermark types.TS,
		accountID int32,
		indexID int32,
		errorCode int,
		info string,
		errorMsg string,
	) error,
	replayFn replayFn,
	deleteFn deleteFn,
	mp *mpool.MPool,
) *CDCTaskExecutor2 {
	ctx, cancel := context.WithCancel(ctx)
	worker := NewWorker()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(accountID))
	return &CDCTaskExecutor2{
		ctx:                  ctx,
		cancel:               cancel,
		packer:               types.NewPacker(),
		tables:               btree.NewBTreeGOptions(tableInfoLess, btree.Options{NoLocks: true}),
		spec:                 spec,
		sqlExecutorFactory:   sqlExecutorFactory,
		cnUUID:               cdUUID,
		txnFactory:           txnFactory,
		sinkerFactory:        sinkerFactory,
		txnEngine:            txnEngine,
		worker:               worker,
		wg:                   sync.WaitGroup{},
		rpcHandleFn:          rpcHandleFn,
		tableMu:              sync.RWMutex{},
		getInsertWatermarkFn: getInsertWatermarkFn,
		getFlushWatermarkFn:  getFlushWatermarkFn,
		replayFn:             replayFn,
		deleteFn:             deleteFn,
		mp:                   mp,
	}
}

// scan candidates
func (exec *CDCTaskExecutor2) getAllTables() []*TableInfo_2 {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	ret := make([]*TableInfo_2, 0)
	items := exec.tables.Items()
	for _, t := range items {
		ret = append(ret, t)
	}
	return ret
}

// get watermark, register new table
func (exec *CDCTaskExecutor2) getTable(tableID uint64) (*TableInfo_2, bool) {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	return exec.tables.Get(&TableInfo_2{tableID: tableID})
}

func (exec *CDCTaskExecutor2) setTable(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Set(table)
}
func (exec *CDCTaskExecutor2) deleteTableEntry(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Delete(table)
}

func (exec *CDCTaskExecutor2) Resume() error {
	// restart
	return nil
}
func (exec *CDCTaskExecutor2) Pause() error {
	// stop
	return nil
}
func (exec *CDCTaskExecutor2) Cancel() error {
	// stop
	return nil
}
func (exec *CDCTaskExecutor2) Restart() error {
	// stop
	// restart
	return nil
}
func (exec *CDCTaskExecutor2) Start() {
	exec.wg.Add(1)
	go exec.run()
}

func (exec *CDCTaskExecutor2) Stop() {
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
}

func (exec *CDCTaskExecutor2) run() {
	defer exec.wg.Done()
	trigger := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-exec.ctx.Done():
			return
		case <-trigger.C:
			candidateTables := exec.getCandidateTables()
			tables, toTS, err := exec.getDirtyTables(exec.ctx, candidateTables, exec.cnUUID, exec.txnEngine)
			if err != nil {
				logutil.Errorf("cdc task %s get dirty tables failed, err: %v", exec.spec.TaskName, err)
				continue
			}
			for _, table := range candidateTables {
				_, ok := tables[table.tableID]
				if ok {
					task, err := exec.getIterationTask(exec.ctx, table, toTS)
					if err != nil {
						logutil.Errorf("cdc task %s get dirty tables failed, err: %v", exec.spec.TaskName, err)
						continue
					}
					exec.worker.Submit(exec.ctx, task)
				} else {
					table.SetWatermark(toTS)
				}
			}
		}
	}
}

func (exec *CDCTaskExecutor2) GetWatermark(srcTableID uint64) types.TS {
	table, ok := exec.getTable(srcTableID)
	if !ok {
		return types.TS{}
	}
	return table.GetWatermark()
}

func (exec *CDCTaskExecutor2) startTable(ctx context.Context, table *cdc.PatternTuple) (err error) {
	var tableInfo *TableInfo_2
	if tableInfo, err = exec.getTableInfoWithPattern(ctx, table, exec.txnEngine); err != nil {
		return err
	}
	_, ok := exec.getTable(tableInfo.tableID)
	if ok {
		return moerr.NewInternalError(ctx, "table already started")
	}
	err = tableInfo.ReplayWatermark(ctx, exec.sqlExecutorFactory(), exec.accountID)
	if err != nil {
		//TODO: check error
		err = exec.initTable(ctx, tableInfo)
		if err != nil {
			return err
		}
	}
	exec.setTable(tableInfo)
	return
}

func (exec *CDCTaskExecutor2) initTable(ctx context.Context, tableInfo *TableInfo_2) (err error) {
	to := types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
	if err = tableInfo.InsertIndexWatermark(ctx, exec.sqlExecutorFactory(), exec.accountID); err != nil {
		return err
	}
	task, err := exec.getIterationTask(ctx, tableInfo, to)
	if err != nil {
		return err
	}
	exec.worker.Submit(ctx, task)
	return
}

func (exec *CDCTaskExecutor2) pauseTable(ctx context.Context, table *cdc.PatternTuple) (err error) {
	var tableInfo *TableInfo_2
	if tableInfo, err = exec.getTableInfoWithPattern(ctx, table, exec.txnEngine); err != nil {
		return err
	}
	exec.deleteTableEntry(tableInfo)
	return nil
}
func (exec *CDCTaskExecutor2) dropTable(ctx context.Context, table *cdc.PatternTuple) (err error) {
	var tableInfo *TableInfo_2
	if tableInfo, err = exec.getTableInfoWithPattern(ctx, table, exec.txnEngine); err != nil {
		return err
	}
	err = tableInfo.Delete(ctx, exec.sqlExecutorFactory(), exec.accountID)
	if err != nil {
		return err
	}
	exec.deleteTableEntry(tableInfo)
	return nil
}

func (exec *CDCTaskExecutor2) StartTables(
	ctx context.Context,
	opts CDCCreateTaskOptions,
) (err error) {
	var tablesPatternTuples cdc.PatternTuples
	cdc.JsonDecode(opts.PitrTables, &tablesPatternTuples)
	for _, table := range tablesPatternTuples.Pts {
		err = exec.startTable(ctx, table)
		if err != nil {
			return err
		}
	}
	return
}

func (exec *CDCTaskExecutor2) PauseTables(
	ctx context.Context,
	opts CDCCreateTaskOptions,
) (err error) {
	var tablesPatternTuples cdc.PatternTuples
	cdc.JsonDecode(opts.PitrTables, &tablesPatternTuples)
	for _, table := range tablesPatternTuples.Pts {
		err = exec.pauseTable(ctx, table)
		if err != nil {
			return err
		}
	}
	return
}

func (exec *CDCTaskExecutor2) DropTables(
	ctx context.Context,
	opts CDCCreateTaskOptions,
) (err error) {
	var tablesPatternTuples cdc.PatternTuples
	cdc.JsonDecode(opts.PitrTables, &tablesPatternTuples)
	for _, table := range tablesPatternTuples.Pts {
		err = exec.dropTable(ctx, table)
		if err != nil {
			return err
		}
	}
	return
}

func (exec *CDCTaskExecutor2) getTableInfoWithPattern(
	ctx context.Context,
	tablePattern *cdc.PatternTuple,
	txnEngine engine.Engine,
) (tableInfo *TableInfo_2, err error) {
	if tablePattern.Source.Database == cdc.CDCPitrGranularity_All ||
		tablePattern.Source.Table == cdc.CDCPitrGranularity_All {
		panic("not support")
		//for each table in mo tables
	}
	return exec.getTableInfoWithTableName(
		ctx,
		tablePattern.Source.Database,
		tablePattern.Source.Table,
		tablePattern.Sink.Database,
		tablePattern.Sink.Table,
	)

}
func (exec *CDCTaskExecutor2) getRelation(
	ctx context.Context,
	dbName, tableName string,
) (table engine.Relation, txnOp client.TxnOperator, err error) {
	txnOp, err = exec.txnFactory()
	if err != nil {
		logutil.Errorf("cdc task %s get txn op failed, err: %v", exec.spec.TaskName, err)
		return
	}
	var db engine.Database
	if db, err = exec.txnEngine.Database(ctx, dbName, txnOp); err != nil {
		return
	}

	if table, err = db.Relation(ctx, tableName, nil); err != nil {
		return
	}
	return
}
func (exec *CDCTaskExecutor2) getTableInfoWithTableName(
	ctx context.Context,
	dbName, tableName string,
	sinkeDBName, sinkeTableName string,
) (tableInfo *TableInfo_2, err error) {

	var table engine.Relation
	var txn client.TxnOperator
	if table, txn, err = exec.getRelation(ctx, dbName, tableName); err != nil {
		return
	}
	defer txn.Commit(ctx)
	def := table.CopyTableDef(ctx)

	sinker, err := exec.sinkerFactory(
		sinkeDBName,
		sinkeTableName,
		engine.PlanColsToExeCols(def.Cols),
	)
	if err != nil {
		return
	}

	sinker.Run(ctx, nil)

	tableInfo = NewTableInfo_2(
		def.DbId,
		def.TblId,
		func(ctx context.Context) (engine.Relation, client.TxnOperator, error) {
			return exec.getRelation(ctx, dbName, tableName)
		},
		sinker,
		exec.getFlushWatermarkFn,
		exec.getInsertWatermarkFn,
		exec.replayFn,
		exec.deleteFn,
	)
	return
}
func (exec *CDCTaskExecutor2) getCandidateTables() []*TableInfo_2 {
	ret := make([]*TableInfo_2, 0)
	items := exec.getAllTables()
	for _, t := range items {
		if t.GetState() == TableState_Running {
			continue
		}
		ret = append(ret, t)
	}
	return ret
}
func (exec *CDCTaskExecutor2) getDirtyTables(
	ctx context.Context,
	candidateTables []*TableInfo_2,
	service string,
	eng engine.Engine,
) (tables map[uint64]struct{}, toTS types.TS, err error) {

	accs := make([]uint64, 0, len(candidateTables))
	dbs := make([]uint64, 0, len(candidateTables))
	tbls := make([]uint64, 0, len(candidateTables))
	ts := make([]timestamp.Timestamp, 0, len(candidateTables))
	for _, t := range candidateTables {
		accs = append(accs, uint64(exec.accountID))
		dbs = append(dbs, t.dbID)
		tbls = append(tbls, t.tableID)
		ts = append(ts, t.GetWatermark().ToTimestamp())
	}
	// tmpTS := types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
	tables = make(map[uint64]struct{})
	disttae.GetChangedTableList(
		ctx,
		service,
		eng,
		accs,
		dbs,
		tbls,
		ts,
		&toTS,
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
			tables[uint64(tableID)] = struct{}{}
		},
		exec.rpcHandleFn,
	)
	return
}

func (exec *CDCTaskExecutor2) getIterationTask(
	ctx context.Context,
	table *TableInfo_2,
	toTs types.TS,
) (task func() error, err error) {
	from := types.TimestampToTS(table.GetWatermark().ToTimestamp())

	table.SetState(TableState_Running)
	rel, txn, err := table.rel(ctx)
	if err != nil {
		return nil, err
	}

	return func() error {
		err := cdc.CollectChanges_2(
			ctx,
			rel,
			from,
			toTs,
			table.sinker,
			true,
			exec.packer,
			exec.mp,
		)
		txn.Commit(ctx)
		// when collect from 0, changes_handle collect all data,
		// so we need to update toTs to the latest snapshot ts
		if from.IsEmpty() {
			toTs = types.TimestampToTS(txn.SnapshotTS())
		}
		var errorMsg string
		if err == nil {
			table.SetWatermark(toTs)
		} else {
			errorMsg = err.Error()
		}
		table.SetState(TableState_Finished)
		table.FlushWatermark(
			ctx,
			exec.sqlExecutorFactory(),
			exec.accountID,
			getErrorCode(err),
			errorMsg,
		)
		return nil
	}, nil
}

func getErrorCode(err error) int {
	//TODO
	return 0
}
