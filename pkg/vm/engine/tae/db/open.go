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

package db

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	// "github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	WALDir = "wal"
)

func fillRuntimeOptions(opts *options.Options) {
	common.RuntimeCNMergeMemControl.Store(opts.MergeCfg.CNMergeMemControlHint)
	common.RuntimeMinCNMergeSize.Store(opts.MergeCfg.CNTakeOverExceed)
	common.RuntimeCNTakeOverAll.Store(opts.MergeCfg.CNTakeOverAll)
	common.RuntimeOverallFlushMemCap.Store(opts.CheckpointCfg.OverallFlushMemControl)
	if opts.IsStandalone {
		common.IsStandaloneBoost.Store(true)
	}
	if opts.MergeCfg.CNStandaloneTake {
		common.ShouldStandaloneCNTakeOver.Store(true)
	}
	if opts.MergeCfg.DisableZMBasedMerge {
		common.RuntimeDisableZMBasedMerge.Store(true)
	}
}

func Open(ctx context.Context, dirname string, opts *options.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)

	logutil.Info(
		"open-tae",
		common.OperationField("Start"),
		common.OperandField("open"),
	)
	totalTime := time.Now()

	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
		logutil.Info(
			"open-tae", common.OperationField("End"),
			common.OperandField("open"),
			common.AnyField("cost", time.Since(totalTime)),
			common.AnyField("err", err),
		)
	}()

	opts = opts.FillDefaults(dirname)
	fillRuntimeOptions(opts)

	wbuf := &bytes.Buffer{}
	werr := toml.NewEncoder(wbuf).Encode(opts)
	logutil.Info(
		"open-tae",
		common.OperationField("Config"),
		common.AnyField("toml", wbuf.String()),
		common.ErrorField(werr),
	)
	serviceDir := path.Join(dirname, "data")
	if opts.Fs == nil {
		// TODO:fileservice needs to be passed in as a parameter
		opts.Fs = objectio.TmpNewFileservice(ctx, path.Join(dirname, "data"))
	}
	if opts.LocalFs == nil {
		opts.LocalFs = objectio.TmpNewFileservice(ctx, path.Join(dirname, "data"))
	}

	db = &DB{
		Dir:       dirname,
		Opts:      opts,
		Closed:    new(atomic.Value),
		usageMemo: logtail.NewTNUsageMemo(nil),
	}
	fs := objectio.NewObjectFS(opts.Fs, serviceDir)
	localFs := objectio.NewObjectFS(opts.LocalFs, serviceDir)
	transferTable, err := model.NewTransferTable[*model.TransferHashPage](ctx, opts.LocalFs)
	if err != nil {
		panic(fmt.Sprintf("open-tae: model.NewTransferTable failed, %s", err))
	}

	switch opts.LogStoreT {
	case options.LogstoreBatchStore:
		db.Wal = wal.NewDriverWithBatchStore(opts.Ctx, dirname, WALDir, nil)
	case options.LogstoreLogservice:
		db.Wal = wal.NewDriverWithLogservice(opts.Ctx, opts.Lc)
	}
	scheduler := newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	db.Runtime = dbutils.NewRuntime(
		dbutils.WithRuntimeTransferTable(transferTable),
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeLocalFS(localFs),
		dbutils.WithRuntimeSmallPool(dbutils.MakeDefaultSmallPool("small-vector-pool")),
		dbutils.WithRuntimeTransientPool(dbutils.MakeDefaultTransientPool("trasient-vector-pool")),
		dbutils.WithRuntimeScheduler(scheduler),
		dbutils.WithRuntimeOptions(db.Opts),
	)

	dataFactory := tables.NewDataFactory(
		db.Runtime, db.Dir,
	)
	catalog.DefaultTableDataFactory = dataFactory.MakeTableFactory()
	if db.Catalog, err = catalog.OpenCatalog(db.usageMemo); err != nil {
		return
	}
	db.usageMemo.C = db.Catalog

	// Init and start txn manager
	txnStoreFactory := txnimpl.TxnStoreFactory(
		opts.Ctx,
		db.Catalog,
		db.Wal,
		db.Runtime,
		dataFactory,
		opts.MaxMessageSize,
	)
	txnFactory := txnimpl.TxnFactory(db.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory, db.Opts.Clock)
	db.LogtailMgr = logtail.NewManager(
		db.Runtime,
		int(db.Opts.LogtailCfg.PageSize),
		db.TxnMgr.Now,
	)
	db.Runtime.Now = db.TxnMgr.Now
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)
	db.TxnMgr.Start(opts.Ctx)
	db.LogtailMgr.Start()
	db.BGCheckpointRunner = checkpoint.NewRunner(
		opts.Ctx,
		db.Runtime,
		db.Catalog,
		logtail.NewDirtyCollector(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor)),
		db.Wal,
		checkpoint.WithFlushInterval(opts.CheckpointCfg.FlushInterval),
		checkpoint.WithCollectInterval(opts.CheckpointCfg.ScanInterval),
		checkpoint.WithMinCount(int(opts.CheckpointCfg.MinCount)),
		checkpoint.WithCheckpointBlockRows(opts.CheckpointCfg.BlockRows),
		checkpoint.WithCheckpointSize(opts.CheckpointCfg.Size),
		checkpoint.WithMinIncrementalInterval(opts.CheckpointCfg.IncrementalInterval),
		checkpoint.WithGlobalMinCount(int(opts.CheckpointCfg.GlobalMinCount)),
		checkpoint.WithGlobalVersionInterval(opts.CheckpointCfg.GlobalVersionInterval),
		checkpoint.WithReserveWALEntryCount(opts.CheckpointCfg.ReservedWALEntryCount))

	now := time.Now()
	ckpReplayer := db.BGCheckpointRunner.Replay(dataFactory)
	defer ckpReplayer.Close()
	if err = ckpReplayer.ReadCkpFiles(); err != nil {
		panic(err)
	}

	// 1. replay three tables objectlist
	checkpointed, ckpLSN, valid, err := ckpReplayer.ReplayThreeTablesObjectlist()
	if err != nil {
		panic(err)
	}

	var txn txnif.AsyncTxn
	{
		// create a txn manually
		txnIdAlloc := common.NewTxnIDAllocator()
		store := txnStoreFactory()
		txn = txnFactory(db.TxnMgr, store, txnIdAlloc.Alloc(), checkpointed, types.TS{})
		store.BindTxn(txn)
	}
	// 2. replay all table Entries
	if err = ckpReplayer.ReplayCatalog(txn); err != nil {
		panic(err)
	}

	// 3. replay other tables' objectlist
	if err = ckpReplayer.ReplayObjectlist(); err != nil {
		panic(err)
	}
	logutil.Info(
		"open-tae",
		common.OperationField("replay"),
		common.OperandField("checkpoints"),
		common.AnyField("cost", time.Since(now)),
		common.AnyField("checkpointed", checkpointed.ToString()),
	)

	now = time.Now()
	db.Replay(dataFactory, checkpointed, ckpLSN, valid)
	db.Catalog.ReplayTableRows()

	// checkObjectState(db)
	logutil.Info(
		"open-tae",
		common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("cost", time.Since(now)),
	)

	db.DBLocker, dbLocker = dbLocker, nil

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	db.MergeScheduler = merge.NewScheduler(db.Runtime, merge.NewTaskServiceGetter(opts.TaskServiceGetter))
	scanner.RegisterOp(db.MergeScheduler)
	db.Wal.Start()
	db.BGCheckpointRunner.Start()

	db.BGScanner = w.NewHeartBeater(
		opts.CheckpointCfg.ScanInterval,
		scanner)
	db.BGScanner.Start()
	// TODO: WithGCInterval requires configuration parameters
	gc2.SetDeleteTimeout(opts.GCCfg.GCDeleteTimeout)
	gc2.SetDeleteBatchSize(opts.GCCfg.GCDeleteBatchSize)
	cleaner := gc2.NewCheckpointCleaner(opts.Ctx,
		opts.SID, fs, db.BGCheckpointRunner,
		gc2.WithCanGCCacheSize(opts.GCCfg.CacheSize),
		gc2.WithMaxMergeCheckpointCount(opts.GCCfg.GCMergeCount),
		gc2.WithEstimateRows(opts.GCCfg.GCestimateRows),
		gc2.WithGCProbility(opts.GCCfg.GCProbility),
		gc2.WithCheckOption(opts.GCCfg.CheckGC),
		gc2.WithGCCheckpointOption(!opts.CheckpointCfg.DisableGCCheckpoint))
	cleaner.AddChecker(
		func(item any) bool {
			checkpoint := item.(*checkpoint.CheckpointEntry)
			ts := types.BuildTS(time.Now().UTC().UnixNano()-int64(opts.GCCfg.GCTTL), 0)
			endTS := checkpoint.GetEnd()
			return !endTS.GE(&ts)
		}, cmd_util.CheckerKeyTTL)
	db.DiskCleaner = gc2.NewDiskCleaner(cleaner)
	db.DiskCleaner.Start()
	// Init gc manager at last
	// TODO: clean-try-gc requires configuration parameters
	cronJobs := []func(*gc.Manager){
		gc.WithCronJob(
			"clean-transfer-table",
			opts.CheckpointCfg.TransferInterval,
			func(_ context.Context) (err error) {
				db.Runtime.PoolUsageReport()
				db.Runtime.TransferDelsMap.Prune(opts.TransferTableTTL)
				transferTable.RunTTL()
				return
			}),

		gc.WithCronJob(
			"disk-gc",
			opts.GCCfg.ScanGCInterval,
			func(ctx context.Context) (err error) {
				db.DiskCleaner.GC(ctx)
				return
			}),
		gc.WithCronJob(
			"checkpoint-gc",
			opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) error {
				if opts.CheckpointCfg.DisableGCCheckpoint {
					return nil
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetCheckpointGCWaterMark()
				if gcWaterMark == nil {
					return nil
				}
				return db.BGCheckpointRunner.GCByTS(ctx, *gcWaterMark)
			}),
		gc.WithCronJob(
			"catalog-gc",
			opts.CatalogCfg.GCInterval,
			func(ctx context.Context) error {
				if opts.CatalogCfg.DisableGC {
					return nil
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetScanWaterMark()
				if gcWaterMark == nil {
					return nil
				}
				db.Catalog.GCByTS(ctx, gcWaterMark.GetEnd())
				return nil
			}),
		gc.WithCronJob(
			"logtail-gc",
			opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) error {
				ckp := db.BGCheckpointRunner.MaxIncrementalCheckpoint()
				if ckp != nil {
					// use previous end to gc logtail
					ts := types.BuildTS(ckp.GetStart().Physical(), 0) // GetStart is previous + 1, reset it here
					if updated := db.LogtailMgr.GCByTS(ctx, ts); updated {
						logutil.Info(db.Runtime.ExportLogtailStats())
					}
				}
				return nil
			},
		),
		gc.WithCronJob(
			"prune-lockmerge",
			options.DefaultLockMergePruneInterval,
			func(ctx context.Context) error {
				db.Runtime.LockMergeService.Prune()
				return nil
			},
		),
	}
	if opts.CheckpointCfg.MetadataCheckInterval != 0 {
		cronJobs = append(cronJobs,
			gc.WithCronJob(
				"metadata-check",
				opts.CheckpointCfg.MetadataCheckInterval,
				func(ctx context.Context) error {
					db.Catalog.CheckMetadata()
					return nil
				}))
	}
	db.GCManager = gc.NewManager(cronJobs...)

	db.GCManager.Start()

	go TaeMetricsTask(ctx)

	// database, err := db.Catalog.GetDatabaseByID(272515)
	// if err != nil {
	// 	panic(err)
	// }
	// table, err := database.GetTableEntryByID(272516)
	// if err != nil {
	// 	panic(err)
	// }

	// segID, err := uuid.Parse("0195f99d-6d8f-7e7d-9672-0372dd368fa2")
	// var objectID objectio.ObjectId
	// copy(objectID[:types.UuidSize], segID[:])
	// stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, false)
	// objectio.SetObjectStatsExtent(stats, objectio.NewExtent(1, 7542, 1165, 8279))
	// objectio.SetObjectStatsRowCnt(stats, 1)
	// objectio.SetObjectStatsBlkCnt(stats, 1)
	// objectio.SetObjectStatsSize(stats, 8771)
	// objectio.SetObjectStatsOriginSize(stats, 17967)
	// zm := objectio.NewZM(types.T_varchar, types.T_varchar.ToType().Scale)
	// zm.Update([]byte("CID-0886783ec7de"))
	// zm.Update([]byte("CID-0886783ec7de"))
	// objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
	// objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectNameWithObjectID(&objectID))
	// e := &catalog.ObjectEntry{
	// 	ObjectNode: catalog.ObjectNode{
	// 		SortHint:    db.Catalog.NextObject(),
	// 		IsTombstone: false,
	// 	},
	// 	EntryMVCCNode: catalog.EntryMVCCNode{
	// 		CreatedAt: types.BuildTS(1743649598915390142, 0),
	// 	},
	// 	CreateNode: txnbase.TxnMVCCNode{
	// 		Start:   types.BuildTS(1743649598863585565, 0),
	// 		Prepare: types.BuildTS(1743649598915390142, 0),
	// 		End:     types.BuildTS(1743649598915390142, 0),
	// 	},
	// 	ObjectState: catalog.ObjectState_Create_ApplyCommit,
	// 	ObjectMVCCNode: catalog.ObjectMVCCNode{
	// 		ObjectStats: *stats,
	// 	},
	// }
	// e.SetTable(table)
	// table.AddEntryLocked(e)

	// segID2, err := uuid.Parse("019610b9-b08e-7cfc-97e0-6ccf6d055848")
	// var objectID2 objectio.ObjectId
	// copy(objectID2[:types.UuidSize], segID2[:])
	// stats2 := objectio.NewObjectStatsWithObjectID(&objectID2, false, true, false)
	// objectio.SetObjectStatsExtent(stats2, objectio.NewExtent(1, 2629, 1317, 8279))
	// objectio.SetObjectStatsRowCnt(stats2, 1)
	// objectio.SetObjectStatsBlkCnt(stats2, 1)
	// objectio.SetObjectStatsSize(stats2, 4010)
	// objectio.SetObjectStatsOriginSize(stats2, 12636)
	// zm2 := objectio.NewZM(types.T_varchar, types.T_varchar.ToType().Scale)
	// zm2.Update([]byte("CID-585ae638bd68"))
	// zm2.Update([]byte("CID-585ae638bd68"))
	// objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)
	// objectio.SetObjectStatsObjectName(stats2, objectio.BuildObjectNameWithObjectID(&objectID2))
	// e2 := &catalog.ObjectEntry{
	// 	ObjectNode: catalog.ObjectNode{
	// 		SortHint:    db.Catalog.NextObject(),
	// 		IsTombstone: false,
	// 	},
	// 	EntryMVCCNode: catalog.EntryMVCCNode{
	// 		CreatedAt: types.BuildTS(1744037327046535626, 0),
	// 	},
	// 	CreateNode: txnbase.TxnMVCCNode{
	// 		Start:   types.BuildTS(1744037326988921478, 0),
	// 		Prepare: types.BuildTS(1744037327046535626, 0),
	// 		End:     types.BuildTS(1744037327046535626, 0),
	// 	},
	// 	ObjectState: catalog.ObjectState_Create_ApplyCommit,
	// 	ObjectMVCCNode: catalog.ObjectMVCCNode{
	// 		ObjectStats: *stats2,
	// 	},
	// }
	// e2.SetTable(table)
	// table.AddEntryLocked(e2)

	// segID3, err := uuid.Parse("019610b9-d3fa-79b2-b13d-6db4c4140843")
	// var objectID3 objectio.ObjectId
	// copy(objectID3[:types.UuidSize], segID3[:])
	// stats3 := objectio.NewObjectStatsWithObjectID(&objectID3, true, false, false)
	// objectio.SetObjectStatsExtent(stats3, objectio.NewExtent(1, 3337, 1385, 8775))
	// objectio.SetObjectStatsRowCnt(stats3, 5)
	// objectio.SetObjectStatsBlkCnt(stats3, 1)
	// objectio.SetObjectStatsSize(stats3, 4786)
	// objectio.SetObjectStatsOriginSize(stats3, 17140)
	// zm3 := objectio.NewZM(types.T_varchar, types.T_varchar.ToType().Scale)
	// zm3.Update([]byte("CID-585ae638bd68"))
	// zm3.Update([]byte("CID-585ae638bd68"))
	// objectio.SetObjectStatsSortKeyZoneMap(stats3, zm3)
	// objectio.SetObjectStatsObjectName(stats3, objectio.BuildObjectNameWithObjectID(&objectID3))
	// e3 := &catalog.ObjectEntry{
	// 	ObjectNode: catalog.ObjectNode{
	// 		SortHint:    db.Catalog.NextObject(),
	// 		IsTombstone: false,
	// 	},
	// 	EntryMVCCNode: catalog.EntryMVCCNode{
	// 		CreatedAt: types.BuildTS(1744037327018917818, 0),
	// 	},
	// 	CreateNode: txnbase.TxnMVCCNode{
	// 		Start:   types.BuildTS(1744037326988921478, 1),
	// 		Prepare: types.BuildTS(1744037327018917818, 0),
	// 		End:     types.BuildTS(1744037327018917818, 0),
	// 	},
	// 	ObjectState: catalog.ObjectState_Create_ApplyCommit,
	// 	ObjectMVCCNode: catalog.ObjectMVCCNode{
	// 		ObjectStats: *stats3,
	// 	},
	// }
	// e3.SetTable(table)
	// table.AddEntryLocked(e3)

	// segID4, err := uuid.Parse("019610b9-b08e-7c97-98c1-f734beeee2f2")
	// var objectID4 objectio.ObjectId
	// copy(objectID4[:types.UuidSize], segID4[:])
	// stats4 := objectio.NewObjectStatsWithObjectID(&objectID4, false, true, false)
	// objectio.SetObjectStatsExtent(stats4, objectio.NewExtent(1, 675, 396, 1335))
	// objectio.SetObjectStatsRowCnt(stats4, 4)
	// objectio.SetObjectStatsBlkCnt(stats4, 1)
	// objectio.SetObjectStatsSize(stats4, 1135)
	// objectio.SetObjectStatsOriginSize(stats4, 2231)
	// zm4 := objectio.NewZM(types.T_Rowid, types.T_Rowid.ToType().Scale)
	// objectio.SetObjectStatsSortKeyZoneMap(stats4, zm4)
	// objectio.SetObjectStatsObjectName(stats4, objectio.BuildObjectNameWithObjectID(&objectID4))
	// e4 := &catalog.ObjectEntry{
	// 	ObjectNode: catalog.ObjectNode{
	// 		SortHint:    db.Catalog.NextObject(),
	// 		IsTombstone: true,
	// 	},
	// 	EntryMVCCNode: catalog.EntryMVCCNode{
	// 		CreatedAt: types.BuildTS(1744037327018917818, 0),
	// 	},
	// 	CreateNode: txnbase.TxnMVCCNode{
	// 		Start:   types.BuildTS(1744037326988921478, 0),
	// 		Prepare: types.BuildTS(1744037327018917818, 0),
	// 		End:     types.BuildTS(1744037327018917818, 0),
	// 	},
	// 	ObjectState: catalog.ObjectState_Create_ApplyCommit,
	// 	ObjectMVCCNode: catalog.ObjectMVCCNode{
	// 		ObjectStats: *stats4,
	// 	},
	// }
	// e4.SetTable(table)
	// table.AddEntryLocked(e4)

	// segID5, err := uuid.Parse("019610b9-d3fa-7a66-855b-c64aa8285f41")
	// var objectID5 objectio.ObjectId
	// copy(objectID5[:types.UuidSize], segID5[:])
	// stats5 := objectio.NewObjectStatsWithObjectID(&objectID5, true, true, false)
	// objectio.SetObjectStatsExtent(stats5, objectio.NewExtent(1, 669, 404, 1335))
	// objectio.SetObjectStatsRowCnt(stats5, 5)
	// objectio.SetObjectStatsBlkCnt(stats5, 1)
	// objectio.SetObjectStatsSize(stats5, 1137)
	// objectio.SetObjectStatsOriginSize(stats5, 2291)
	// zm5 := objectio.NewZM(types.T_Rowid, types.T_Rowid.ToType().Scale)
	// minRowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID2, 0, 0)
	// zm5.Update(minRowID)
	// maxRowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID3, 0, 3)
	// zm5.Update(maxRowID)
	// objectio.SetObjectStatsSortKeyZoneMap(stats5, zm5)
	// objectio.SetObjectStatsObjectName(stats5, objectio.BuildObjectNameWithObjectID(&objectID5))
	// e5 := &catalog.ObjectEntry{
	// 	ObjectNode: catalog.ObjectNode{
	// 		SortHint:    db.Catalog.NextObject(),
	// 		IsTombstone: true,
	// 	},
	// 	EntryMVCCNode: catalog.EntryMVCCNode{
	// 		CreatedAt: types.BuildTS(1744037336058707664, 0),
	// 	},
	// 	CreateNode: txnbase.TxnMVCCNode{
	// 		Start:   types.BuildTS(1744037336052462982, 1),
	// 		Prepare: types.BuildTS(1744037336058707664, 0),
	// 		End:     types.BuildTS(1744037336058707664, 0),
	// 	},
	// 	ObjectState: catalog.ObjectState_Create_ApplyCommit,
	// 	ObjectMVCCNode: catalog.ObjectMVCCNode{
	// 		ObjectStats: *stats5,
	// 	},
	// }
	// e5.SetTable(table)
	// table.AddEntryLocked(e5)

	// For debug or test

	fmt.Println(db.Catalog.SimplePPString(common.PPL3))
	return
}

// TODO: remove it
// func checkObjectState(db *DB) {
// 	p := &catalog.LoopProcessor{}
// 	p.ObjectFn = func(oe *catalog.ObjectEntry) error {
// 		if oe.IsAppendable() == oe.IsSorted() {
// 			panic(fmt.Sprintf("logic err %v", oe.ID.String()))
// 		}
// 		return nil
// 	}
// 	db.Catalog.RecurLoop(p)
// }

func TaeMetricsTask(ctx context.Context) {
	logutil.Info("tae metrics task started")
	defer logutil.Info("tae metrics task exit")

	timer := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			mpoolAllocatorSubTask()
		}
	}

}

func mpoolAllocatorSubTask() {
	v2.MemTAEDefaultAllocatorGauge.Set(float64(common.DefaultAllocator.CurrNB()))
	v2.MemTAEDefaultHighWaterMarkGauge.Set(float64(common.DefaultAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMutableAllocatorGauge.Set(float64(common.MutMemAllocator.CurrNB()))
	v2.MemTAEMutableHighWaterMarkGauge.Set(float64(common.MutMemAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAESmallAllocatorGauge.Set(float64(common.SmallAllocator.CurrNB()))
	v2.MemTAESmallHighWaterMarkGauge.Set(float64(common.SmallAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEVectorPoolDefaultAllocatorGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().CurrNB()))
	v2.MemTAEVectorPoolDefaultHighWaterMarkGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().Stats().HighWaterMark.Load()))

	v2.MemTAELogtailAllocatorGauge.Set(float64(common.LogtailAllocator.CurrNB()))
	v2.MemTAELogtailHighWaterMarkGauge.Set(float64(common.LogtailAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAECheckpointAllocatorGauge.Set(float64(common.CheckpointAllocator.CurrNB()))
	v2.MemTAECheckpointHighWaterMarkGauge.Set(float64(common.CheckpointAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMergeAllocatorGauge.Set(float64(common.MergeAllocator.CurrNB()))
	v2.MemTAEMergeHighWaterMarkGauge.Set(float64(common.MergeAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEWorkSpaceAllocatorGauge.Set(float64(common.WorkspaceAllocator.CurrNB()))
	v2.MemTAEWorkSpaceHighWaterMarkGauge.Set(float64(common.WorkspaceAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEDebugAllocatorGauge.Set(float64(common.DebugAllocator.CurrNB()))
	v2.MemTAEDebugHighWaterMarkGauge.Set(float64(common.DebugAllocator.Stats().HighWaterMark.Load()))

}
