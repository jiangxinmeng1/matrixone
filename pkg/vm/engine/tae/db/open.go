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
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	WALDir = "wal"

	Phase_Open = "open-tae"
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

func Open(
	ctx context.Context,
	dirname string,
	opts *options.Options,
	dbOpts ...DBOption,
) (db *DB, err error) {
	opts = opts.FillDefaults(dirname)
	// TODO: remove
	fillRuntimeOptions(opts)

	var (
		dbLocker      io.Closer
		startTime     = time.Now()
		rollbackSteps stepFuncs
		logger        = logutil.Info
	)

	defer func() {
		if err == nil && dbLocker != nil {
			db.DBLocker, dbLocker = dbLocker, nil
		}
		if dbLocker != nil {
			dbLocker.Close()
		}
		if err != nil {
			if err2 := rollbackSteps.Apply("open-tae", true, 1); err2 != nil {
				panic(fmt.Sprintf("open-tae: rollback failed, %s", err2))
			}
			logger = logutil.Error
		}
		logger(
			Phase_Open,
			zap.Duration("total-cost", time.Since(startTime)),
			zap.String("mode", db.GetTxnMode().String()),
			zap.String("db-dirname", dirname),
			zap.String("config", opts.JsonString()),
			zap.Error(err),
		)
	}()

	db = &DB{
		Dir:       dirname,
		Opts:      opts,
		Closed:    new(atomic.Value),
		usageMemo: logtail.NewTNUsageMemo(nil),
	}
	for _, opt := range dbOpts {
		opt(db)
	}

	if db.IsWriteMode() {
		if dbLocker, err = createDBLock(dirname); err != nil {
			return
		}
	}

	transferTable, err := model.NewTransferTable[*model.TransferHashPage](ctx, opts.LocalFs)
	if err != nil {
		return
	}

	if opts.WalClientFactory != nil {
		db.Wal = wal.NewLogserviceHandle(opts.WalClientFactory)
	} else {
		db.Wal = wal.NewLocalHandle(dirname, WALDir, nil)
	}

	rollbackSteps.Add("rollback open wal", func() error {
		return db.Wal.Close()
	})

	scheduler := newTaskScheduler(
		db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers,
	)
	rollbackSteps.Add("rollback open scheduler", func() error {
		scheduler.Stop()
		return nil
	})

	db.Runtime = dbutils.NewRuntime(
		dbutils.WithRuntimeTransferTable(transferTable),
		dbutils.WithRuntimeObjectFS(opts.Fs),
		dbutils.WithRuntimeLocalFS(opts.LocalFs),
		dbutils.WithRuntimeSmallPool(dbutils.MakeDefaultSmallPool("small-vector-pool")),
		dbutils.WithRuntimeTransientPool(dbutils.MakeDefaultTransientPool("trasient-vector-pool")),
		dbutils.WithRuntimeScheduler(scheduler),
		dbutils.WithRuntimeOptions(db.Opts),
	)

	dataFactory := tables.NewDataFactory(
		db.Runtime, db.Dir,
	)
	if db.Catalog, err = catalog.OpenCatalog(db.usageMemo, dataFactory); err != nil {
		return
	}
	db.usageMemo.C = db.Catalog
	rollbackSteps.Add("rollback open catalog", func() error {
		db.Catalog.Close()
		return nil
	})

	db.Controller = NewController(db)
	if err = db.Controller.AssembleDB(ctx); err != nil {
		return
	}
	db.Controller.Start()

	database, _ := db.Catalog.GetDatabaseByID(272515)
	table1, _ := database.GetTableEntryByID(272516)
	replayObjects(table1, db.Catalog, db.Runtime.Now())
	table2, _ := database.GetTableEntryByID(272518)
	replayObjects2(table2, db.Catalog, db.Runtime.Now())
	// For debug or test
	fmt.Println(db.Catalog.SimplePPString(common.PPL3))
	return
}

func replayObjects(table *catalog.TableEntry, ctlg *catalog.Catalog, now types.TS) {
	//dd4d2138bb4d_0
	replayObject(
		false,
		"AZY4VpgOflKLXd1NITi7TQAAMDE5NjM4NTYtOTgwZS03ZTUyLThiNWQtZGQ0ZDIxMzhiYjRkXzAwMDAwAc8iAAApBAAAvxMAAIgAAAABAAAAZAAAAAAAAAD8WAAAAAAAAAAAAAAAAAAAAAAAAAAACOsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/FzgnAABQ+gAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//8a80b5b99599_0
	replayObject(
		false,
		"AZY4hUkvew60DIqAtbmVmQAAMDE5NjM4ODUtNDkyZi03YjBlLWI0MGMtOGE4MGI1Yjk5NTk5XzAwMDAwASwJAACpAwAAvxMAAAYAAAABAAAAcgAAAAAAAACDAwAAAAAAAAAAAAAAAAAAAAAAAAAACLkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/FxUNAAC7JgAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//23db6ff21c6c_0
	replayObject(
		false,
		"AZY4gKHGffi+ByPbb/IcbAAAMDE5NjM4ODAtYTFjNi03ZGY4LWJlMDctMjNkYjZmZjIxYzZjXzAwMDAwAf4FAABsAwAAvxMAAAEAAAABAAAAhgAAAAAAAACGAAAAAAAAAAAAAAAAAAAAAAAAAAAACIYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/F6oJAABjHQAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//1e4c1b5047de_0
	replayObject(
		false,
		"AZbW/jxld4Kpbx5MG1BH3gAAMDE5NmQ2ZmUtM2M2NS03NzgyLWE5NmYtMWU0YzFiNTA0N2RlXzAwMDAwASMGAABsAwAAvxMAAAEAAAABAAAAAlCt0GCYsBoCUK3QYJiwGgAAAAAAAAAAAAAAAAAACAJQrdBgmLAaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/F88JAADKHQAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//5c85c630eeee_0
	replayObject(
		false,
		"AZbW8vHmfLufJVyFxjDu7gAAMDE5NmQ2ZjItZjFlNi03Y2JiLTlmMjUtNWM4NWM2MzBlZWVlXzAwMDAwAYAHAADhAwAAvxMAAAIAAAABAAAAAlCt0GCYsBoDoBrB8TBhNQAAAAAAAAAAAAAAAAAACAFQbfCQmLAaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/F6ELAACZHwAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//f91d730f2b6b_0
	replayObject(
		false,
		"AZbXApWtffq69vkdcw8rawAAMDE5NmQ3MDItOTVhZC03ZGZhLWJhZjYtZjkxZDczMGYyYjZiXzAwMDAwAZ4JAADyAwAAvxMAAAUAAAABAAAAAlCt0GCYsBoAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAJQ7YpMnbAaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/F9ANAAA0JQAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//bd1cf4ccfca5_0
	replayObject(
		false,
		"AZbXB3etc7OMVr0c9Mz8pQAAMDE5NmQ3MDctNzdhZC03M2IzLThjNTYtYmQxY2Y0Y2NmY2E1XzAwMDAwAQkIAACuAwAAvxMAAAUAAAABAAAAAVCtcGydsBoAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAJQLan/nbAaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/F/cLAAAfJAAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//f410eccc2068_0
	replayObject(
		true,
		"AZbXApcVcIahxfQQ7MwgaAAAMDE5NmQ3MDItOTcxNS03MDg2LWExYzUtZjQxMGVjY2MyMDY4XzAwMDAwAT8DAACWAQAANwUAAAkAAAABAAAAAZY4VpgOflKLXd1NITi7TQAAAAAOAAAAAAAAAAAAGAGW1v48ZXeCqW8eTBtQR94AAAAAAAAAAAAAAAAAABiAZRUFAACbCQAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)

}

func replayObjects2(table *catalog.TableEntry, ctlg *catalog.Catalog, now types.TS) {
	//172e6e4893e8_0
	replayObject(
		false,
		"AZdof+OwdM+pSBcubkiT6AAAMDE5NzY4N2YtZTNiMC03NGNmLWE5NDgtMTcyZTZlNDg5M2U4XzAwMDAwAXE9AAB+BAAApxYAAPUAAAABAAAAAqD/nG0CqhkAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAKwrMKO/NQaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAi/Fy9CAADvXwEAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
	//c786ad9d3330_0
	replayObject(
		true,
		"AZdof+OwdnmDpceGrZ0zMAAAMDE5NzY4N2YtZTNiMC03Njc5LTgzYTUtYzc4NmFkOWQzMzMwXzAwMDAwAVcKAACZAQAANwUAADcAAAABAAAAAZavSgZue3y2vBdV7GvLygAAAACgAAAAAAAAAAAAGAGXaHC73nGMp35SJVIyPWwAAAAAAQAAAAAAAAAAABiAZTAMAAATEgAAAg==",
		now,
		types.TS{},
		table,
		ctlg,
	)
}

func replayObject(
	isTombstone bool,
	statsString string,
	create, delete types.TS,
	table *catalog.TableEntry,
	ctlg *catalog.Catalog,
) {

	decodedData, err := base64.StdEncoding.DecodeString(statsString)
	if err != nil {
		logutil.Infof(err.Error())
	}

	stats := objectio.ObjectStats(decodedData)

	createEntry := &catalog.ObjectEntry{
		ObjectNode: catalog.ObjectNode{IsTombstone: isTombstone},
		EntryMVCCNode: catalog.EntryMVCCNode{
			CreatedAt: create,
			DeletedAt: delete,
		},
		CreateNode:  txnbase.NewTxnMVCCNodeWithTS(create),
		ObjectState: catalog.ObjectState_Create_ApplyCommit,
	}
	if !stats.GetAppendable() {
		createEntry.ObjectStats = stats
	} else {
		emptyStats := objectio.NewObjectStatsWithObjectID(
			stats.ObjectName().ObjectId(), stats.GetAppendable(), stats.GetSorted(), stats.GetCNCreated(),
		)
		createEntry.ObjectStats = *emptyStats
	}
	createEntry.SetTable(table)
	createEntry.SetObjectData(ctlg.MakeObjectFactory()(createEntry))
	table.AddEntryLocked(createEntry)
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
