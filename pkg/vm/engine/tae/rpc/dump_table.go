// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	mosystem "github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/spf13/cobra"

	"go.uber.org/zap"
)

const (
	DumpTableFSName     = "live-dump"
	DumpTableObjectList = "object_list"
	DumpTableSchema     = "schema"
	DumpTableTable      = "table"
)

const (
	ObjectListAttr_ObjectType  = "object_type"
	ObjectListAttr_ID          = "id"
	ObjectListAttr_CreateTS    = "create_ts"
	ObjectListAttr_DeleteTS    = "delete_ts"
	ObjectListAttr_IsPersisted = "is_persisted"
)

const (
	ObjectListAttr_ObjectType_Idx = iota
	ObjectListAttr_ID_Idx
	ObjectListAttr_CreateTS_Idx
	ObjectListAttr_DeleteTS_Idx
	ObjectListAttr_IsPersisted_Idx
)

var ObjectListAttrs = []string{
	ObjectListAttr_ObjectType,
	ObjectListAttr_ID,
	ObjectListAttr_CreateTS,
	ObjectListAttr_DeleteTS,
	ObjectListAttr_IsPersisted,
}

var ObjectListTypes = []types.Type{
	types.T_int8.ToType(),
	types.T_varchar.ToType(),
	types.T_TS.ToType(),
	types.T_TS.ToType(),
	types.T_bool.ToType(),
}
var ObjectListSeqnums = []uint16{0, 1, 2, 3, 4}

func NewObjectListBatch() *batch.Batch {
	return batch.NewWithSchema(false, ObjectListAttrs, ObjectListTypes)
}

type DumpTableArg struct {
	ctx             context.Context
	txn             txnif.AsyncTxn
	table           *catalog.TableEntry
	dir             string
	snapshotTSStr   string
	inspectContext  *inspectContext
	objectListBatch *batch.Batch
	tombstones      []*catalog.ObjectEntry
	workers         int
	mp              *mpool.MPool
	srcfs, dstfs    fileservice.FileService
}

type dumpObjectListEntry struct {
	objectType  int8
	stats       objectio.ObjectStats
	createTS    types.TS
	deleteTS    types.TS
	isPersisted bool
}

// for UT
func NewDumpTableArg(
	ctx context.Context,
	table *catalog.TableEntry,
	dir string,
	inspectContext *inspectContext,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *DumpTableArg {
	return &DumpTableArg{
		ctx:             ctx,
		table:           table,
		dir:             dir,
		inspectContext:  inspectContext,
		objectListBatch: NewObjectListBatch(),
		mp:              mp,
		dstfs:           fs,
		srcfs:           fs,
	}
}
func (c *DumpTableArg) PrepareCommand() *cobra.Command {
	dumpTableCmd := &cobra.Command{
		Use:   "dump-table",
		Short: "Dump table",
		Run:   RunFactory(c),
	}
	dumpTableCmd.SetUsageTemplate(c.Usage())

	dumpTableCmd.Flags().IntP("tid", "t", 0, "set table id")
	dumpTableCmd.Flags().IntP("did", "d", 0, "set database id")
	dumpTableCmd.Flags().StringP("dir", "o", "", "set dump directory")
	dumpTableCmd.Flags().StringP("snapshot-ts", "s", "", "snapshot timestamp for consistent cross-table dump")
	dumpTableCmd.Flags().IntP("workers", "w", 0, "object pipeline workers; 0 chooses a fixed worker count from CPU and memory")
	return dumpTableCmd
}

func (c *DumpTableArg) FromCommand(cmd *cobra.Command) (err error) {
	tid, _ := cmd.Flags().GetInt("tid")
	did, _ := cmd.Flags().GetInt("did")
	c.dir, _ = cmd.Flags().GetString("dir")
	c.snapshotTSStr, _ = cmd.Flags().GetString("snapshot-ts")
	c.workers, _ = cmd.Flags().GetInt("workers")
	c.workers = dumpTableWorkerCount(c.workers)
	if cmd.Flag("ictx") != nil {
		if c.dir == "" {
			return moerr.NewInternalErrorNoCtx("dump directory is required")
		}
		c.inspectContext = cmd.Flag("ictx").Value.(*inspectContext)
		c.mp = common.DefaultAllocator
		if c.dstfs, err = fileservice.NewLocalETLFS(DumpTableFSName, c.dir); err != nil {
			return
		}
		c.dir = ""
		c.srcfs = c.inspectContext.db.Opts.Fs
		c.ctx = c.inspectContext.Context()
		database, err := c.inspectContext.db.Catalog.GetDatabaseByID(uint64(did))
		if err != nil {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("get database by id %d failed", did))
			return err
		}
		c.table, err = database.GetTableEntryByID(uint64(tid))
		if err != nil {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("get table by id %d-%d failed", did, tid))
			return err
		}
		c.objectListBatch = NewObjectListBatch()
	} else {
		return moerr.NewInternalErrorNoCtx("inspect context not found")
	}
	return nil
}

func (c *DumpTableArg) String() string {
	return "dump-table"
}

func (c *DumpTableArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v dump table data\n", "dump-table")

	res += "\n"
	res += "Usage:\n"
	res += "inspect table [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect table <command> --help\" for more information about a given command.\n"

	return
}
func (c *DumpTableArg) Run() (err error) {
	if c.workers <= 0 {
		c.workers = dumpTableWorkerCount(0)
	}
	if c.snapshotTSStr != "" {
		snapshotTS := types.StringToTS(c.snapshotTSStr)
		if c.txn, err = c.inspectContext.db.StartTxnWithStartTSAndSnapshotTS(nil, snapshotTS); err != nil {
			return
		}
	} else {
		if c.txn, err = c.inspectContext.db.StartTxn(nil); err != nil {
			return
		}
	}
	defer func() {
		if err == nil {
			err = c.ctx.Err()
		}
		if err != nil {
			if err2 := c.txn.Rollback(c.ctx); err2 != nil {
				logutil.Error("DUMP-TABLE-ROLLBACK-ERROR", zap.Error(err2))
			}
			return
		}
		err = c.txn.Commit(c.ctx)
	}()
	logutil.Info(
		"DUMP-TABLE-START",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
	)
	defer c.objectListBatch.Clean(c.mp)
	if err := c.flushTableSchema(); err != nil {
		return err
	}
	if err := c.flushTableEntry(); err != nil {
		return err
	}

	p := &catalog.LoopProcessor{}
	p.TombstoneFn = c.collectTombstoneObject
	if err = c.table.RecurLoop(p); err != nil {
		return err
	}

	objects, err := c.collectDataObjects()
	if err != nil {
		return err
	}
	if err = c.dumpObjectsParallel(objects); err != nil {
		return err
	}
	if err := c.flush(DumpTableObjectList, c.objectListBatch); err != nil {
		return err
	}
	logutil.Info(
		"DUMP-TABLE-END",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
		zap.Int("object_count", c.objectListBatch.RowCount()),
	)
	return nil
}

func (c *DumpTableArg) flushTableSchema() (err error) {
	bat := containers.NewBatch()
	typs := catalog.SystemColumnSchema.AllTypes()
	attrs := catalog.SystemColumnSchema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	for _, def := range catalog.SystemColumnSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillColumnRow(c.table, c.table.GetLastestSchema(false), def.Name, bat.Vecs[def.Idx])
	}
	cnBatch := containers.ToCNBatch(bat)
	if err := c.flush(DumpTableSchema, cnBatch); err != nil {
		return err
	}
	return nil
}

func (c *DumpTableArg) flushTableEntry() (err error) {
	bat := containers.NewBatch()
	typs := catalog.SystemTableSchema.AllTypes()
	attrs := catalog.SystemTableSchema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	for _, def := range catalog.SystemTableSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillTableRow(c.table, c.table.GetLastestSchema(false), def.Name, bat.Vecs[def.Idx])
	}
	cnBatch := containers.ToCNBatch(bat)
	if err := c.flush(DumpTableTable, cnBatch); err != nil {
		return err
	}
	return
}

func (c *DumpTableArg) collectDataObjects() ([]*catalog.ObjectEntry, error) {
	var objects []*catalog.ObjectEntry
	p := &catalog.LoopProcessor{}
	p.ObjectFn = func(e *catalog.ObjectEntry) error {
		objects = append(objects, e)
		return nil
	}
	if err := c.table.RecurLoop(p); err != nil {
		return nil, err
	}
	return objects, nil
}

func (c *DumpTableArg) dumpObjectsParallel(objects []*catalog.ObjectEntry) error {
	if len(objects) == 0 {
		return nil
	}
	type result struct {
		entry *dumpObjectListEntry
		err   error
	}

	workers := c.workers
	if workers > len(objects) {
		workers = len(objects)
	}
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	jobsCh := make(chan *catalog.ObjectEntry)
	results := make(chan result, workers)
	var busy atomic.Int64
	var queued atomic.Int64
	var objectListWorkers atomic.Int64
	queued.Store(int64(len(objects)))

	done := make(chan struct{})
	var printerWG sync.WaitGroup
	printerWG.Add(1)
	go func() {
		defer printerWG.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if busy.Load() > 0 || objectListWorkers.Load() > 0 {
					logutil.Info(
						"DUMP-TABLE-PIPELINE-STATUS",
						zap.String("dir", c.dir),
						zap.Int64("busy_workers", busy.Load()),
						zap.Int64("queued_objects", queued.Load()),
						zap.Int64("objectlist_workers", objectListWorkers.Load()),
						zap.Int("total_workers", workers),
					)
				}
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txn, err := c.startWorkerTxn()
			if err != nil {
				cancel()
				results <- result{err: err}
				return
			}
			defer func() {
				if ctx.Err() != nil {
					if err2 := txn.Rollback(c.ctx); err2 != nil {
						logutil.Error("DUMP-TABLE-WORKER-ROLLBACK-ERROR", zap.Error(err2))
					}
					return
				}
				if err2 := txn.Commit(c.ctx); err2 != nil {
					logutil.Error("DUMP-TABLE-WORKER-COMMIT-ERROR", zap.Error(err2))
				}
			}()
			for {
				var e *catalog.ObjectEntry
				var ok bool
				select {
				case <-ctx.Done():
					return
				case e, ok = <-jobsCh:
					if !ok {
						return
					}
				}
				queued.Add(-1)
				busy.Add(1)
				entry, err := c.dumpObject(txn, e)
				busy.Add(-1)
				if err == nil && entry != nil {
					objectListWorkers.Add(1)
				}
				results <- result{entry: entry, err: err}
			}
		}()
	}
	go func() {
		defer close(results)
		for _, e := range objects {
			select {
			case <-ctx.Done():
				close(jobsCh)
				wg.Wait()
				return
			case jobsCh <- e:
			}
		}
		close(jobsCh)
		wg.Wait()
	}()

	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
				cancel()
			}
			continue
		}
		if res.entry != nil {
			if firstErr == nil {
				if err := c.appendObjectListEntry(res.entry); err != nil {
					firstErr = err
				}
			}
			objectListWorkers.Add(-1)
		}
	}
	close(done)
	printerWG.Wait()
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

func (c *DumpTableArg) startWorkerTxn() (txnif.AsyncTxn, error) {
	return c.inspectContext.db.StartTxnWithStartTSAndSnapshotTS(nil, c.txn.GetStartTS())
}

func (c *DumpTableArg) dumpObject(txn txnif.AsyncTxn, e *catalog.ObjectEntry) (*dumpObjectListEntry, error) {
	if e.IsTombstone {
		return nil, nil
	}
	startTS := txn.GetStartTS()
	if e.CreatedAt.EQ(&txnif.UncommitTS) || e.DeleteBefore(startTS) {
		return nil, nil
	}
	bat, err := c.visitObjectData(txn, e)
	if err != nil {
		return nil, err
	}
	if bat == nil {
		logutil.Info(
			"DUMP-TABLE-SKIP-EMPTY-OBJECT",
			zap.String("dir", c.dir),
			zap.String("name", objectio.BuildObjectNameWithObjectID(e.ID()).String()),
		)
		return nil, nil
	}
	defer bat.Close()
	if err = c.filterDeletedRows(txn, bat); err != nil {
		return nil, err
	}
	if bat.Length() == 0 {
		logutil.Info(
			"DUMP-TABLE-SKIP-FULLY-DELETED-OBJECT",
			zap.String("dir", c.dir),
			zap.String("name", objectio.BuildObjectNameWithObjectID(e.ID()).String()),
		)
		return nil, nil
	}
	objectListEntry, err := c.prepareObjectListEntry(txn, e)
	if err != nil {
		return nil, err
	}
	cnBatch := containers.ToCNBatch(bat)
	objectName := objectio.BuildObjectNameWithObjectID(e.ID())
	if objectListEntry.isPersisted {
		objectName = e.ObjectStats.ObjectName()
	}
	if err := c.flush(objectName.String(), cnBatch); err != nil {
		return nil, err
	}
	return objectListEntry, nil
}

func (c *DumpTableArg) flush(name string, bat *batch.Batch) (err error) {
	nameWithDir := path.Join(c.dir, name)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterDumpTable, nameWithDir, c.dstfs)
	if err != nil {
		return
	}
	if _, err = writer.Write(bat); err != nil {
		return
	}
	_, err = writer.WriteEnd(c.ctx)
	if err != nil {
		return
	}
	logutil.Info(
		"DUMP-TABLE-FLUSH",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
		zap.String("name", name),
	)
	return
}

func (c *DumpTableArg) prepareObjectListEntry(txn txnif.AsyncTxn, e *catalog.ObjectEntry) (*dumpObjectListEntry, error) {
	startTS := txn.GetStartTS()
	objectType := int8(ckputil.ObjectType_Data)
	var deleteTS types.TS
	if e.DeletedAt.EQ(&txnif.UncommitTS) || e.DeletedAt.LT(&startTS) {
		deleteTS = types.TS{}
	} else {
		deleteTS = e.DeletedAt
	}
	isPersisted, err := c.isObjectPersisted(txn, e)
	if err != nil {
		return nil, err
	}
	stats := e.ObjectStats
	if !isPersisted {
		if err := objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(e.ID())); err != nil {
			return nil, err
		}
	}
	return &dumpObjectListEntry{
		objectType:  objectType,
		stats:       stats,
		createTS:    e.CreatedAt,
		deleteTS:    deleteTS,
		isPersisted: isPersisted,
	}, nil
}

func (c *DumpTableArg) appendObjectListEntry(entry *dumpObjectListEntry) error {
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_ObjectType_Idx], entry.objectType, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendBytes(
		c.objectListBatch.Vecs[ObjectListAttr_ID_Idx], []byte(entry.stats[:]), false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_CreateTS_Idx], entry.createTS, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_DeleteTS_Idx], entry.deleteTS, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_IsPersisted_Idx], entry.isPersisted, false, c.mp,
	); err != nil {
		return err
	}
	c.objectListBatch.SetRowCount(c.objectListBatch.Vecs[0].Length())
	return nil
}

func (c *DumpTableArg) visitObjectData(txn txnif.AsyncTxn, e *catalog.ObjectEntry) (bat *containers.Batch, err error) {
	schema := e.GetTable().GetLastestSchema(false)
	colIdxes := make([]int, 0)
	// user rows, rowID, commitTS
	for i := range schema.ColDefs {
		colIdxes = append(colIdxes, i)
	}
	colIdxes = append(colIdxes, objectio.SEQNUM_ROWID)
	colIdxes = append(colIdxes, objectio.SEQNUM_COMMITTS)
	for blkID := 0; blkID < e.BlockCnt(); blkID++ {
		if err = e.GetObjectData().Scan(c.ctx, &bat, txn, schema, uint16(blkID), colIdxes, c.mp); err != nil {
			return
		}
	}
	return
}

func (c *DumpTableArg) collectTombstoneObject(e *catalog.ObjectEntry) error {
	startTS := c.txn.GetStartTS()
	if !e.IsTombstone || e.CreatedAt.EQ(&txnif.UncommitTS) || e.DeleteBefore(startTS) {
		return nil
	}
	c.tombstones = append(c.tombstones, e)
	return nil
}

func (c *DumpTableArg) filterDeletedRows(txn txnif.AsyncTxn, bat *containers.Batch) error {
	if bat == nil {
		return nil
	}
	if len(c.tombstones) == 0 || bat.Length() == 0 {
		return nil
	}
	rowIDVec := bat.Vecs[len(bat.Vecs)-2]
	deletedByBlock := make(map[types.Blockid]objectio.Bitmap)
	defer func() {
		for _, deletedRows := range deletedByBlock {
			deletedRows.Release()
		}
	}()

	hasDelete := false
	for rowIdx := 0; rowIdx < bat.Length(); rowIdx++ {
		rowID := rowIDVec.Get(rowIdx).(types.Rowid)
		blockID := rowID.CloneBlockID()
		deletedRows, ok := deletedByBlock[blockID]
		if !ok {
			var err error
			if deletedRows, err = c.getDeletedMaskForBlock(txn, &blockID); err != nil {
				return err
			}
			deletedByBlock[blockID] = deletedRows
		}
		if deletedRows.Contains(uint64(rowID.GetRowOffset())) {
			bat.Delete(rowIdx)
			hasDelete = true
		}
	}
	if hasDelete {
		bat.Compact()
	}
	return nil
}

func (c *DumpTableArg) getDeletedMaskForBlock(txn txnif.AsyncTxn, blockID *types.Blockid) (objectio.Bitmap, error) {
	deletedRows := objectio.GetReusableBitmap()
	startTS := txn.GetStartTS()
	for _, tombstone := range c.tombstones {
		stats := tombstone.ObjectStats
		if !stats.ZMIsEmpty() && !stats.SortKeyZoneMap().RowidPrefixEq(blockID[:]) {
			continue
		}
		isPersisted, err := c.isObjectPersisted(txn, tombstone)
		if err != nil {
			deletedRows.Release()
			return deletedRows, err
		}
		if isPersisted {
			used := false
			getTombstone := func() (*objectio.ObjectStats, error) {
				if used {
					return nil, nil
				}
				used = true
				return &stats, nil
			}
			if err := ioutil.GetTombstonesByBlockId(
				c.ctx,
				&startTS,
				blockID,
				getTombstone,
				&deletedRows,
				c.srcfs,
			); err != nil {
				deletedRows.Release()
				return deletedRows, err
			}
			continue
		}
		if err := c.applyAppendableTombstoneForBlock(txn, tombstone, blockID, &deletedRows); err != nil {
			deletedRows.Release()
			return deletedRows, err
		}
	}
	return deletedRows, nil
}

func (c *DumpTableArg) applyAppendableTombstoneForBlock(
	txn txnif.AsyncTxn,
	tombstone *catalog.ObjectEntry,
	blockID *types.Blockid,
	deletedRows *objectio.Bitmap,
) error {
	schema := tombstone.GetTable().GetLastestSchema(true)
	var bat *containers.Batch
	for blkID := 0; blkID < tombstone.BlockCnt(); blkID++ {
		if err := tombstone.GetObjectData().Scan(
			c.ctx,
			&bat,
			txn,
			schema,
			uint16(blkID),
			[]int{objectio.TombstoneAttr_Rowid_SeqNum},
			c.mp,
		); err != nil {
			return err
		}
	}
	if bat == nil {
		return nil
	}
	defer bat.Close()
	if bat.Length() == 0 {
		return nil
	}
	rowIDVec := bat.Vecs[0]
	for rowIdx := 0; rowIdx < bat.Length(); rowIdx++ {
		rowID := rowIDVec.Get(rowIdx).(types.Rowid)
		if rowID.BorrowBlockID().EQ(blockID) {
			deletedRows.Add(uint64(rowID.GetRowOffset()))
		}
	}
	return nil
}

func (c *DumpTableArg) isObjectPersisted(txn txnif.AsyncTxn, e *catalog.ObjectEntry) (bool, error) {
	startTS := txn.GetStartTS()
	var deleteTS types.TS
	if e.DeletedAt.EQ(&txnif.UncommitTS) || e.DeletedAt.LT(&startTS) {
		deleteTS = types.TS{}
	} else {
		deleteTS = e.DeletedAt
	}
	return !e.GetAppendable() || !deleteTS.IsEmpty(), nil
}

func dumpTableWorkerCount(requested int) int {
	if requested > 0 {
		return requested
	}
	cpuWorkers := runtime.GOMAXPROCS(0)
	if cpuWorkers < 1 {
		cpuWorkers = 1
	}
	mem := mosystem.MemoryAvailable()
	if mem == 0 {
		mem = mosystem.MemoryTotal() / 2
	}
	memWorkers := int(mem / (2 << 30))
	if memWorkers < 1 {
		memWorkers = 1
	}
	if cpuWorkers < memWorkers {
		return cpuWorkers
	}
	return memWorkers
}
