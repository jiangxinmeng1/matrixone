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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	enginepkg "github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

type ApplyTableDataArg struct {
	ctx            context.Context
	dir            string
	inspectContext *inspectContext
	mp             *mpool.MPool
	srcFS, dstFS   fileservice.FileService

	txn     txnif.AsyncTxn
	catalog *catalog.Catalog

	rel    handle.Relation
	schema *catalog.Schema

	tableName    string
	tableID      uint64
	databaseName string
	databaseID   uint64

	autoIncrementCols []incrservice.AutoColumn
	autoIncrementMaxs []uint64

	workers int
}

type applyObjectResult struct {
	index            int
	stats            []byte
	autoIncrementMax []uint64
	err              error
}

type applyObjectJob struct {
	index int
	stats []byte
}

var globalApplyObjectWorkers = struct {
	sync.Mutex
	ch        chan struct{}
	total     atomic.Int64
	queued    atomic.Int64
	active    atomic.Int64
	completed atomic.Int64
}{}

func NewApplyTableDataArg(
	ctx context.Context,
	dir string,
	inspectContext *inspectContext,
	dbName string,
	tableName string,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (*ApplyTableDataArg, error) {
	a := &ApplyTableDataArg{
		ctx:            ctx,
		dir:            dir,
		databaseName:   dbName,
		tableName:      tableName,
		inspectContext: inspectContext,
		mp:             mp,
		srcFS:          fs,
		dstFS:          fs,
	}
	var err error
	if a.txn, err = a.inspectContext.db.StartTxn(nil); err != nil {
		return nil, err
	}
	a.catalog = a.inspectContext.db.Catalog
	return a, nil
}

func (a *ApplyTableDataArg) PrepareCommand() *cobra.Command {
	applyTableDataCmd := &cobra.Command{
		Use:   "apply-table-data",
		Short: "Apply table data",
		Run:   RunFactory(a),
	}
	applyTableDataCmd.SetUsageTemplate(a.Usage())

	applyTableDataCmd.Flags().StringP("tname", "t", "", "set table name")
	applyTableDataCmd.Flags().StringP("dname", "d", "", "set database name")
	applyTableDataCmd.Flags().StringP("dir", "o", "", "set output directory")
	applyTableDataCmd.Flags().IntP("workers", "w", 0, "object apply workers; 0 chooses a fixed worker count from CPU and memory")
	return applyTableDataCmd
}

func (a *ApplyTableDataArg) FromCommand(cmd *cobra.Command) (err error) {
	a.tableName, _ = cmd.Flags().GetString("tname")
	a.databaseName, _ = cmd.Flags().GetString("dname")
	a.dir, _ = cmd.Flags().GetString("dir")
	a.workers, _ = cmd.Flags().GetInt("workers")
	a.workers = dumpTableWorkerCount(a.workers)
	if cmd.Flag("ictx") != nil {
		if a.dir == "" {
			return moerr.NewInternalErrorNoCtx("dump directory is required")
		}
		a.inspectContext = cmd.Flag("ictx").Value.(*inspectContext)
		a.mp = common.DefaultAllocator
		if a.srcFS, err = fileservice.NewLocalETLFS(DumpTableFSName, a.dir); err != nil {
			return err
		}
		a.dir = ""
		a.dstFS = a.inspectContext.db.Opts.Fs
		a.ctx = a.inspectContext.Context()
		if a.txn, err = a.inspectContext.db.StartTxn(nil); err != nil {
			return err
		}
		a.catalog = a.inspectContext.db.Catalog
	} else {
		return moerr.NewInternalErrorNoCtx("inspect context not found")
	}
	return nil
}

func (a *ApplyTableDataArg) String() string {
	return "apply-table-data"
}

func (a *ApplyTableDataArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v apply table data\n", "apply-table-data")

	res += "\n"
	res += "Usage:\n"
	res += "inspect table [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect table <command> --help\" for more information about a given command.\n"

	return
}
func (a *ApplyTableDataArg) Run() (err error) {
	if a.workers <= 0 {
		a.workers = dumpTableWorkerCount(0)
	}
	logutil.Info(
		"APPLY-TABLE-DATA-START",
		zap.String("dir", a.dir),
		zap.String("start ts", a.txn.GetStartTS().ToString()),
		zap.Int("workers", a.workers),
	)
	defer func() {
		if err == nil {
			err = a.ctx.Err()
		}
		if err != nil {
			err2 := a.txn.Rollback(a.ctx)
			if err2 != nil {
				logutil.Error("APPLY-TABLE-DATA-ROLLBACK-ERROR", zap.Error(err2))
			}
		} else {
			err = a.txn.Commit(a.ctx)
		}
		logutil.Info(
			"APPLY-TABLE-DATA-END",
			zap.String("dir", a.dir),
			zap.String(
				"table",
				fmt.Sprintf(
					"%d-%v, %d-%s",
					a.databaseID,
					a.databaseName,
					a.tableID,
					a.tableName,
				),
			),
			zap.String("end ts", a.txn.GetCommitTS().ToString()),
			zap.Any("error", err),
		)
	}()
	if err = a.createDatabase(); err != nil {
		return
	}
	if err = a.createTable(); err != nil {
		return
	}
	if a.isView() {
		return nil
	}

	objectlistBatch, release, err := a.readBatch(DumpTableObjectList, ObjectListAttrs)
	if err != nil {
		return
	}
	defer release()
	defer objectlistBatch.Clean(a.mp)
	objTypes := vector.MustFixedColNoTypeCheck[int8](objectlistBatch.Vecs[ObjectListAttr_ObjectType_Idx])
	idVec := objectlistBatch.Vecs[ObjectListAttr_ID_Idx]
	dataObjects := make([][]byte, 0, objectlistBatch.RowCount())
	for i := 0; i < objectlistBatch.RowCount(); i++ {
		if objTypes[i] == ckputil.ObjectType_Data {
			dataObjects = append(dataObjects, append([]byte(nil), idVec.GetBytesAt(i)...))
		} else if objTypes[i] == ckputil.ObjectType_Tombstone {
			return moerr.NewInternalErrorNoCtx("invalid dump table object list: tombstone object is not supported")
		} else {
			panic(fmt.Sprintf("invalid object type: %d", objTypes[i]))
		}
	}

	statsVec := containers.MakeVector(types.T_varchar.ToType(), a.mp)
	defer statsVec.Close()
	if err = a.applyDataObjectsParallel(dataObjects, statsVec); err != nil {
		return
	}
	if statsVec.Length() > 0 {
		if err = a.rel.AddDataFiles(a.ctx, statsVec); err != nil {
			return
		}
	}
	err = a.createAutoIncrementMetadata()
	if err != nil {
		return
	}
	return

}

func (a *ApplyTableDataArg) isView() bool {
	return a.schema != nil && a.schema.Relkind == pkgcatalog.SystemViewRel
}

func (a *ApplyTableDataArg) applyDataObjectsParallel(
	objects [][]byte,
	statsVec containers.Vector,
) error {
	if len(objects) == 0 {
		return nil
	}
	workers := a.workers
	if workers > len(objects) {
		workers = len(objects)
	}
	ctx, cancel := context.WithCancel(a.ctx)
	defer cancel()
	limiter := getGlobalApplyObjectLimiter(a.workers)
	jobsCh := make(chan applyObjectJob)
	results := make(chan applyObjectResult, workers)
	var active atomic.Int64
	var queued atomic.Int64
	var completed atomic.Int64
	queued.Store(int64(len(objects)))
	globalApplyObjectWorkers.total.Add(int64(len(objects)))
	globalApplyObjectWorkers.queued.Add(int64(len(objects)))
	defer func() {
		globalApplyObjectWorkers.total.Add(-int64(len(objects)))
		globalApplyObjectWorkers.queued.Add(-queued.Load())
		globalApplyObjectWorkers.completed.Add(-completed.Load())
	}()

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
				if queued.Load() > 0 || active.Load() > 0 {
					logutil.Info(
						"APPLY-TABLE-DATA-PIPELINE-STATUS",
						zap.String("dir", a.dir),
						zap.String("table", fmt.Sprintf("%d-%s", a.tableID, a.tableName)),
						zap.Int("total_objects", len(objects)),
						zap.Int64("queued_objects", queued.Load()),
						zap.Int64("active_objects", active.Load()),
						zap.Int64("active_objects_global", globalApplyObjectWorkers.active.Load()),
						zap.Int64("total_objects_global", globalApplyObjectWorkers.total.Load()),
						zap.Int64("queued_objects_global", globalApplyObjectWorkers.queued.Load()),
						zap.Int64("completed_objects_global", globalApplyObjectWorkers.completed.Load()),
						zap.Int("object_worker_limit", cap(limiter)),
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
			for {
				var job applyObjectJob
				var ok bool
				select {
				case <-ctx.Done():
					return
				case job, ok = <-jobsCh:
					if !ok {
						return
					}
				}
				select {
				case limiter <- struct{}{}:
				case <-ctx.Done():
					return
				}
				active.Add(1)
				globalApplyObjectWorkers.active.Add(1)
				queued.Add(-1)
				globalApplyObjectWorkers.queued.Add(-1)
				res := func() applyObjectResult {
					defer func() {
						active.Add(-1)
						globalApplyObjectWorkers.active.Add(-1)
						<-limiter
					}()
					return a.applyDataObject(job.index, job.stats)
				}()
				completed.Add(1)
				globalApplyObjectWorkers.completed.Add(1)
				results <- res
			}
		}()
	}
	go func() {
		defer close(results)
		for i, rawStats := range objects {
			select {
			case <-ctx.Done():
				close(jobsCh)
				wg.Wait()
				return
			case jobsCh <- applyObjectJob{index: i, stats: rawStats}:
			}
		}
		close(jobsCh)
		wg.Wait()
	}()

	var firstErr error
	ordered := make([]applyObjectResult, len(objects))
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
				cancel()
			}
			continue
		}
		ordered[res.index] = res
	}
	for _, res := range ordered {
		if res.err != nil || res.stats == nil {
			continue
		}
		statsVec.Append(res.stats, false)
		a.mergeAutoIncrementMaxs(res.autoIncrementMax)
	}
	close(done)
	printerWG.Wait()
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

func getGlobalApplyObjectLimiter(workers int) chan struct{} {
	if workers <= 0 {
		workers = dumpTableWorkerCount(0)
	}
	globalApplyObjectWorkers.Lock()
	defer globalApplyObjectWorkers.Unlock()
	if globalApplyObjectWorkers.ch == nil ||
		(cap(globalApplyObjectWorkers.ch) != workers &&
			globalApplyObjectWorkers.active.Load() == 0 &&
			len(globalApplyObjectWorkers.ch) == 0) {
		globalApplyObjectWorkers.ch = make(chan struct{}, workers)
	}
	return globalApplyObjectWorkers.ch
}

func (a *ApplyTableDataArg) applyDataObject(
	index int,
	rawStats []byte,
) applyObjectResult {
	sourceStats := objectio.ObjectStats(rawStats)
	sourceName := sourceStats.ObjectName().String()
	schema := a.rel.GetMeta().(*catalog.TableEntry).GetLastestSchema(false)
	attrs := append(append([]string{}, schema.AllNames()...), objectio.PhysicalAddr_Attr, objectio.TombstoneAttr_CommitTs_Attr)
	bats, release, err := a.readBatches(sourceName, nil, attrs)
	if err != nil {
		return applyObjectResult{index: index, err: err}
	}
	defer releaseBatches(release, bats, a.mp)
	if len(bats) == 0 {
		return applyObjectResult{index: index}
	}

	arena := objectio.GetArena(objectio.ArenaSmall)
	defer func() {
		arena.Reset()
		objectio.PutArena(arena)
	}()

	objID := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&objID)
	writer, err := ioutil.NewBlockWriterWithArena(
		a.dstFS,
		name,
		schema.Version,
		schema.AllSeqnums(),
		false,
		arena,
	)
	if err != nil {
		return applyObjectResult{index: index, err: err}
	}
	if schema.HasPK() {
		writer.SetPrimaryKey(uint16(schema.GetSingleSortKeyIdx()))
	} else if schema.HasSortKey() {
		writer.SetSortKey(uint16(schema.GetSingleSortKeyIdx()))
	}
	if schema.HasFakePK() {
		writer.SetFakePK(uint16(schema.GetPrimaryKey().Idx))
	}

	userColCnt := len(schema.AllNames())
	totalRows := 0
	autoIncrementMaxs := make([]uint64, len(a.autoIncrementCols))
	for _, bat := range bats {
		if bat.RowCount() == 0 {
			continue
		}
		a.collectAutoIncrementMax(bat, autoIncrementMaxs)
		writeBat := batch.NewWithSize(userColCnt)
		writeBat.Vecs = bat.Vecs[:userColCnt]
		writeBat.Attrs = attrs[:userColCnt]
		writeBat.SetRowCount(bat.RowCount())
		totalRows += bat.RowCount()
		if _, err = writer.WriteBatch(writeBat); err != nil {
			return applyObjectResult{index: index, err: err}
		}
	}
	if totalRows == 0 {
		logutil.Info(
			"APPLY-TABLE-DATA-SKIP-FULLY-DELETED-OBJECT",
			zap.String("dir", a.dir),
			zap.String("source-name", sourceName),
		)
		return applyObjectResult{index: index}
	}
	if _, _, err = writer.Sync(a.ctx); err != nil {
		return applyObjectResult{index: index, err: err}
	}
	stats := writer.GetObjectStats(objectio.WithCNCreated())
	logutil.Info(
		"APPLY-TABLE-DATA-WRITE-OBJECT",
		zap.String("dir", a.dir),
		zap.String("source-name", sourceName),
		zap.String("target-name", name.String()),
		zap.Int("rows", totalRows),
	)
	return applyObjectResult{
		index:            index,
		stats:            append([]byte(nil), stats[:]...),
		autoIncrementMax: autoIncrementMaxs,
	}
}

func releaseBatches(release func(), bats []*batch.Batch, mp *mpool.MPool) {
	if release != nil {
		release()
	}
	for _, bat := range bats {
		if bat != nil {
			bat.Clean(mp)
		}
	}
}

func (a *ApplyTableDataArg) createDatabase() (err error) {

	var database handle.Database

	if database, err = a.txn.CreateDatabase(a.databaseName, "", ""); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
			database, err = a.txn.GetDatabase(a.databaseName)
			if err != nil {
				return
			}
			a.databaseID = database.GetID()
			return nil
		}
		return
	}

	dbEntry := database.GetMeta().(*catalog.DBEntry)

	bat := containers.NewBatch()
	defer bat.Close()
	typs := catalog.SystemDBSchema.AllTypes()
	attrs := catalog.SystemDBSchema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], a.mp))
	}
	for _, def := range catalog.SystemDBSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillDBRow(dbEntry, def.Name, bat.Vecs[def.Idx])
	}

	a.databaseID = dbEntry.GetID()

	var db handle.Database
	if db, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return
	}
	var table handle.Relation
	if table, err = db.GetRelationByName(pkgcatalog.MO_DATABASE); err != nil {
		return
	}
	if err = table.Append(a.ctx, bat); err != nil {
		return
	}
	return
}

func (a *ApplyTableDataArg) createTable() (err error) {

	var schemaBatch, tableBatch *batch.Batch
	var schemaRelease, tableRelease func()
	if schemaBatch, schemaRelease, err = a.readBatch(DumpTableSchema, catalog.SystemColumnSchema.AllNames()); err != nil {
		return
	}
	defer schemaRelease()
	defer schemaBatch.Clean(a.mp)
	tnSchemaBatch := containers.ToTNBatch(schemaBatch, a.mp)
	if tableBatch, tableRelease, err = a.readBatch(DumpTableTable, catalog.SystemTableSchema.AllNames()); err != nil {
		return
	}
	defer tableRelease()
	defer tableBatch.Clean(a.mp)
	tnTableBatch := containers.ToTNBatch(tableBatch, a.mp)
	originalTableID := vector.MustFixedColNoTypeCheck[uint64](
		tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_ID).GetDownstreamVector(),
	)[0]
	useOriginalTableID := a.tableName == ""

	// If no target table name provided, use the original name from the dump.
	if a.tableName == "" {
		a.tableName = string(tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(0).([]byte))
	}

	var db handle.Database
	if db, err = a.txn.GetDatabase(a.databaseName); err != nil {
		return
	}

	a.schema, err = readSchema(a.tableName, tnSchemaBatch, tnTableBatch)

	if useOriginalTableID {
		a.rel, err = db.CreateRelationWithID(a.schema, originalTableID)
	} else {
		a.rel, err = db.CreateRelation(a.schema)
	}
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("table %q.%q already exists", a.databaseName, a.tableName))
		}
		return
	}

	a.tableID = a.rel.ID()
	a.initAutoIncrementMetadata()

	packer := types.NewPacker()
	tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Update(0, a.tableID, false)
	tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Update(0, []byte(a.tableName), false)
	tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Update(0, a.databaseID, false)
	tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_DBName).Update(0, []byte(a.databaseName), false)
	tenantID := tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(0).(uint32)
	packer.EncodeUint32(tenantID)
	packer.EncodeStringType([]byte(a.databaseName))
	packer.EncodeStringType([]byte(a.tableName))
	colData := packer.Bytes()
	tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_CPKey).Update(0, colData, false)

	uniqNameVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_UniqName)
	dbidVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_DBID)
	dbNameVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_DBName)
	relIDVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_RelID)
	relNameVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_RelName)
	ckpKeyVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_CPKey)
	nameVec := tnSchemaBatch.GetVectorByName(pkgcatalog.SystemColAttr_Name)
	for i := 0; i < tnSchemaBatch.Length(); i++ {
		colName := string(nameVec.Get(i).([]byte))
		uniqNameVec.Update(i, []byte(fmt.Sprintf("%d-%s", a.tableID, colName)), false)
		dbidVec.Update(i, a.databaseID, false)
		dbNameVec.Update(i, []byte(a.databaseName), false)
		relIDVec.Update(i, a.tableID, false)
		relNameVec.Update(i, []byte(a.tableName), false)
		packer.Reset()
		packer.EncodeUint32(tenantID)
		packer.EncodeStringType([]byte(a.databaseName))
		packer.EncodeStringType([]byte(a.tableName))
		packer.EncodeStringType([]byte(colName))
		colData := packer.Bytes()
		ckpKeyVec.Update(i, colData, false)
	}
	packer.Close()

	if db, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return
	}
	var table handle.Relation
	if table, err = db.GetRelationByName(pkgcatalog.MO_COLUMNS); err != nil {
		return
	}
	if err = table.Append(a.ctx, tnSchemaBatch); err != nil {
		return
	}
	if table, err = db.GetRelationByName(pkgcatalog.MO_TABLES); err != nil {
		return
	}
	if err = table.Append(a.ctx, tnTableBatch); err != nil {
		return
	}
	if useOriginalTableID {
		err = a.createForeignKeyMetadata()
	}
	return
}

func (a *ApplyTableDataArg) createForeignKeyMetadata() error {
	if a.schema == nil || len(a.schema.Constraint) == 0 {
		return nil
	}
	constraints := new(enginepkg.ConstraintDef)
	if err := constraints.UnmarshalBinary(a.schema.Constraint); err != nil {
		return err
	}

	var fkeys []*enginepkg.ForeignKeyDef
	for _, ct := range constraints.Cts {
		if fkDef, ok := ct.(*enginepkg.ForeignKeyDef); ok {
			fkeys = append(fkeys, fkDef)
		}
	}
	if len(fkeys) == 0 {
		return nil
	}

	var moCatalog handle.Database
	var err error
	if moCatalog, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return err
	}
	var fkTable handle.Relation
	if fkTable, err = moCatalog.GetRelationByName(pkgcatalog.MOForeignKeys); err != nil {
		return err
	}

	tableEntry, ok := fkTable.GetMeta().(*catalog.TableEntry)
	if !ok {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid %s metadata", pkgcatalog.MOForeignKeys))
	}
	fkSchema := tableEntry.GetLastestSchema(false)
	bat := containers.NewBatch()
	defer bat.Close()
	for _, def := range fkSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		bat.AddVector(def.Name, containers.MakeVector(def.Type, a.mp))
	}

	for _, fkDef := range fkeys {
		for _, fk := range fkDef.Fkeys {
			if err = a.appendForeignKeyRows(bat, fk); err != nil {
				return err
			}
		}
	}
	if bat.Length() == 0 {
		return nil
	}
	return fkTable.Append(a.ctx, bat)
}

func (a *ApplyTableDataArg) appendForeignKeyRows(bat *containers.Batch, fk *plan.ForeignKeyDef) error {
	if fk == nil || len(fk.Cols) == 0 {
		return nil
	}
	parentSchema := a.schema
	parentName := a.tableName
	if fk.ForeignTbl != 0 && fk.ForeignTbl != a.tableID {
		db, err := a.txn.GetDatabase(a.databaseName)
		if err != nil {
			return err
		}
		parentRel, err := db.GetRelationByID(fk.ForeignTbl)
		if err != nil {
			return err
		}
		parentEntry, ok := parentRel.GetMeta().(*catalog.TableEntry)
		if !ok {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid parent table %d metadata", fk.ForeignTbl))
		}
		parentSchema = parentEntry.GetLastestSchema(false)
		parentName = parentSchema.Name
	}
	for i, childColID := range fk.Cols {
		if i >= len(fk.ForeignCols) {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid foreign key %q column mapping", fk.Name))
		}
		childColName, ok := schemaColumnNameByID(a.schema, childColID)
		if !ok {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("foreign key %q child column %d not found", fk.Name, childColID))
		}
		parentColName, ok := schemaColumnNameByID(parentSchema, fk.ForeignCols[i])
		if !ok {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("foreign key %q parent column %d not found", fk.Name, fk.ForeignCols[i]))
		}
		appendForeignKeyValue(bat, "constraint_name", []byte(fk.Name))
		appendForeignKeyValue(bat, "constraint_id", uint64(0))
		appendForeignKeyValue(bat, "db_name", []byte(a.databaseName))
		appendForeignKeyValue(bat, "db_id", uint64(0))
		appendForeignKeyValue(bat, "table_name", []byte(a.tableName))
		appendForeignKeyValue(bat, "table_id", uint64(0))
		appendForeignKeyValue(bat, "column_name", []byte(childColName))
		appendForeignKeyValue(bat, "column_id", childColID)
		appendForeignKeyValue(bat, "refer_db_name", []byte(a.databaseName))
		appendForeignKeyValue(bat, "refer_db_id", uint64(0))
		appendForeignKeyValue(bat, "refer_table_name", []byte(parentName))
		appendForeignKeyValue(bat, "refer_table_id", uint64(0))
		appendForeignKeyValue(bat, "refer_column_name", []byte(parentColName))
		appendForeignKeyValue(bat, "refer_column_id", fk.ForeignCols[i])
		appendForeignKeyValue(bat, "on_delete", []byte(fk.OnDelete.String()))
		appendForeignKeyValue(bat, "on_update", []byte(fk.OnUpdate.String()))
	}
	return nil
}

func appendForeignKeyValue(bat *containers.Batch, name string, v any) {
	bat.GetVectorByName(name).Append(v, false)
}

func schemaColumnNameByID(schema *catalog.Schema, id uint64) (string, bool) {
	if schema == nil || id > uint64(len(schema.ColDefs)) {
		return "", false
	}
	idx := int(id)
	if idx < 0 || idx >= len(schema.ColDefs) {
		return "", false
	}
	return schema.ColDefs[idx].Name, true
}

func (a *ApplyTableDataArg) initAutoIncrementMetadata() {
	a.autoIncrementCols = a.autoIncrementCols[:0]
	a.autoIncrementMaxs = a.autoIncrementMaxs[:0]
	for _, def := range a.schema.ColDefs {
		if !def.AutoIncrement || def.Hidden || def.FakePK || def.PhyAddr {
			continue
		}
		a.autoIncrementCols = append(a.autoIncrementCols, incrservice.AutoColumn{
			TableID:  a.tableID,
			ColName:  def.Name,
			ColIndex: def.Idx,
			Offset:   0,
			Step:     1,
		})
		a.autoIncrementMaxs = append(a.autoIncrementMaxs, 0)
	}
}

func (a *ApplyTableDataArg) collectAutoIncrementMax(bat *batch.Batch, maxs []uint64) {
	for i, col := range a.autoIncrementCols {
		if i >= len(maxs) {
			continue
		}
		if col.ColIndex < 0 || col.ColIndex >= len(bat.Vecs) {
			continue
		}
		vec := bat.Vecs[col.ColIndex]
		for row := 0; row < bat.RowCount(); row++ {
			if vec.IsNull(uint64(row)) {
				continue
			}
			if v, ok := autoIncrementValueToUint64(vec, row); ok && v > maxs[i] {
				maxs[i] = v
			}
		}
	}
}

func (a *ApplyTableDataArg) mergeAutoIncrementMaxs(maxs []uint64) {
	for i, v := range maxs {
		if i < len(a.autoIncrementMaxs) && v > a.autoIncrementMaxs[i] {
			a.autoIncrementMaxs[i] = v
		}
	}
}

func autoIncrementValueToUint64(vec *vector.Vector, row int) (uint64, bool) {
	switch vec.GetType().Oid {
	case types.T_int8:
		return signedAutoIncrementValueToUint64(int64(vector.GetFixedAtNoTypeCheck[int8](vec, row)))
	case types.T_int16:
		return signedAutoIncrementValueToUint64(int64(vector.GetFixedAtNoTypeCheck[int16](vec, row)))
	case types.T_int32:
		return signedAutoIncrementValueToUint64(int64(vector.GetFixedAtNoTypeCheck[int32](vec, row)))
	case types.T_int64:
		return signedAutoIncrementValueToUint64(vector.GetFixedAtNoTypeCheck[int64](vec, row))
	case types.T_uint8:
		return uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, row)), true
	case types.T_uint16:
		return uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, row)), true
	case types.T_uint32:
		return uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, row)), true
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, row), true
	default:
		return 0, false
	}
}

func signedAutoIncrementValueToUint64(v int64) (uint64, bool) {
	if v <= 0 {
		return 0, false
	}
	return uint64(v), true
}

func (a *ApplyTableDataArg) createAutoIncrementMetadata() (err error) {
	if len(a.autoIncrementCols) == 0 {
		return nil
	}

	var db handle.Database
	if db, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return
	}
	var table handle.Relation
	if table, err = db.GetRelationByName(pkgcatalog.MOAutoIncrTable); err != nil {
		return
	}
	tableEntry, ok := table.GetMeta().(*catalog.TableEntry)
	if !ok {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid %s metadata", pkgcatalog.MOAutoIncrTable))
	}
	schema := tableEntry.GetLastestSchema(false)
	bat := containers.NewBatch()
	defer bat.Close()

	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		bat.AddVector(def.Name, containers.MakeVector(def.Type, a.mp))
	}

	packer := types.NewPacker()
	defer packer.Close()
	for i, col := range a.autoIncrementCols {
		col.Offset = a.autoIncrementMaxs[i]
		for _, def := range schema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			vec := bat.GetVectorByName(def.Name)
			switch def.Name {
			case "table_id":
				vec.Append(col.TableID, false)
			case "col_name":
				vec.Append([]byte(col.ColName), false)
			case "col_index":
				vec.Append(int32(col.ColIndex), false)
			case "offset":
				vec.Append(col.Offset, false)
			case "step":
				vec.Append(col.Step, false)
			case pkgcatalog.CPrimaryKeyColName:
				packer.Reset()
				packer.EncodeUint64(col.TableID)
				packer.EncodeStringType([]byte(col.ColName))
				vec.Append(packer.Bytes(), false)
			default:
				return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected %s column %q", pkgcatalog.MOAutoIncrTable, def.Name))
			}
		}
	}
	return table.Append(a.ctx, bat)
}

func (a *ApplyTableDataArg) readBatch(name string, attrs []string) (bat *batch.Batch, release func(), err error) {
	var bats []*batch.Batch
	if bats, release, err = a.readBatches(name, nil, attrs); err != nil {
		return
	}
	_, injected := objectio.GCDumpTableInjected()
	if len(bats) != 1 || injected {
		releaseBatches(release, bats, a.mp)
		return nil, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid object list batch, %d", len(bats)))
	}
	bat = bats[0]
	return
}

func (a *ApplyTableDataArg) readBatches(name string, idxs []uint16, attrs []string) (bats []*batch.Batch, release func(), err error) {
	logutil.Info(
		"APPLY-TABLE-DATA-READ-BATCH",
		zap.String("dir", a.dir),
		zap.String("name", name),
	)
	fname := path.Join(a.dir, name)
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewFileReader(
		a.srcFS,
		fname,
	); err != nil {
		return
	}
	if bats, release, err = reader.LoadAllColumns(
		a.ctx, idxs, a.mp,
	); err != nil {
		return
	}
	for _, bat := range bats {
		bat.Attrs = attrs
	}
	return
}

func readSchema(
	name string,
	colBat *containers.Batch,
	tblBat *containers.Batch,
) (*catalog.Schema, error) {

	if tblBat.Length() != 1 {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid table batch, %d", tblBat.Length()))
	}
	versions := vector.MustFixedColNoTypeCheck[uint32](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Version).GetDownstreamVector())
	catalogVersions := vector.MustFixedColNoTypeCheck[uint32](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).GetDownstreamVector())
	partitioneds := vector.MustFixedColNoTypeCheck[int8](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).GetDownstreamVector())
	roleIDs := vector.MustFixedColNoTypeCheck[uint32](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).GetDownstreamVector())
	userIDs := vector.MustFixedColNoTypeCheck[uint32](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).GetDownstreamVector())
	createAts := vector.MustFixedColNoTypeCheck[types.Timestamp](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).GetDownstreamVector())
	tenantIDs := vector.MustFixedColNoTypeCheck[uint32](tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).GetDownstreamVector())

	colTids := vector.MustFixedColNoTypeCheck[uint64](colBat.GetVectorByName(pkgcatalog.SystemColAttr_RelID).GetDownstreamVector())
	nullables := vector.MustFixedColNoTypeCheck[int8](colBat.GetVectorByName(pkgcatalog.SystemColAttr_NullAbility).GetDownstreamVector())
	isHiddens := vector.MustFixedColNoTypeCheck[int8](colBat.GetVectorByName(pkgcatalog.SystemColAttr_IsHidden).GetDownstreamVector())
	clusterbys := vector.MustFixedColNoTypeCheck[int8](colBat.GetVectorByName(pkgcatalog.SystemColAttr_IsClusterBy).GetDownstreamVector())
	autoIncrements := vector.MustFixedColNoTypeCheck[int8](colBat.GetVectorByName(pkgcatalog.SystemColAttr_IsAutoIncrement).GetDownstreamVector())
	idxes := vector.MustFixedColNoTypeCheck[int32](colBat.GetVectorByName(pkgcatalog.SystemColAttr_Num).GetDownstreamVector())
	seqNums := vector.MustFixedColNoTypeCheck[uint16](colBat.GetVectorByName(pkgcatalog.SystemColAttr_Seqnum).GetDownstreamVector())

	schema := catalog.NewEmptySchema(name)
	schema.ReadFromBatch(
		colBat, colTids, nullables, isHiddens, clusterbys, autoIncrements, idxes, seqNums, 0,
		func(currentName string, currentTid uint64) (goNext bool) {
			return true
		},
	)
	schema.Comment = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).GetDownstreamVector().GetStringAt(0)
	schema.Version = versions[0]
	schema.CatalogVersion = catalogVersions[0]
	schema.Partitioned = partitioneds[0]
	schema.Partition = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).GetDownstreamVector().GetStringAt(0)
	schema.Relkind = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).GetDownstreamVector().GetStringAt(0)
	schema.Createsql = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).GetDownstreamVector().GetStringAt(0)
	schema.View = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).GetDownstreamVector().GetStringAt(0)
	schema.Constraint = tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).GetDownstreamVector().CloneBytesAt(0)
	schema.AcInfo.RoleID = roleIDs[0]
	schema.AcInfo.UserID = userIDs[0]
	schema.AcInfo.CreateAt = createAts[0]
	schema.AcInfo.TenantID = tenantIDs[0]
	// unmarshal before releasing, no need to copy
	extra := tblBat.GetVectorByName(pkgcatalog.SystemRelAttr_ExtraInfo).GetDownstreamVector().GetBytesAt(0)
	schema.MustRestoreExtra(extra)
	if err := schema.Finalize(true); err != nil {
		return nil, err
	}
	return schema, nil
}
