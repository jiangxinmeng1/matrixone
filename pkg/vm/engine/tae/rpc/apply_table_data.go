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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
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
}

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
	return applyTableDataCmd
}

func (a *ApplyTableDataArg) FromCommand(cmd *cobra.Command) (err error) {
	a.tableName, _ = cmd.Flags().GetString("tname")
	a.databaseName, _ = cmd.Flags().GetString("dname")
	a.dir, _ = cmd.Flags().GetString("dir")
	if cmd.Flag("ictx") != nil {
		a.inspectContext = cmd.Flag("ictx").Value.(*inspectContext)
		a.mp = common.DefaultAllocator
		if a.srcFS, err = a.inspectContext.db.Opts.TmpFs.GetOrCreateApp(
			&fileservice.AppConfig{
				Name: DumpTableDir,
				GCFn: GCDumpTableFiles,
			},
		); err != nil {
			return err
		}
		a.dstFS = a.inspectContext.db.Opts.Fs
		a.ctx = context.Background()
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
	logutil.Info(
		"APPLY-TABLE-DATA-START",
		zap.String("dir", a.dir),
		zap.String("start ts", a.txn.GetStartTS().ToString()),
	)
	defer func() {
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

	objectlistBatch, release, err := a.readBatch(DumpTableObjectList, ObjectListAttrs)
	if err != nil {
		return
	}
	defer release()
	defer objectlistBatch.Clean(a.mp)
	objTypes := vector.MustFixedColNoTypeCheck[int8](objectlistBatch.Vecs[ObjectListAttr_ObjectType_Idx])
	idVec := objectlistBatch.Vecs[ObjectListAttr_ID_Idx]

	statsVec := containers.MakeVector(types.T_varchar.ToType(), a.mp)
	defer statsVec.Close()
	for i := 0; i < objectlistBatch.RowCount(); i++ {
		if objTypes[i] == ckputil.ObjectType_Data {
			if err = a.applyDataObject(idVec.GetBytesAt(i), statsVec); err != nil {
				return
			}
		} else if objTypes[i] == ckputil.ObjectType_Tombstone {
			return moerr.NewInternalErrorNoCtx("invalid dump table object list: tombstone object is not supported")
		} else {
			panic(fmt.Sprintf("invalid object type: %d", objTypes[i]))
		}
	}
	if statsVec.Length() > 0 {
		err = a.rel.AddDataFiles(a.ctx, statsVec)
	}
	return

}

func (a *ApplyTableDataArg) applyDataObject(
	rawStats []byte,
	statsVec containers.Vector,
) (err error) {
	sourceStats := objectio.ObjectStats(rawStats)
	sourceName := sourceStats.ObjectName().String()
	schema := a.rel.GetMeta().(*catalog.TableEntry).GetLastestSchema(false)
	attrs := append(append([]string{}, schema.AllNames()...), objectio.PhysicalAddr_Attr, objectio.TombstoneAttr_CommitTs_Attr)
	bats, release, err := a.readBatches(sourceName, nil, attrs)
	if err != nil {
		return err
	}
	defer releaseBatches(release, bats, a.mp)
	if len(bats) == 0 {
		return nil
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
		return err
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
	for _, bat := range bats {
		if bat.RowCount() == 0 {
			continue
		}
		writeBat := batch.NewWithSize(userColCnt)
		writeBat.Vecs = bat.Vecs[:userColCnt]
		writeBat.Attrs = attrs[:userColCnt]
		writeBat.SetRowCount(bat.RowCount())
		totalRows += bat.RowCount()
		if _, err = writer.WriteBatch(writeBat); err != nil {
			return err
		}
	}
	if totalRows == 0 {
		logutil.Info(
			"APPLY-TABLE-DATA-SKIP-FULLY-DELETED-OBJECT",
			zap.String("dir", a.dir),
			zap.String("source-name", sourceName),
		)
		return nil
	}
	if _, _, err = writer.Sync(a.ctx); err != nil {
		return err
	}
	stats := writer.GetObjectStats(objectio.WithCNCreated())
	statsVec.Append(stats[:], false)
	logutil.Info(
		"APPLY-TABLE-DATA-WRITE-OBJECT",
		zap.String("dir", a.dir),
		zap.String("source-name", sourceName),
		zap.String("target-name", name.String()),
		zap.Int("rows", totalRows),
	)
	return nil
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
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("database %q already exists", a.databaseName))
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

	var db handle.Database
	if db, err = a.txn.GetDatabase(a.databaseName); err != nil {
		return
	}

	a.schema, err = readSchema(a.tableName, tnSchemaBatch, tnTableBatch)

	if a.rel, err = db.CreateRelation(a.schema); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("table %q.%q already exists", a.databaseName, a.tableName))
		}
		return
	}

	a.tableID = a.rel.ID()

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
	return
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
