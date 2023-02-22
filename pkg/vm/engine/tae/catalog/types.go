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

package catalog

import (
	"fmt"
	"hash/fnv"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

func (es EntryState) Repr() string {
	switch es {
	case ES_Appendable:
		return "A"
	case ES_NotAppendable:
		return "NA"
	case ES_Frozen:
		return "F"
	}
	panic("not supported")
}

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
const (
	SnapshotAttr_SegID           = "segment_id"
	SnapshotAttr_TID             = "table_id"
	SnapshotAttr_DBID            = "db_id"
	SegmentAttr_ID               = "id"
	SegmentAttr_CreateAt         = "create_at"
	SegmentAttr_State            = "state"
	SegmentAttr_Sorted           = "sorted"
	SnapshotAttr_BlockMaxRow     = "block_max_row"
	SnapshotAttr_SegmentMaxBlock = "segment_max_block"
)

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *Schema) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), false))
	bat.AddVector(AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), false))
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	nullables := schema.AllNullables()
	for i, attr := range attrs {
		if attr == PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], nullables[i]))
	}
	return bat
}

var (
	BlkMetaSchema        *Schema
	DelBlockSchema       *Schema
	DelTableSchema       *Schema
	SegmentSchema        *Schema
	DelSchema            *Schema
	SystemTableSchemaLog *Schema

	SegmentAttr = []string{
		SegmentAttr_State,
		SegmentAttr_Sorted,
	}
	SegmentTypes = []types.Type{
		types.New(types.T_bool, 0, 0, 0),
		types.New(types.T_bool, 0, 0, 0),
	}

	SystemTableLogAttr = []string{
		SnapshotAttr_BlockMaxRow,
		SnapshotAttr_SegmentMaxBlock,
	}
	SystemTableLogTypes = []types.Type{
		types.New(types.T_uint32, 0, 0, 0),
		types.New(types.T_uint16, 0, 0, 0),
	}
)

func init() {
	BlkMetaSchema = NewEmptySchema("blkMeta")
	DelBlockSchema = NewEmptySchema("deleteBlock")
	DelTableSchema = NewEmptySchema("deleteTable")
	SegmentSchema = NewEmptySchema("segment")
	DelSchema = NewEmptySchema("del")
	SystemTableSchemaLog = SystemTableSchema.Clone()

	for i, colname := range pkgcatalog.MoTableMetaSchema {
		if i == 0 {
			if err := BlkMetaSchema.AppendPKCol(colname, pkgcatalog.MoTableMetaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkMetaSchema.AppendCol(colname, pkgcatalog.MoTableMetaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := BlkMetaSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	if err := DelBlockSchema.AppendCol(pkgcatalog.BlockMeta_SegmentID, types.New(types.T_uint64, 0, 0, 0)); err != nil {
		panic(err)
	}
	if err := DelBlockSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	if err := DelTableSchema.AppendCol(pkgcatalog.SystemRelAttr_DBID, types.New(types.T_uint64, 0, 0, 0)); err != nil {
		panic(err)
	}
	if err := DelTableSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	for i, colname := range SegmentAttr {
		if i == 0 {
			if err := SegmentSchema.AppendPKCol(colname, SegmentTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegmentSchema.AppendCol(colname, SegmentTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := SegmentSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	for i, colname := range SystemTableLogAttr {
		if err := SystemTableSchemaLog.AppendCol(colname, SystemTableLogTypes[i]); err != nil {
			panic(err)
		}
	}
	if err := SystemTableSchemaLog.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}
}

func u64ToRowID(v uint64) types.Rowid {
	var rowid types.Rowid
	bs := types.EncodeUint64(&v)
	copy(rowid[0:], bs)
	return rowid
}

func containersBatchToProtoBatch(bat *containers.Batch) (*api.Batch, error) {
	mobat := containers.CopyToMoBatch(bat)
	return batch.BatchToProtoBatch(mobat)
}

func protoBatchToContainersBatch(bat *api.Batch) (*containers.Batch, error) {
	mobat, err := batch.ProtoBatchToBatch(bat)
	if err != nil {
		panic(err)
	}
	containerBatch := containers.NewBatch()
	for i, vec := range mobat.Vecs {
		containerBatch.AddVector(mobat.Attrs[i], containers.NewVectorWithSharedMemory(vec, false))
	}
	return containerBatch, nil
}

func isDeleteBatch(bat *containers.Batch) bool {
	return len(bat.Attrs) == 2
}

func isDeleteTableBatch(bat *containers.Batch) bool {
	return len(bat.Attrs) == 3
}
func FillColumnRow(table *TableEntry, attr string, colData containers.Vector) {
	schema := table.GetSchema()
	tableID := table.GetID()
	for i, colDef := range table.GetSchema().ColDefs {
		switch attr {
		case pkgcatalog.SystemColAttr_UniqName:
			colData.Append([]byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)))
		case pkgcatalog.SystemColAttr_AccID:
			colData.Append(schema.AcInfo.TenantID)
		case pkgcatalog.SystemColAttr_Name:
			colData.Append([]byte(colDef.Name))
		case pkgcatalog.SystemColAttr_Num:
			colData.Append(int32(i + 1))
		case pkgcatalog.SystemColAttr_Type:
			//colData.Append(int32(colDef.Type.Oid))
			data, _ := types.Encode(colDef.Type)
			colData.Append(data)
		case pkgcatalog.SystemColAttr_DBID:
			colData.Append(table.GetDB().GetID())
		case pkgcatalog.SystemColAttr_DBName:
			colData.Append([]byte(table.GetDB().GetName()))
		case pkgcatalog.SystemColAttr_RelID:
			colData.Append(tableID)
		case pkgcatalog.SystemColAttr_RelName:
			colData.Append([]byte(table.GetSchema().Name))
		case pkgcatalog.SystemColAttr_ConstraintType:
			if colDef.Primary {
				colData.Append([]byte(pkgcatalog.SystemColPKConstraint))
			} else {
				colData.Append([]byte(pkgcatalog.SystemColNoConstraint))
			}
		case pkgcatalog.SystemColAttr_Length:
			colData.Append(int32(colDef.Type.Width))
		case pkgcatalog.SystemColAttr_NullAbility:
			colData.Append(bool2i8(!colDef.NullAbility))
		case pkgcatalog.SystemColAttr_HasExpr:
			colData.Append(bool2i8(len(colDef.Default) > 0)) // @imlinjunhong says always has Default, expect row_id
		case pkgcatalog.SystemColAttr_DefaultExpr:
			colData.Append(colDef.Default)
		case pkgcatalog.SystemColAttr_IsDropped:
			colData.Append(int8(0))
		case pkgcatalog.SystemColAttr_IsHidden:
			colData.Append(bool2i8(colDef.Hidden))
		case pkgcatalog.SystemColAttr_IsUnsigned:
			v := int8(0)
			switch colDef.Type.Oid {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				v = int8(1)
			}
			colData.Append(v)
		case pkgcatalog.SystemColAttr_IsAutoIncrement:
			colData.Append(bool2i8(colDef.AutoIncrement))
		case pkgcatalog.SystemColAttr_Comment:
			colData.Append([]byte(colDef.Comment))
		case pkgcatalog.SystemColAttr_HasUpdate:
			colData.Append(bool2i8(len(colDef.OnUpdate) > 0))
		case pkgcatalog.SystemColAttr_IsClusterBy:
			colData.Append(bool2i8(colDef.IsClusterBy()))
		case pkgcatalog.SystemColAttr_Update:
			colData.Append(colDef.OnUpdate)
		default:
			panic("unexpected colname. if add new catalog def, fill it in this switch")
		}
	}
}

func bool2i8(v bool) int8 {
	if v {
		return int8(1)
	} else {
		return int8(0)
	}
}

func bytesToRowID(bs []byte) types.Rowid {
	var rowid types.Rowid
	if size := len(bs); size <= types.RowidSize {
		copy(rowid[:size], bs[:size])
	} else {
		hasher := fnv.New128()
		hasher.Write(bs)
		hasher.Sum(rowid[:0])
	}
	return rowid
}

func catalogEntry2Batch[T *DBEntry | *TableEntry](
	dstBatch *containers.Batch,
	e T,
	schema *Schema,
	fillDataRow func(e T, attr string, col containers.Vector, ts types.TS),
	rowid types.Rowid,
	commitTs types.TS,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, col.Name, dstBatch.GetVectorByName(col.Name), commitTs)
	}
	dstBatch.GetVectorByName(AttrRowID).Append(rowid)
	dstBatch.GetVectorByName(AttrCommitTs).Append(commitTs)
}

func FillTableRow(table *TableEntry, attr string, colData containers.Vector, ts types.TS) {
	schema := table.GetSchema()
	switch attr {
	case pkgcatalog.SystemRelAttr_ID:
		colData.Append(table.GetID())
	case pkgcatalog.SystemRelAttr_Name:
		colData.Append([]byte(schema.Name))
	case pkgcatalog.SystemRelAttr_DBName:
		colData.Append([]byte(table.GetDB().GetName()))
	case pkgcatalog.SystemRelAttr_DBID:
		colData.Append(table.GetDB().GetID())
	case pkgcatalog.SystemRelAttr_Comment:
		colData.Append([]byte(table.GetSchema().Comment))
	case pkgcatalog.SystemRelAttr_Partition:
		colData.Append([]byte(table.GetSchema().Partition))
	case pkgcatalog.SystemRelAttr_Persistence:
		colData.Append([]byte(pkgcatalog.SystemPersistRel))
	case pkgcatalog.SystemRelAttr_Kind:
		colData.Append([]byte(table.GetSchema().Relkind))
	case pkgcatalog.SystemRelAttr_CreateSQL:
		colData.Append([]byte(table.GetSchema().Createsql))
	case pkgcatalog.SystemRelAttr_ViewDef:
		colData.Append([]byte(schema.View))
	case pkgcatalog.SystemRelAttr_Owner:
		colData.Append(schema.AcInfo.RoleID)
	case pkgcatalog.SystemRelAttr_Creator:
		colData.Append(schema.AcInfo.UserID)
	case pkgcatalog.SystemRelAttr_CreateAt:
		colData.Append(schema.AcInfo.CreateAt)
	case pkgcatalog.SystemRelAttr_AccID:
		colData.Append(schema.AcInfo.TenantID)
	case pkgcatalog.SystemRelAttr_Constraint:
		table.RLock()
		defer table.RUnlock()
		if node := table.MVCCChain.GetVisibleNode(ts); node != nil {
			colData.Append([]byte(node.(*TableMVCCNode).SchemaConstraints))
		} else {
			colData.Append([]byte(""))
		}
	case SnapshotAttr_BlockMaxRow:
		colData.Append(table.schema.BlockMaxRows)
	case SnapshotAttr_SegmentMaxBlock:
		colData.Append(table.schema.SegmentMaxBlocks)
	default:
		panic("unexpected colname. if add new catalog def, fill it in this switch")
	}
}
