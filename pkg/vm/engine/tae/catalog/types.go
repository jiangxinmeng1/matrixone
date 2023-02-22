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
	BlkMetaSchema  *Schema
	DelBlockSchema *Schema
	SegmentSchema  *Schema
	DelSchema      *Schema

	SegmentAttr = []string{
		SegmentAttr_State,
		SegmentAttr_Sorted,
	}
	SegmentTypes = []types.Type{
		types.New(types.T_bool, 0, 0, 0),
		types.New(types.T_bool, 0, 0, 0),
	}
)

func init() {
	BlkMetaSchema = NewEmptySchema("blkMeta")
	DelBlockSchema = NewEmptySchema("deleteBlock")
	SegmentSchema = NewEmptySchema("segment")
	DelSchema = NewEmptySchema("del")

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

func isDeleteBatch(bat *containers.Batch)bool{
	return len(bat.Attrs)==2
}