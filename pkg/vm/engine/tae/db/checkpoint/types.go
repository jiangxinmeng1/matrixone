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

package checkpoint

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var ErrPendingCheckpoint = moerr.NewPrevCheckpointNotFinished()
var ErrCheckpointDisabled = moerr.NewInternalErrorNoCtxf("checkpoint disabled")
var ErrExecutorRestarted = moerr.NewInternalErrorNoCtxf("executor restarted")
var ErrExecutorClosed = moerr.NewInternalErrorNoCtxf("executor closed")
var ErrBadIntent = moerr.NewInternalErrorNoCtxf("bad intent")
var ErrStopRunner = moerr.NewInternalErrorNoCtxf("runner stopped")

type State int8

const (
	ST_Running State = iota
	ST_Pending
	ST_Finished
)

type EntryType int8

const (
	ET_Global EntryType = iota
	ET_Incremental
	ET_Backup
	ET_Compacted
)

func (e EntryType) String() string {
	switch e {
	case ET_Global:
		return "G"
	case ET_Incremental:
		return "I"
	case ET_Backup:
		return "B"
	case ET_Compacted:
		return "C"
	}
	return "Unknown"
}

type CheckpointScheduler interface {
	TryScheduleCheckpoint(types.TS, bool) (Intent, error)
	RunnerReader
}

type ReplayClient interface {
	AddCheckpointMetaFile(string)
	ReplayCKPEntry(*CheckpointEntry) error
}

type Runner interface {
	ReplayClient
	CheckpointScheduler
	TestRunner
	RunnerWriter
	RunnerReader

	Start()
	Stop()

	BuildReplayer(string) *CkpReplayer
	GCByTS(ctx context.Context, ts types.TS) error
}

type Observer interface {
	OnNewCheckpoint(ts types.TS)
}

type observers struct {
	os []Observer
}

func (os *observers) add(o Observer) {
	os.os = append(os.os, o)
}

func (os *observers) OnNewCheckpoint(ts types.TS) {
	for _, o := range os.os {
		o.OnNewCheckpoint(ts)
	}
}

const (
	CheckpointAttr_StartTS       = "start_ts"
	CheckpointAttr_EndTS         = "end_ts"
	CheckpointAttr_MetaLocation  = "meta_location"
	CheckpointAttr_EntryType     = "entry_type"
	CheckpointAttr_Version       = "version"
	CheckpointAttr_AllLocations  = "all_locations"
	CheckpointAttr_CheckpointLSN = "checkpoint_lsn"
	CheckpointAttr_TruncateLSN   = "truncate_lsn"
	CheckpointAttr_Type          = "type"

	CheckpointAttr_StartTSIdx       = 0
	CheckpointAttr_EndTSIdx         = 1
	CheckpointAttr_MetaLocationIdx  = 2
	CheckpointAttr_EntryTypeIdx     = 3
	CheckpointAttr_VersionIdx       = 4
	CheckpointAttr_AllLocationsIdx  = 5
	CheckpointAttr_CheckpointLSNIdx = 6
	CheckpointAttr_TruncateLSNIdx   = 7
	CheckpointAttr_TypeIdx          = 8

	CheckpointSchemaColumnCountV1 = 5 // start, end, loc, type, ver
	CheckpointSchemaColumnCountV2 = 9
)

var (
	CheckpointSchema *catalog.Schema
)

var (
	CheckpointSchemaAttr = []string{
		CheckpointAttr_StartTS,
		CheckpointAttr_EndTS,
		CheckpointAttr_MetaLocation,
		CheckpointAttr_EntryType,
		CheckpointAttr_Version,
		CheckpointAttr_AllLocations,
		CheckpointAttr_CheckpointLSN,
		CheckpointAttr_TruncateLSN,
		CheckpointAttr_Type,
	}
	CheckpointSchemaTypes = []types.Type{
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_bool, 0, 0), // true for incremental
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_int8, 0, 0),
	}
)

func init() {
	var err error
	CheckpointSchema = catalog.NewEmptySchema("checkpoint")
	for i, colname := range CheckpointSchemaAttr {
		if err = CheckpointSchema.AppendCol(colname, CheckpointSchemaTypes[i]); err != nil {
			panic(err)
		}
	}
}

func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}
