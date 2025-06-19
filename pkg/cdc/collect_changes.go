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

package cdc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func CollectChanges_2(
	ctx context.Context,
	rel engine.Relation,
	fromTs types.TS,
	toTs types.TS,
	sinker Sinker,
	initSnapshotSplitTxn bool,
	packer *types.Packer,
	mp *mpool.MPool,
) {
	changes, err := CollectChanges(ctx, rel, fromTs, toTs, mp)
	if err != nil {
		return
	}
	defer changes.Close()
	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch
	var hasBegin bool

	tableDef := rel.CopyTableDef(ctx)
	insTSColIdx := len(tableDef.Cols) - 1
	insCompositedPkColIdx := len(tableDef.Cols) - 2
	delTSColIdx := 1
	delCompositedPkColIdx := 0
	if len(tableDef.Pkey.Names) == 1 {
		insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}

	defer func() {
		if insertData != nil {
			insertData.Clean(mp)
		}
		if deleteData != nil {
			deleteData.Clean(mp)
		}
		if insertAtmBatch != nil {
			insertAtmBatch.Close()
		}
		if deleteAtmBatch != nil {
			deleteAtmBatch.Close()
		}
	}()

	allocateAtomicBatchIfNeed := func(atomicBatch *AtomicBatch) *AtomicBatch {
		if atomicBatch == nil {
			atomicBatch = NewAtomicBatch(mp)
		}
		return atomicBatch
	}

	var currentHint engine.ChangesHandle_Hint
	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}
		// check sinker error of last round
		if err = sinker.Error(); err != nil {
			return
		}

		insertData, deleteData, currentHint, err = changes.Next(ctx, mp)
		if err != nil {
			return
		}

		// both nil denote no more data (end of this tail)
		if insertData == nil && deleteData == nil {
			// heartbeat, send remaining data in sinker
			sinker.Sink(ctx, &DecoderOutput{
				noMoreData: true,
				fromTs:     fromTs,
				toTs:       toTs,
			})

			// send a dummy to guarantee last piece of snapshot/tail send successfully
			sinker.SendDummy()
			if err = sinker.Error(); err == nil {
				if hasBegin {
					// error may not be caught immediately
					sinker.SendCommit()
					// so send a dummy sql to guarantee previous commit is sent successfully
					sinker.SendDummy()
					err = sinker.Error()
				}
			}
			return
		}

		switch currentHint {
		case engine.ChangesHandle_Snapshot:
			// output sql in a txn
			if !hasBegin && !initSnapshotSplitTxn {
				sinker.SendBegin()
				hasBegin = true
			}

			// transform into insert instantly
			sinker.Sink(ctx, &DecoderOutput{
				outputTyp:     OutputTypeSnapshot,
				checkpointBat: insertData,
				fromTs:        fromTs,
				toTs:          toTs,
			})
			insertData.Clean(mp)
		case engine.ChangesHandle_Tail_wip:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)
		case engine.ChangesHandle_Tail_done:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)

			//if !strings.Contains(reader.info.SourceTblName, "order") {
			//	logutil.Errorf("tableReader(%s)[%s, %s], insertAtmBatch: %s, deleteAtmBatch: %s",
			//		reader.info.SourceTblName, fromTs.ToString(), toTs.ToString(),
			//		insertAtmBatch.DebugString(reader.tableDef, false),
			//		deleteAtmBatch.DebugString(reader.tableDef, true))
			//}

			// output sql in a txn
			if !hasBegin {
				sinker.SendBegin()
				hasBegin = true
			}

			sinker.Sink(ctx, &DecoderOutput{
				outputTyp:      OutputTypeTail,
				insertAtmBatch: insertAtmBatch,
				deleteAtmBatch: deleteAtmBatch,
				fromTs:         fromTs,
				toTs:           toTs,
			})
			addTailEndMetrics(insertAtmBatch)
			addTailEndMetrics(deleteAtmBatch)
			insertAtmBatch.Close()
			deleteAtmBatch.Close()
			// reset, allocate new when next wip/done
			insertAtmBatch = nil
			deleteAtmBatch = nil
		}
	}
}
