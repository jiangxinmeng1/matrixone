// Copyright 2026 Matrix Origin
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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (shared *sharedAppender) PrepareAppend(
	schema *catalog.Schema,
	txn txnif.AsyncTxn,
	rows uint32,
	isMergeCompact bool,
) (appender data.ObjectAppender, node txnif.AppendNode, startRow, allocated uint32, created bool, err error) {
	shared.Lock()
	defer shared.Unlock()

	for allocated == 0 {
		if shared.current == nil || !shared.currentUsable(schema) {
			if err = shared.useLastAppendableOrCreate(txn); err != nil {
				return
			}
		}
		maxRows := shared.table.meta.GetLastestSchemaLocked(shared.isTombstone).Extra.BlockMaxRows
		if shared.nextRow >= maxRows {
			if err = shared.createAObject(txn); err != nil {
				return
			}
			continue
		}

		startRow = shared.nextRow
		allocated = rows
		if left := maxRows - startRow; allocated > left {
			allocated = left
		}
		if allocated == 0 {
			continue
		}

		shared.current.freezelock.Lock()
		if shared.current.IsAppendFrozen() {
			shared.current.freezelock.Unlock()
			shared.current = nil
			shared.nextRow = 0
			allocated = 0
			continue
		}

		pinned := shared.current.PinNode()
		if pinned.IsPersisted() {
			pinned.Unref()
			shared.current.freezelock.Unlock()
			shared.current = nil
			shared.nextRow = 0
			allocated = 0
			continue
		}

		shared.current.Lock()
		node, created = shared.current.appendMVCC.AddAppendNodeLocked(txn, startRow, startRow+allocated)
		if isMergeCompact {
			node.SetIsMergeCompact()
		}
		pinned.MustMNode().EnsureLengthLocked(startRow + allocated)
		shared.current.Unlock()
		pinned.Unref()
		shared.current.freezelock.Unlock()

		shared.nextRow += allocated
		shared.current.Ref()
		appender, err = shared.current.MakeAppender()
		return
	}
	return
}

func (shared *sharedAppender) currentUsable(schema *catalog.Schema) bool {
	if shared.current == nil || shared.current.IsAppendFrozen() || shared.current.meta.Load().HasDropCommitted() {
		return false
	}
	pinned := shared.current.PinNode()
	defer pinned.Unref()
	if pinned.IsPersisted() {
		return false
	}
	if !pinned.MustMNode().writeSchema.IsSameColumns(schema) {
		return false
	}
	return shared.nextRow < shared.table.meta.GetLastestSchemaLocked(shared.isTombstone).Extra.BlockMaxRows
}

func (shared *sharedAppender) useLastAppendableOrCreate(txn txnif.AsyncTxn) error {
	objMeta := shared.table.meta.TryFindLastAppendableObject(shared.isTombstone)
	if objMeta != nil && objMeta.IsInMemory() && !objMeta.HasDropCommitted() {
		obj := objMeta.GetObjectData().(*aobject)
		pinned := obj.PinNode()
		if !pinned.IsPersisted() {
			rows, err := pinned.Rows()
			pinned.Unref()
			if err != nil {
				return err
			}
			shared.current = obj
			shared.nextRow = rows
			return nil
		}
		pinned.Unref()
	}
	return shared.createAObject(txn)
}

func (shared *sharedAppender) createAObject(txn txnif.AsyncTxn) error {
	if txn == nil {
		return moerr.NewInternalErrorNoCtx("missing txn for shared appendable object")
	}
	objEntry := catalog.NewInMemoryObject(shared.table.meta, txn.GetStartTS(), shared.isTombstone)
	objEntry.InitData(shared.table.meta.GetCatalog().DataFactory)
	shared.table.meta.Lock()
	shared.table.meta.AddEntryLocked(objEntry)
	shared.table.meta.Unlock()
	obj := objEntry.GetObjectData().(*aobject)
	shared.current = obj
	shared.nextRow = 0
	if shared.isTombstone {
		shared.table.aTombstone = obj
	} else {
		shared.table.aObj = obj
	}
	return nil
}
