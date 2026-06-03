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

package tables

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

func init() {
	// Register factory function to create table-level singleton
	catalog.SetAppenderFactory(func(table *catalog.TableEntry, rt *dbutils.Runtime, isTombstone bool) catalog.AppenderFactory {
		return NewSharedAppender(table, rt, isTombstone)
	})
}

type SharedAppender interface {
	// PrepareAppend allocates space, creates AppendNodes, and generates RowIDs.
	// Returns the list of created AppendNodes for tableSpace to register to txnEntries.
	PrepareAppend(node txnif.AppendableNode) ([]txnif.TxnEntry, error)

	// ApplyAppend writes data.
	ApplyAppend() error

	// Close releases resources.
	Close()

	// Append is a simplified interface for testing (internally calls PrepareAppend + ApplyAppend).
	Append(node txnif.AppendableNode) error

	// GetCurrentAobj returns the currently used aobj (for setting tableHandle).
	GetCurrentAobj() *aobject

	// Test interfaces
	GetRefedAobjs() []*aobject
}

type appendContext struct {
	objEntry *catalog.ObjectEntry
	aobj     *aobject
	srcStart uint32
	srcCount uint32
	destRow  uint32
}

// sharedAppender is table-level singleton, only manages space allocation
type sharedAppender struct {
	table       *catalog.TableEntry
	rt          *dbutils.Runtime
	isTombstone bool
	mu          sync.Mutex

	currentEntry *catalog.ObjectEntry
	currentAobj  *aobject
	nextRow      uint32
	maxRows      uint32
}

// txnAppender is per-txn, holds per-txn state
type txnAppender struct {
	shared *sharedAppender
	txn    txnif.AsyncTxn

	refedAobjs         []*aobject
	createdAppendNodes []txnif.TxnEntry
	preparedNodes      []txnif.AppendableNode
	preparedContexts   [][]*appendContext
	objectCallback     catalog.TxnObjectCallback
}

// NewSharedAppender creates table-level singleton
func NewSharedAppender(
	table *catalog.TableEntry,
	rt *dbutils.Runtime,
	isTombstone bool,
) *sharedAppender {
	return &sharedAppender{
		table:       table,
		rt:          rt,
		isTombstone: isTombstone,
	}
}

// GetTxnAppender creates per-txn appender
func (app *sharedAppender) GetTxnAppender(txn txnif.AsyncTxn) catalog.TxnAppender {
	txnApp := &txnAppender{
		shared:           app,
		txn:              txn,
		refedAobjs:       make([]*aobject, 0),
		preparedNodes:    make([]txnif.AppendableNode, 0),
		preparedContexts: make([][]*appendContext, 0),
	}
	return txnApp
}

// Append is a simple one-step interface for testing
func (txnApp *txnAppender) Append(node txnif.AppendableNode) error {
	_, err := txnApp.PrepareAppend(node)
	if err != nil {
		return err
	}
	return txnApp.ApplyAppend()
}

// PrepareAppend allocates space, creates AppendNodes, and generates RowIDs
// Returns the list of created AppendNodes for tableSpace to register to txnEntries
func (txnApp *txnAppender) PrepareAppend(node txnif.AppendableNode) ([]txnif.TxnEntry, error) {
	if node == nil {
		return nil, nil
	}

	data := node.GetData()
	if data == nil {
		return nil, moerr.NewInternalErrorNoCtx("node data is nil")
	}

	totalRows := node.Rows()
	if totalRows == 0 {
		return nil, nil
	}

	// Accumulate node (support multiple PrepareAppend calls)
	txnApp.preparedNodes = append(txnApp.preparedNodes, node)

	// Create contexts for this node
	contexts := make([]*appendContext, 0)
	createdAppendNodes := make([]txnif.TxnEntry, 0)

	// Use node's phyAddrIdx from writeSchema (at creation time)
	phyAddrIdx := node.GetPhyAddrIdx()

	// Handle PhyAddr column - unified for both data and tombstone
	// Create new vector for PhyAddr
	phyAddrVec := txnApp.shared.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer func() {
		data.Vecs[phyAddrIdx].Close()
		data.Vecs[phyAddrIdx] = phyAddrVec
	}()

	remaining := totalRows
	srcOffset := uint32(0)

	for remaining > 0 {
		objEntry, aobj, appendNode, startRow, allocated, err := txnApp.allocateSpace(remaining)
		if err != nil {
			return nil, err
		}

		if allocated == 0 {
			// Should not happen
			return nil, moerr.NewInternalErrorNoCtx("failed to allocate space")
		}

		// Add AppendNode to the list
		if appendNode != nil {
			createdAppendNodes = append(createdAppendNodes, appendNode)
		}

		// Generate RowIDs
		if err := txnApp.generatePhyAddr(phyAddrVec, objEntry, allocated, startRow); err != nil {
			return nil, err
		}

		// Save context for ApplyAppend
		contexts = append(contexts, &appendContext{
			objEntry: objEntry,
			aobj:     aobj,
			srcStart: srcOffset,
			srcCount: allocated,
			destRow:  startRow,
		})

		// Register object to txn (warChecker, GetMemo)
		txnApp.registerObjectToTxn(objEntry)
		txnApp.onObjectAllocated(objEntry)

		// Notify node about the mapping
		node.AddApplyInfo(srcOffset, allocated, startRow, allocated, objEntry.AsCommonID())

		srcOffset += allocated
		remaining -= allocated
	}

	// Accumulate contexts for this node
	txnApp.preparedContexts = append(txnApp.preparedContexts, contexts)

	// Accumulate created AppendNodes
	txnApp.createdAppendNodes = append(txnApp.createdAppendNodes, createdAppendNodes...)

	return createdAppendNodes, nil
}

// ApplyAppend writes the actual data to aobjects
func (txnApp *txnAppender) ApplyAppend() error {
	if len(txnApp.preparedNodes) == 0 {
		return nil
	}

	// Iterate over all prepared nodes
	for i, node := range txnApp.preparedNodes {
		data := node.GetData()
		if data == nil {
			return moerr.NewInternalErrorNoCtx("prepared node data is nil")
		}

		contexts := txnApp.preparedContexts[i]

		// Write data for each context
		for _, ctx := range contexts {
			if err := txnApp.writeDataToAobj(data, ctx); err != nil {
				return err
			}
		}
	}

	// Clear prepared state
	txnApp.preparedNodes = nil
	txnApp.preparedContexts = nil

	return nil
}

func (txnApp *txnAppender) writeDataToAobj(data *containers.Batch, ctx *appendContext) error {
	bat := data.Window(int(ctx.srcStart), int(ctx.srcCount))
	defer bat.Close()

	ctx.aobj.Lock()
	defer ctx.aobj.Unlock()

	n := ctx.aobj.PinNode()
	defer n.Unref()

	if !n.IsPersisted() {
		mnode := n.MustMNode()
		if err := mnode.OverwriteAtLocked(bat, ctx.destRow); err != nil {
			return err
		}

		schema := mnode.writeSchema
		for _, colDef := range schema.ColDefs {
			if colDef.IsPhyAddr() {
				continue
			}
			if colDef.IsRealPrimary() && !schema.IsSecondaryIndexTable() {
				if err := mnode.pkIndex.BatchUpsert(
					bat.GetVectorByName(colDef.Name).GetDownstreamVector(), int(ctx.destRow)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return moerr.NewInternalErrorNoCtx("cannot append to persisted node")
}

func (txnApp *txnAppender) Close() {
	for _, aobj := range txnApp.refedAobjs {
		aobj.Unref()
	}
	txnApp.refedAobjs = nil
	txnApp.preparedNodes = nil
	txnApp.preparedContexts = nil
	txnApp.objectCallback = nil
}

func (txnApp *txnAppender) GetCurrentAobj() *aobject {
	return txnApp.shared.currentAobj
}

func (txnApp *txnAppender) SetObjectCallback(fn catalog.TxnObjectCallback) {
	txnApp.objectCallback = fn
}

// allocateSpace ensures aobj and allocates space (with locking)
func (txnApp *txnAppender) allocateSpace(count uint32) (*catalog.ObjectEntry, *aobject, txnif.TxnEntry, uint32, uint32, error) {
	shared := txnApp.shared
	shared.mu.Lock()
	defer shared.mu.Unlock()

	// Try current aobj first
	if shared.currentAobj != nil && shared.nextRow < shared.maxRows {
		// Ref aobj first to prevent concurrent flush/compact
		shared.currentAobj.Ref()

		// Check if frozen
		if shared.currentAobj.IsAppendFrozen() {
			shared.currentAobj.Unref()
			shared.currentAobj = nil
			shared.currentEntry = nil
			// Fall through to create new aobj
		} else {
			// Pin node to prevent ReleaseAppends during our operation
			node := shared.currentAobj.PinNode()
			if node.IsPersisted() {
				// Node already persisted, appends released
				node.Unref()
				shared.currentAobj.Unref()
				shared.currentAobj = nil
				shared.currentEntry = nil
				// Fall through to create new aobj
			} else {
				available := shared.maxRows - shared.nextRow
				if available > 0 {
					allocated := count
					if allocated > available {
						allocated = available
					}
					startRow := shared.nextRow
					shared.nextRow += allocated

					// Create AppendNode (safe: we hold node ref, appends won't be released)
					shared.currentAobj.Lock()
					node.MustMNode().EnsureLength(startRow + allocated)
					appendNode, _ := shared.currentAobj.appendMVCC.AddAppendNodeLocked(
						txnApp.txn, startRow, startRow+allocated)
					shared.currentAobj.Unlock()

					// Release node ref (no longer needed after AddAppendNodeLocked)
					node.Unref()

					// Track aobj ref for this txn
					found := false
					for _, a := range txnApp.refedAobjs {
						if a == shared.currentAobj {
							found = true
							break
						}
					}
					if found {
						// Already tracked, release the extra ref
						shared.currentAobj.Unref()
					} else {
						// First time, keep the ref
						txnApp.refedAobjs = append(txnApp.refedAobjs, shared.currentAobj)
					}

					return shared.currentEntry, shared.currentAobj, appendNode, startRow, allocated, nil
				}
				// No space, release refs
				node.Unref()
				shared.currentAobj.Unref()
			}
		}
	}

	// Create new aobj
	objEntry := catalog.NewInMemoryObject(shared.table, txnApp.txn.GetStartTS(), shared.isTombstone)

	dataFactory := shared.table.GetDB().GetCatalog().DataFactory
	if dataFactory == nil {
		return nil, nil, nil, 0, 0, moerr.NewInternalErrorNoCtx("DataFactory is nil")
	}
	objEntry.InitData(dataFactory) // Initialize objData BEFORE adding to catalog

	shared.table.Lock()
	shared.table.AddEntryLocked(objEntry)
	shared.table.Unlock()

	aobj := objEntry.GetObjectData().(*aobject)

	// Ensure appendMVCC is initialized
	if aobj == nil {
		return nil, nil, nil, 0, 0, moerr.NewInternalErrorNoCtx("GetObjectData returned nil")
	}
	if aobj.appendMVCC == nil {
		// Manually initialize appendMVCC if DataFactory didn't do it
		aobj.appendMVCC = updates.NewAppendMVCCHandle(objEntry)
		aobj.appendMVCC.SetAppendListener(aobj.OnApplyAppend)
		aobj.RWMutex = aobj.appendMVCC.RWMutex
	}

	aobj.Ref()
	txnApp.refedAobjs = append(txnApp.refedAobjs, aobj)

	shared.currentEntry = objEntry
	shared.currentAobj = aobj
	shared.nextRow = 0
	shared.maxRows = shared.table.GetLastestSchema(shared.isTombstone).Extra.BlockMaxRows

	// Allocate from new aobj
	allocated := count
	if allocated > shared.maxRows {
		allocated = shared.maxRows
	}
	startRow := uint32(0)
	shared.nextRow = allocated

	// Create AppendNode for new aobj
	aobj.Lock()
	node := aobj.PinNode()
	node.MustMNode().EnsureLength(allocated)
	node.Unref()
	appendNode, _ := aobj.appendMVCC.AddAppendNodeLocked(
		txnApp.txn, startRow, startRow+allocated)
	aobj.Unlock()

	return objEntry, aobj, appendNode, startRow, allocated, nil
}

// allocateRows allocates space in aobj (must be called with shared.mu locked)
func (txnApp *txnAppender) generatePhyAddr(
	phyAddrVec containers.Vector,
	objEntry *catalog.ObjectEntry,
	count, destOffset uint32,
) error {
	// Generate rowid for both data and tombstone
	blkID := objectio.NewBlockidWithObjectID(objEntry.ID(), 0)
	col := txnApp.shared.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer col.Close()

	// Construct rowids to temporary col
	if err := objectio.ConstructRowidColumnTo(
		col.GetDownstreamVector(),
		&blkID,
		destOffset,
		count,
		col.GetAllocator(),
	); err != nil {
		return err
	}

	// Extend to phyAddrVec (accumulate all rowids)
	return phyAddrVec.ExtendVec(col.GetDownstreamVector())
}

func (txnApp *txnAppender) registerObjectToTxn(objEntry *catalog.ObjectEntry) {
	id := objEntry.AsCommonID()
	txnApp.txn.GetMemo().AddObject(
		txnApp.shared.table.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		txnApp.shared.isTombstone,
	)
}

func (txnApp *txnAppender) onObjectAllocated(objEntry *catalog.ObjectEntry) {
	if txnApp.objectCallback == nil || objEntry == nil {
		return
	}
	txnApp.objectCallback(objEntry)
}

func (txnApp *txnAppender) GetRefedAobjs() []*aobject {
	return txnApp.refedAobjs
}
