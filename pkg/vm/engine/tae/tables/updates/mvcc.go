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

package updates

import (
	"context"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func init() {
	txnNodeSize := int(unsafe.Sizeof(txnbase.TxnMVCCNode{}))
	catalog.AppendNodeApproxSize = int(unsafe.Sizeof(AppendNode{})) + txnNodeSize
}

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), types.TS{})
	return txn
}

func MockTxnWithStartTS(ts types.TS) *txnbase.Txn {
	txn := mockTxn()
	txn.StartTS = ts
	return txn
}

type AppendMVCCHandle struct {
	*sync.RWMutex
	meta           *catalog.ObjectEntry
	appends        *txnbase.MVCCSlice[*AppendNode]
	appendListener func(txnif.AppendNode) error
}

func NewAppendMVCCHandle(meta *catalog.ObjectEntry) *AppendMVCCHandle {
	node := &AppendMVCCHandle{
		RWMutex: &sync.RWMutex{},
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
	return node
}

// ==========================================================
// *************** All appends related APIs *****************
// ==========================================================

// NOTE: after this call all appends related APIs should not be called
// ReleaseAppends release all append nodes.
// it is only called when the appendable block is persisted and the
// memory node is released
func (n *AppendMVCCHandle) ReleaseAppends() {
	n.Lock()
	defer n.Unlock()
	n.appends = nil
}

// only for internal usage
// given a row, it returns the append node which contains the row
func (n *AppendMVCCHandle) GetAppendNodeByRowLocked(row uint32) (an *AppendNode) {
	_, an = n.appends.SearchNodeByCompareFn(func(node *AppendNode) int {
		if node.maxRow <= row {
			return -1
		}
		if node.startRow > row {
			return 1
		}
		return 0
	})
	return
}

// GetRowSelectionByTSLocked returns rows prepared no later than ts. Physical
// row order and prepare timestamp order are independent.
func (n *AppendMVCCHandle) GetRowSelectionByTSLocked(ts types.TS) index.RowSelection {
	selection := n.GetRowSelectionInRangeLocked(types.TS{}, ts)
	selection.MakePrefix()
	return selection
}

// GetRowSelectionInRangeLocked returns rows whose prepare timestamps are in
// [start, end]. Holes represent physical rows between selected append nodes.
func (n *AppendMVCCHandle) GetRowSelectionInRangeLocked(
	start, end types.TS,
) (selection index.RowSelection) {
	n.appends.ForEach(func(node *AppendNode) bool {
		if in, _ := node.PreparedIn(start, end); in {
			selection.AddRange(node.startRow, node.maxRow)
		}
		return true
	}, true)
	return
}

// GetRowSelectionAfterLocked returns rows whose prepare timestamps are in
// (start, end]. It is used by incremental dedup, whose lower bound was already
// checked by the preceding timestamp window.
func (n *AppendMVCCHandle) GetRowSelectionAfterLocked(
	start, end types.TS,
) (selection index.RowSelection) {
	n.appends.ForEach(func(node *AppendNode) bool {
		prepare := node.GetPrepare()
		if prepare.GT(&start) && prepare.LE(&end) {
			selection.AddRange(node.startRow, node.maxRow)
		}
		return true
	}, true)
	return
}

// it collects all append nodes in the range [start, end]
// selection: is the physical row selection, including holes between nodes
// commitTSVec: is the commit ts vector
// abortVec: is the abort vector
// The vectors contain selected rows only and follow physical row order.
func (n *AppendMVCCHandle) CollectAppendLocked(
	start, end types.TS, mp *mpool.MPool,
) (
	selection index.RowSelection,
	commitTSVec, abortVec containers.Vector,
) {
	for {
		txns := make([]txnif.TxnReader, 0)
		n.appends.ForEach(func(node *AppendNode) bool {
			if in, _ := node.PreparedIn(start, end); in && node.GetTxn() != nil {
				txns = append(txns, node.GetTxn())
			}
			return true
		}, true)
		if len(txns) == 0 {
			break
		}
		n.RUnlock()
		for _, txn := range txns {
			txn.GetTxnState(true)
		}
		n.RLock()
	}

	selection = n.GetRowSelectionInRangeLocked(start, end)
	if selection.IsEmpty() {
		return
	}
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
	abortVec = containers.MakeVector(types.T_bool.ToType(), mp)
	n.appends.ForEach(
		func(node *AppendNode) bool {
			if in, _ := node.PreparedIn(start, end); !in {
				return true
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
				abortVec.Append(node.IsAborted(), false)
			}
			return true
		}, true)
	return
}

func (n *AppendMVCCHandle) FillInCommitTSVecLocked(commitTSVec containers.Vector, maxrow uint32, mp *mpool.MPool) {
	n.appends.ForEach(
		func(node *AppendNode) bool {
			if node.maxRow > maxrow {
				return false
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
			}
			return true
		},
		true)
}

func (n *AppendMVCCHandle) GetCommitTSVecInRange(start, end types.TS, mp *mpool.MPool) containers.Vector {
	n.RLock()
	defer n.RUnlock()
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	n.appends.ForEach(
		func(node *AppendNode) bool {
			in, _ := node.PreparedIn(start, end)
			if in {
				for i := 0; i < int(node.maxRow-node.startRow); i++ {
					commitTSVec.Append(node.GetCommitTS(), false)
				}
			}
			return true
		},
		true)
	return commitTSVec
}

// GetVisibleRowLocked returns all rows visible to txn. Holes include aborted
// rows and committed rows outside the transaction snapshot.
func (n *AppendMVCCHandle) GetVisibleRowLocked(
	_ context.Context,
	txn txnif.TxnReader,
) (selection index.RowSelection, visible bool, err error) {
	for {
		txnToWait := make([]txnif.TxnReader, 0)
		n.appends.ForEach(func(an *AppendNode) bool {
			if !an.IsSameTxn(txn) {
				if needWait, waitTxn := an.NeedWaitCommitting(txn.GetStartTS()); needWait {
					txnToWait = append(txnToWait, waitTxn)
				}
			}
			return true
		}, true)
		if len(txnToWait) == 0 {
			break
		}
		n.RUnlock()
		for _, waitTxn := range txnToWait {
			waitTxn.GetTxnState(true)
		}
		n.RLock()
	}

	n.appends.ForEach(func(an *AppendNode) bool {
		if an.IsVisible(txn) {
			visible = true
			selection.AddRange(an.startRow, an.maxRow)
		}
		return true
	}, true)
	selection.MakePrefix()
	return
}

// it collects all append nodes that are prepared before the given ts
// foreachFn is called for each append node that is prepared before the given ts
func (n *AppendMVCCHandle) CollectUncommittedANodesPreparedBeforeLocked(
	ts types.TS,
	foreachFn func(*AppendNode),
) (anyWaitable bool) {
	if n.appends.IsEmpty() {
		return
	}
	n.appends.ForEach(func(an *AppendNode) bool {
		needWait, txn := an.NeedWaitCommitting(ts)
		if txn == nil {
			return true
		}
		if needWait {
			foreachFn(an)
			anyWaitable = true
		}
		return true
	}, false)
	return
}

func (n *AppendMVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.appends.InsertNode(an)
}

// AddAppendNodeLocked add a new appendnode to the list.
func (n *AppendMVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32,
) (an *AppendNode, created bool) {
	if n.appends.IsEmpty() || !n.appends.GetUpdateNodeLocked().IsSameTxn(txn) {
		// if the appends is empty or the last appendnode is not of the same txn,
		// create a new appendnode and append it to the list.
		an = NewAppendNode(txn, startRow, maxRow, n.meta.IsTombstone, n)
		n.appends.InsertNode(an)
		created = true
	} else {
		// if the last appendnode is of the same txn, update the maxrow of the last appendnode.
		an = n.appends.GetUpdateNodeLocked()
		created = false
		an.SetMaxRow(maxRow)
	}
	return
}

// Reschedule until all appendnode is committed.
// Pending appendnode is not visible for compaction txn.
func (n *AppendMVCCHandle) PrepareCompactLocked() bool {
	return n.allAppendsCommittedLocked()
}
func (n *AppendMVCCHandle) PrepareCompact() bool {
	n.RLock()
	defer n.RUnlock()
	return n.allAppendsCommittedLocked()
}

func (n *AppendMVCCHandle) GetLatestAppendPrepareTSLocked() types.TS {
	if n.appends == nil || n.appends.IsEmpty() {
		return types.TS{}
	}
	var latest types.TS
	n.appends.ForEach(func(node *AppendNode) bool {
		prepare := node.GetPrepare()
		if prepare.GT(&latest) {
			latest = prepare
		}
		return true
	}, true)
	return latest
}
func (n *AppendMVCCHandle) GetMeta() *catalog.ObjectEntry {
	return n.meta
}

// check if all appendnodes are committed.
func (n *AppendMVCCHandle) allAppendsCommittedLocked() bool {
	if n.appends == nil {
		meta := n.GetMeta()
		logutil.Warnf("[MetadataCheck] appends mvcc is nil, obj %v, has dropped %v, deleted at %v",
			meta.ID().String(),
			meta.HasDropCommitted(),
			meta.GetDeleteAt().ToString())
		return false
	}
	if n.appends.IsEmpty() {
		return true
	}
	allCommitted := true
	n.appends.ForEach(func(node *AppendNode) bool {
		if !node.IsCommitted() {
			allCommitted = false
			return false
		}
		return true
	}, true)
	return allCommitted
}

func (n *AppendMVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *AppendMVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

// AllAppendsCommittedBefore returns true if all appendnode is committed before ts.
func (n *AppendMVCCHandle) AllAppendsCommittedBeforeLocked(ts types.TS) bool {
	if n.appends == nil || n.appends.IsEmpty() {
		return false
	}
	allBefore := true
	n.appends.ForEach(func(node *AppendNode) bool {
		commitTS := node.GetCommitTS()
		if !node.IsCommitted() || !commitTS.LT(&ts) {
			allBefore = false
			return false
		}
		return true
	}, true)
	return allBefore
}

func (n *AppendMVCCHandle) StringLocked() string {
	return n.appends.StringLocked()
}

func (n *AppendMVCCHandle) EstimateMemSizeLocked() int {
	asize := 0
	if n.appends != nil {
		asize += len(n.appends.MVCC) * catalog.AppendNodeApproxSize
	}
	return asize
}

// GetTotalRow is only for replay
func (n *AppendMVCCHandle) GetTotalRow() uint32 {
	an := n.appends.GetUpdateNodeLocked()
	if an == nil {
		return 0
	}
	return an.maxRow
}

func (n *AppendMVCCHandle) GetID() *common.ID {
	return n.meta.AsCommonID()
}
