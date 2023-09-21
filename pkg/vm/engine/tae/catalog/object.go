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
	"context"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ObjectDataFactory = func(meta *ObjectEntry) data.Block

type ObjectEntry struct {
	*BaseEntryImpl[*ObjectMVCCNode]
	table *TableEntry
	*ObjectNode
	ID          types.Blockid
	blkData     data.Block
	location    objectio.Location
	pkZM        atomic.Pointer[index.ZM]
	isTombstone bool
}

func NewReplayObjectEntry() *ObjectEntry {
	return &ObjectEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{} },
		),
	}
}

func NewObjectEntry(
	table *TableEntry,
	id *objectio.Blockid,
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory ObjectDataFactory,
) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{} }),
		table: table,
		ObjectNode: &ObjectNode{
			state: state,
		},
	}
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	e.BaseEntryImpl.CreateWithTxn(txn, &ObjectMVCCNode{})
	return e
}

func NewObjectEntryWithMeta(
	segment *SegmentEntry,
	id *objectio.Blockid,
	txn txnif.AsyncTxn,
	state EntryState,
	dataFactory BlockDataFactory,
	metaLoc objectio.Location,
	deltaLoc objectio.Location) *BlockEntry {
	e := &BlockEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: state,
		},
	}
	e.CreateWithTxnAndMeta(txn, metaLoc, deltaLoc)
	if dataFactory != nil {
		e.blkData = dataFactory(e)
	}
	return e
}

func NewStandaloneObject(table *TableEntry, id *objectio.Blockid, ts types.TS) *ObjectEntry {
	e := &ObjectEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *ObjectMVCCNode { return &ObjectMVCCNode{} }),
		table: table,
		ObjectNode: &ObjectNode{
			state: ES_Appendable,
		},
	}
	e.BaseEntryImpl.CreateWithTS(ts, &ObjectMVCCNode{})
	return e
}

func NewStandaloneObjectWithLoc(
	segment *SegmentEntry,
	id *objectio.Blockid,
	ts types.TS,
	metaLoc objectio.Location,
	delLoc objectio.Location) *BlockEntry {
	e := &BlockEntry{
		ID: *id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: ES_Appendable,
		},
	}
	e.CreateWithLoc(ts, metaLoc, delLoc)
	return e
}

func NewSysObjectEntry(segment *SegmentEntry, id types.Blockid) *BlockEntry {
	e := &BlockEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *MetadataMVCCNode { return &MetadataMVCCNode{} }),
		segment: segment,
		BlockNode: &BlockNode{
			state: ES_Appendable,
		},
	}
	e.BaseEntryImpl.CreateWithTS(types.SystemDBTS, &MetadataMVCCNode{})
	return e
}

func (entry *ObjectEntry) BuildDeleteObjectName() objectio.ObjectName {
	entry.table.Lock()
	id := entry.segment.nextObjectIdx
	entry.segment.nextObjectIdx++
	entry.segment.Unlock()
	return objectio.BuildObjectName(entry.ID.Segment(), id)
}

func (entry *ObjectEntry) Less(b *BlockEntry) int {
	return entry.ID.Compare(b.ID)
}

func (entry *ObjectEntry) GetCatalog() *Catalog { return entry.table.db.catalog }

func (entry *ObjectEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *ObjectEntry) GetTable() *TableEntry {
	return entry.table
}
func (entry *ObjectEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Block
	entry.RLock()
	defer entry.RUnlock()
	return newBlockCmd(id, cmdType, entry), nil
}

func (entry *ObjectEntry) Set1PC() {
	entry.GetLatestNodeLocked().Set1PC()
}
func (entry *ObjectEntry) Is1PC() bool {
	return entry.GetLatestNodeLocked().Is1PC()
}
func (entry *ObjectEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevelLocked(level))
	return s
}

func (entry *ObjectEntry) Repr() string {
	id := entry.AsCommonID()
	return fmt.Sprintf("[%s]BLK[%s]", entry.state.Repr(), id.String())
}

func (entry *ObjectEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *ObjectEntry) StringLocked() string {
	return fmt.Sprintf("[%s]BLK%s", entry.state.Repr(), entry.BaseEntryImpl.StringLocked())
}

func (entry *ObjectEntry) StringWithLevel(level common.PPLevel) string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringWithLevelLocked(level)
}

func (entry *ObjectEntry) StringWithLevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("[%s]BLK[%s][C@%s,D@%s]",
			entry.state.Repr(), entry.ID.ShortString(), entry.GetCreatedAt().ToString(), entry.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("[%s]BLK[%s]%s", entry.state.Repr(), entry.ID.String(), entry.BaseEntryImpl.StringLocked())
}

func (entry *ObjectEntry) AsCommonID() *common.ID {
	return &common.ID{
		DbID:    entry.GetTable().GetDB().ID,
		TableID: entry.GetTable().ID,
		BlockID: entry.ID,
	}
}

func (entry *ObjectEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeBlockFactory()
	entry.blkData = dataFactory(entry)
}
func (entry *ObjectEntry) GetBlockData() data.Block { return entry.blkData }
func (entry *ObjectEntry) GetSchema() *Schema       { return entry.GetTable().GetLastestSchema() }
func (entry *ObjectEntry) PrepareRollback() (err error) {
	var empty bool
	empty, err = entry.BaseEntryImpl.PrepareRollback()
	if err != nil {
		panic(err)
	}
	if empty {
		if err = entry.GetTable().RemoveEntry(entry); err != nil {
			return
		}
	}
	return
}

func (entry *ObjectEntry) MakeKey() []byte {
	prefix := entry.ID // copy id
	return prefix[:]
}

// PrepareCompact is performance insensitive
// a block can be compacted:
// 1. no uncommited node
// 2. at least one committed node
// 3. not compacted
func (entry *ObjectEntry) PrepareCompact() bool {
	entry.RLock()
	defer entry.RUnlock()
	if entry.HasUncommittedNode() {
		return false
	}
	if !entry.HasCommittedNode() {
		return false
	}
	if entry.HasDropCommittedLocked() {
		return false
	}
	return true
}

// IsActive is coarse API: no consistency check
func (entry *ObjectEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

// GetTerminationTS is coarse API: no consistency check
func (entry *ObjectEntry) GetTerminationTS() (ts types.TS, terminated bool) {
	tableEntry := entry.GetTable()
	dbEntry := tableEntry.GetDB()

	dbEntry.RLock()
	terminated, ts = dbEntry.TryGetTerminatedTS(true)
	if terminated {
		dbEntry.RUnlock()
		return
	}
	dbEntry.RUnlock()

	tableEntry.RLock()
	terminated, ts = tableEntry.TryGetTerminatedTS(true)
	if terminated {
		tableEntry.RUnlock()
		return
	}
	tableEntry.RUnlock()
	return
}

func (entry *ObjectEntry) HasPersistedData() bool {
	return !entry.GetMetaLoc().IsEmpty()
}
func (entry *ObjectEntry) FastGetMetaLoc() objectio.Location {
	return entry.location
}

func (entry *ObjectEntry) GetMetaLoc() objectio.Location {
	if len(entry.location) > 0 {
		return entry.location
	}
	entry.RLock()
	defer entry.RUnlock()
	if entry.GetLatestNodeLocked() == nil {
		return nil
	}
	str := entry.GetLatestNodeLocked().BaseNode.MetaLoc
	return str
}

func (entry *ObjectEntry) GetVisibleMetaLoc(txn txnif.TxnReader) objectio.Location {
	entry.RLock()
	defer entry.RUnlock()
	str := entry.GetVisibleNode(txn).BaseNode.MetaLoc
	return str
}

func (entry *ObjectEntry) CreateWithLoc(ts types.TS, metaLoc objectio.Location, deltaLoc objectio.Location) {
	baseNode := &ObjectMVCCNode{
		MetaLoc:  metaLoc,
	}
	node := &MVCCNode[*ObjectMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(ts),
		BaseNode:    baseNode.CloneAll(),
	}
	entry.Insert(node)
	entry.location = metaLoc
}

func (entry *ObjectEntry) CreateWithTxnAndMeta(txn txnif.AsyncTxn, metaLoc objectio.Location, deltaLoc objectio.Location) {
	baseNode := &ObjectMVCCNode{
		MetaLoc:  metaLoc,
	}
	node := &MVCCNode[*ObjectMVCCNode]{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		BaseNode:    baseNode.CloneAll(),
	}
	entry.Insert(node)
	entry.location = metaLoc
}
func (entry *ObjectEntry) UpdateMetaLoc(txn txnif.TxnReader, metaLoc objectio.Location) (isNewNode bool, err error) {
	entry.Lock()
	defer entry.Unlock()
	needWait, txnToWait := entry.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		entry.Unlock()
		txnToWait.GetTxnState(true)
		entry.Lock()
	}
	err = entry.CheckConflict(txn)
	if err != nil {
		return
	}
	baseNode := &ObjectMVCCNode{
		MetaLoc: metaLoc,
	}
	var node *MVCCNode[*ObjectMVCCNode]
	isNewNode, node = entry.getOrSetUpdateNode(txn)
	node.BaseNode.Update(baseNode)
	if !entry.IsAppendable() {
		entry.location = metaLoc
	}
	return
}

func (entry *ObjectEntry) GetPKZoneMap(
	ctx context.Context,
	fs fileservice.FileService,
) (zm *index.ZM, err error) {

	zm = entry.pkZM.Load()
	if zm != nil {
		return
	}
	location := entry.GetMetaLoc()
	var meta objectio.ObjectMeta
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	seqnum := entry.GetSchema().GetSingleSortKeyIdx()
	cloned := meta.MustDataMeta().GetBlockMeta(uint32(location.ID())).MustGetColumn(uint16(seqnum)).ZoneMap().Clone()
	zm = &cloned
	entry.pkZM.Store(zm)
	return
}
