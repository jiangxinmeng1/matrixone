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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type less2TestObject struct {
	data.Object
	minCommitTS types.TS
	meta        *ObjectEntry
}

func (o *less2TestObject) GetMinCommitTS() types.TS { return o.minCommitTS }
func (o *less2TestObject) UpdateMeta(meta any)      { o.meta = meta.(*ObjectEntry) }

func newLess2TestObjectEntry(
	tbl *TableEntry,
	appendable bool,
	create types.TS,
	minCommitTS types.TS,
) *ObjectEntry {
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, appendable, false, false)
	entry := &ObjectEntry{
		table: tbl,
		ObjectNode: ObjectNode{
			SortHint: tbl.GetDB().catalog.NextObject(),
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: create,
		},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *stats,
		},
		CreateNode:  txnbase.NewTxnMVCCNodeWithTS(create),
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	entry.objData = &less2TestObject{minCommitTS: minCommitTS, meta: entry}
	return entry
}

func addLess2DroppedObject(l *ObjectList, created *ObjectEntry, delete types.TS) *ObjectEntry {
	dropped := created.Clone()
	dropped.DeletedAt = delete
	dropped.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(delete)
	dropped.ObjectState = ObjectState_Delete_ApplyCommit
	updatedC := created.Clone()
	updatedC.nextVersion = dropped
	dropped.prevVersion = updatedC
	l.modify(nil, dropped, updatedC)
	return dropped
}

func collectObjectListEntries(l *ObjectList) []*ObjectEntry {
	it := l.tree.Load().Iter()
	defer it.Release()
	entries := make([]*ObjectEntry, 0)
	for ok := it.First(); ok; ok = it.Next() {
		entries = append(entries, it.Item())
	}
	return entries
}

func requireLess2Ranks(t *testing.T, entries []*ObjectEntry, ranks ...int) {
	require.Len(t, entries, len(ranks))
	for i, entry := range entries {
		rank, _ := entry.objectListRankAndTS()
		require.Equalf(t, ranks[i], rank, "entry %d: %s", i, entry.String())
	}
}

func TestObjectListLess2TierOrderAfterFlush(t *testing.T) {
	c := MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	list := tbl.getObjectList(false)

	activeAObj := newLess2TestObjectEntry(tbl, true, types.BuildTS(300, 0), types.BuildTS(300, 0))
	flushedAObjC := newLess2TestObjectEntry(tbl, true, types.BuildTS(100, 0), types.BuildTS(100, 0))
	activeNAObj := newLess2TestObjectEntry(tbl, false, types.BuildTS(400, 0), types.TS{})
	flushedNAObjC := newLess2TestObjectEntry(tbl, false, types.BuildTS(200, 0), types.TS{})

	list.Set(activeAObj)
	list.Set(flushedAObjC)
	flushedAObjD := addLess2DroppedObject(list, flushedAObjC, types.BuildTS(500, 0))
	list.Set(activeNAObj)
	list.Set(flushedNAObjC)
	_ = addLess2DroppedObject(list, flushedNAObjC, types.BuildTS(600, 0))

	entries := collectObjectListEntries(list)
	requireLess2Ranks(t, entries, 0, 1, 2, 3, 4, 5)
	require.Same(t, activeAObj, entries[0])
	require.True(t, entries[1].ID().EQ(flushedAObjC.ID()))
	require.Same(t, flushedAObjD, entries[2])
	require.Same(t, activeNAObj, entries[3])
	require.True(t, entries[4].ID().EQ(flushedNAObjC.ID()))
}

func TestObjectListLess2TierMoveOnFlush(t *testing.T) {
	c := MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	list := tbl.getObjectList(false)

	aobj := newLess2TestObjectEntry(tbl, true, types.BuildTS(100, 0), types.BuildTS(100, 0))
	list.Set(aobj)
	rank, _ := aobj.objectListRankAndTS()
	require.Equal(t, 0, rank)

	dropped := addLess2DroppedObject(list, aobj, types.BuildTS(200, 0))
	entries := collectObjectListEntries(list)
	requireLess2Ranks(t, entries, 1, 2)
	require.True(t, entries[0].ID().EQ(aobj.ID()))
	require.Same(t, dropped, entries[1])
}

func TestGetUpdateEntryUsesMinCommitTSForDroppedAndCreatedEntries(t *testing.T) {
	c := MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(MockSchema(1, 0), nil, nil)
	require.NoError(t, err)

	created := newLess2TestObjectEntry(tbl, true, types.BuildTS(999, 0), types.BuildTS(150, 0))
	dropped := created.Clone()
	dropped.DeletedAt = types.BuildTS(300, 0)
	dropped.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(types.BuildTS(300, 0))
	dropped.prevVersion = created
	created.nextVersion = dropped

	stats := objectio.NewObjectStatsWithObjectID(dropped.ID(), true, false, false)
	updatedD, updatedC, isNew := dropped.GetUpdateEntry(txnbase.MockTxnReaderWithNow(), stats)
	require.True(t, isNew)
	require.Equal(t, types.BuildTS(150, 0), updatedD.CreatedAt)
	require.Equal(t, types.BuildTS(150, 0), updatedC.CreatedAt)
	require.Same(t, updatedD, updatedC.nextVersion)
	require.Same(t, updatedC, updatedD.prevVersion)
}
