// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

type objectsIter struct {
	onlyVisible bool
	ts          types.TS
	iter        btree.IterG[objectio.ObjectEntry]
}

var _ objectio.ObjectIter = new(objectsIter)

func (b *objectsIter) Next() bool {
	for b.iter.Next() {
		entry := b.iter.Item()
		if b.onlyVisible && !entry.Visible(b.ts) {
			// not visible
			continue
		}
		return true
	}
	return false
}

func (b *objectsIter) Entry() objectio.ObjectEntry {
	return b.iter.Item()
}

func (b *objectsIter) Close() error {
	b.iter.Release()
	return nil
}

type BlocksIter interface {
	Next() bool
	Close() error
	Entry() types.Blockid
}

func (p *PartitionState) ApproxInMemTombstones() int {
	return p.inMemTombstoneRowIdIndex.Len()
}

func (p *PartitionState) ApproxInMemRows() int {
	return p.rows.Len()
}

// ApproxDataObjectsNum not accurate!  only used by stats
func (p *PartitionState) ApproxDataObjectsNum() int {
	return p.dataObjectsNameIndex.Len()
}

func (p *PartitionState) ApproxTombstoneObjectsNum() int {
	return p.tombstoneObjectsNameIndex.Len()
}

func (p *PartitionState) newTombstoneObjectsIter(
	snapshot types.TS,
	onlyVisible bool) (objectio.ObjectIter, error) {

	iter := p.tombstoneObjectDTSIndex.Iter()
	if onlyVisible {
		pivot := objectio.ObjectEntry{
			DeleteTime: snapshot,
		}

		iter.Seek(pivot)
		if !iter.Prev() && p.tombstoneObjectDTSIndex.Len() > 0 {
			// reset iter only when seeked to the first item
			iter.Release()
			iter = p.tombstoneObjectDTSIndex.Iter()
		}
	}

	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          snapshot,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionState) newDataObjectIter(
	snapshot types.TS,
	onlyVisible bool) (objectio.ObjectIter, error) {

	iter := p.dataObjectsNameIndex.Iter()
	ret := &objectsIter{
		onlyVisible: onlyVisible,
		ts:          snapshot,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionState) IsNil() bool {
	return p == nil
}

func (p *PartitionState) NewObjectsIter(
	snapshot types.TS,
	onlyVisible bool,
	visitTombstone bool,
) (objectio.ObjectIter, error) {
	if !p.IsEmpty() && snapshot.LT(&p.start) {
		logutil.Infof("NewObjectsIter: tid:%v, ps:%p, snapshot ts:%s, minTS:%s",
			p.tid, p, snapshot.ToString(), p.start.ToString())
		msg := fmt.Sprintf("(%s<%s)", snapshot.ToString(), p.start.ToString())
		return nil, moerr.NewTxnStaleNoCtx(msg)
	}

	if visitTombstone {
		return p.newTombstoneObjectsIter(snapshot, onlyVisible)
	} else {
		return p.newDataObjectIter(snapshot, onlyVisible)
	}
}

func (p *PartitionState) NewDirtyBlocksIter() BlocksIter {
	//iter := p.dirtyBlocks.Copy().Iter()

	return nil
}

// In concurrent delete scenario, the following case may happen:
//
// / txn1   cn: write s3 tombstone      dn: commit s3 tombstone
// /	    |                              |
// /	----+---------------------+--------+-----------+---------->
// /	                          |                    |
// / txn2           cn: delete mem row(blocked)        cn: query PrimaryKeysMayBeModified and it returns false, which is wrong
//
// what PrimaryKeysMayBeModified does:
//  1. no mem rows in partition state
//  2. lastFlushTimestamp > from
//  3. it boils down to PKPersistedBetween, where dataobjects are empty and tombstones are ignored
func (p *PartitionState) HasTombstoneChanged(from, to types.TS) (exist bool) {
	if p.tombstoneObjectDTSIndex.Len() == 0 {
		return false
	}
	iter := p.tombstoneObjectDTSIndex.Iter()
	defer iter.Release()

	// Created after from
	if iter.Seek(objectio.ObjectEntry{CreateTime: from}) {
		return true
	}

	iter.First()
	// Deleted after from
	ok := iter.Seek(objectio.ObjectEntry{DeleteTime: from})
	if ok {
		item := iter.Item()
		return !item.DeleteTime.IsEmpty()
	}
	return false
}

// GetChangedObjsBetween get changed objects between [begin, end],
// notice that if an object is created after begin and deleted before end, it will be ignored.
func (p *PartitionState) GetChangedObjsBetween(
	begin types.TS,
	end types.TS,
) (
	deleted map[objectio.ObjectNameShort]struct{},
	inserted map[objectio.ObjectNameShort]struct{},
) {
	inserted = make(map[objectio.ObjectNameShort]struct{})
	deleted = make(map[objectio.ObjectNameShort]struct{})

	iter := p.dataObjectTSIndex.Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.GT(&end) {
			break
		}

		if entry.IsDelete {
			// if the object is inserted and deleted between [begin, end], it will be ignored.
			if _, ok := inserted[entry.ShortObjName]; !ok {
				deleted[entry.ShortObjName] = struct{}{}
			} else {
				delete(inserted, entry.ShortObjName)
			}
		} else {
			inserted[entry.ShortObjName] = struct{}{}
		}

	}
	return
}

func (p *PartitionState) BlockPersisted(blockID *types.Blockid) bool {
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()

	pivot := objectio.ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) CollectObjectsBetween(
	start, end types.TS,
) (insertList, deletedList []objectio.ObjectStats) {

	iter := p.dataObjectTSIndex.Iter()
	defer iter.Release()

	if !iter.Seek(ObjectIndexByTSEntry{
		Time: start,
	}) {
		return
	}

	nameIdx := p.dataObjectsNameIndex

	for ok := true; ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.GT(&end) {
			break
		}

		var ss objectio.ObjectStats
		objectio.SetObjectStatsShortName(&ss, &entry.ShortObjName)

		val, exist := nameIdx.Get(objectio.ObjectEntry{
			ObjectStats: ss,
		})

		if !exist {
			continue
		}

		// case1: no soft delete
		if val.DeleteTime.IsEmpty() {
			insertList = append(insertList, val.ObjectStats)
		} else {
			if val.CreateTime.LT(&start) {
				// create --------- delete
				//          start -------- end
				if val.DeleteTime.LE(&end) {
					deletedList = append(deletedList, val.ObjectStats)
				}
			} else {
				//        create ---------- delete
				// start ------------ end
				if val.DeleteTime.GT(&end) {
					insertList = append(insertList, val.ObjectStats)
				}
			}
		}
	}

	return
}

func (p *PartitionState) CheckIfObjectDeletedBeforeTS(
	ts types.TS,
	isTombstone bool,
	objId *objectio.ObjectId,
) bool {

	var tree *btree.BTreeG[objectio.ObjectEntry]
	if isTombstone {
		tree = p.tombstoneObjectsNameIndex
	} else {
		tree = p.dataObjectsNameIndex
	}

	var stats objectio.ObjectStats
	objectio.SetObjectStatsShortName(&stats, (*objectio.ObjectNameShort)(objId))
	val, exist := tree.Get(objectio.ObjectEntry{
		ObjectStats: stats,
	})

	if !exist {
		return true
	}

	return !val.DeleteTime.IsEmpty() && val.DeleteTime.LE(&ts)
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (objectio.ObjectEntry, bool) {
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()

	pivot := objectio.ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return iter.Item(), true
		}
	}
	return objectio.ObjectEntry{}, false
}

func (p *PartitionState) CollectTombstoneObjects(
	snapshot types.TS,
	appendTo func(stats *objectio.ObjectStats),
) (err error) {

	if p.ApproxTombstoneObjectsNum() == 0 {
		return
	}

	iter, err := p.NewObjectsIter(snapshot, true, true)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		item := iter.Entry()
		appendTo(&item.ObjectStats)
	}

	return nil
}
