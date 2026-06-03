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
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/stretchr/testify/require"
)

type sortTestObjectData struct {
	data.Object
	minCommitTS types.TS
	maxCommitTS types.TS
}

func (d *sortTestObjectData) GetMinCommitTS() types.TS { return d.minCommitTS }
func (d *sortTestObjectData) GetMaxCommitTS() types.TS { return d.maxCommitTS }

func makeSortTestObject(createdAt, deletedAt int64, appendable, persisted, local bool) *ObjectEntry {
	var objectID objectio.ObjectId
	if appendable {
		id := uuid.Must(uuid.NewV7())
		copy(objectID[:], id[:])
	} else {
		objectID = objectio.NewObjectid()
	}

	stats := objectio.NewObjectStatsWithObjectID(&objectID, appendable, false, false)
	if persisted {
		objectio.SetObjectStatsExtent(stats, objectio.NewRandomExtent())
	}

	return &ObjectEntry{
		ObjectNode: ObjectNode{
			IsLocal:    local,
			forcePNode: persisted,
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: types.BuildTS(createdAt, 0),
			DeletedAt: types.BuildTS(deletedAt, 0),
		},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *stats,
		},
	}
}

func makeSortTestAobj(createdAt, minCommitAt, maxCommitAt int64) *ObjectEntry {
	obj := makeSortTestObject(createdAt, 0, true, false, false)
	obj.objData = &sortTestObjectData{
		minCommitTS: types.BuildTS(minCommitAt, 0),
		maxCommitTS: types.BuildTS(maxCommitAt, 0),
	}
	return obj
}

func makeSortTestDeleteEntry(createdAt, deletedAt int64) (*ObjectEntry, *ObjectEntry) {
	created := makeSortTestObject(createdAt, 0, false, true, false)
	deleted := created.Clone()
	deleted.DeletedAt = types.BuildTS(deletedAt, 0)
	deleted.ObjectState = ObjectState_Delete_ApplyCommit
	created.nextVersion = deleted
	deleted.prevVersion = created
	return created, deleted
}

func sortByLess2(objects []*ObjectEntry) {
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Less2(objects[j])
	})
}

func TestLess2TierOrdering(t *testing.T) {
	aobj2 := makeSortTestAobj(10, 200, 260)
	aobj1 := makeSortTestAobj(300, 100, 180)
	create2 := makeSortTestObject(220, 0, false, true, false)
	create1 := makeSortTestObject(120, 0, false, true, false)
	_, delete2 := makeSortTestDeleteEntry(80, 240)
	_, delete1 := makeSortTestDeleteEntry(90, 140)
	uncommitted := makeSortTestObject(50, 0, false, false, true)

	objects := []*ObjectEntry{
		uncommitted, delete2, create2, aobj2, delete1, create1, aobj1,
	}
	sortByLess2(objects)

	require.Equal(t, []*ObjectEntry{
		aobj1,
		aobj2,
		create1,
		create2,
		delete1,
		delete2,
		uncommitted,
	}, objects)
}

func TestLess2SplitsCreateAndDeleteEntries(t *testing.T) {
	createLate := makeSortTestObject(300, 0, false, true, false)
	_, deleteEarly := makeSortTestDeleteEntry(10, 100)
	createEarly := makeSortTestObject(200, 0, false, true, false)
	_, deleteLate := makeSortTestDeleteEntry(20, 400)

	objects := []*ObjectEntry{deleteLate, createLate, deleteEarly, createEarly}
	sortByLess2(objects)

	require.Equal(t, []*ObjectEntry{
		createEarly,
		createLate,
		deleteEarly,
		deleteLate,
	}, objects)
}

func TestLess2ReverseIterationOrder(t *testing.T) {
	aobj := makeSortTestAobj(100, 100, 160)
	create := makeSortTestObject(200, 0, false, true, false)
	_, deleteEntry := makeSortTestDeleteEntry(50, 300)
	uncommitted := makeSortTestObject(400, 0, false, false, true)

	objects := []*ObjectEntry{create, aobj, uncommitted, deleteEntry}
	sortByLess2(objects)

	var reverse []*ObjectEntry
	for i := len(objects) - 1; i >= 0; i-- {
		reverse = append(reverse, objects[i])
	}

	require.Equal(t, []*ObjectEntry{
		uncommitted,
		deleteEntry,
		create,
		aobj,
	}, reverse)
}
