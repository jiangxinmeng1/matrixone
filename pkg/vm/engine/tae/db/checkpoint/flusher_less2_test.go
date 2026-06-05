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

package checkpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func newFlusherLess2ObjectEntry(
	tbl *catalog.TableEntry,
	appendable bool,
	create types.TS,
) *catalog.ObjectEntry {
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, appendable, false, false)
	return catalog.MockObjectEntry(tbl, stats, false, nil, create)
}

func addFlusherLess2DroppedObject(tbl *catalog.TableEntry, created *catalog.ObjectEntry, delete types.TS) {
	_ = tbl
	// Use the catalog helper so next/prev counterpart pointers match production state.
	catalog.MockDroppedObjectEntry2List(created, delete)
}

func TestForeachAobjBeforeCollectsOnlyTier1a(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)

	activeAObj := newFlusherLess2ObjectEntry(tbl, true, types.BuildTS(100, 0))
	flushedAObj := newFlusherLess2ObjectEntry(tbl, true, types.BuildTS(110, 0))
	tooNewAObj := newFlusherLess2ObjectEntry(tbl, true, types.BuildTS(300, 0))
	naObj := newFlusherLess2ObjectEntry(tbl, false, types.BuildTS(90, 0))

	tbl.AddEntryLocked(activeAObj)
	tbl.AddEntryLocked(flushedAObj)
	tbl.AddEntryLocked(tooNewAObj)
	tbl.AddEntryLocked(naObj)
	addFlusherLess2DroppedObject(tbl, flushedAObj, types.BuildTS(200, 0))

	collected := make([]*catalog.ObjectEntry, 0)
	foreachAobjBefore(
		context.Background(),
		tbl,
		types.BuildTS(150, 0),
		types.BuildTS(50, 0),
		func(obj *catalog.ObjectEntry) { collected = append(collected, obj) },
		nil,
	)

	require.Len(t, collected, 1)
	require.True(t, collected[0].ID().EQ(activeAObj.ID()))
}
