// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TestContainsSkipsDuplicateAbortedOffsets(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	idx := NewMutIndex(types.T_int32.ToType())

	insert := func(offset int) {
		vec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(vec, int32(7), false, mp))
		require.NoError(t, idx.BatchUpsert(vec, offset))
		vec.Free(mp)
	}
	insert(0)
	insert(1)

	check := func(skipFn func(uint32) error) bool {
		keys := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(keys, int32(7), false, mp))
		err := idx.Contains(
			context.Background(),
			keys,
			index.NewZM(types.T_int32, 0),
			&types.Blockid{},
			skipFn,
			mp,
		)
		require.NoError(t, err)
		deleted := keys.IsNull(0)
		keys.Free(mp)
		return deleted
	}

	require.True(t, check(func(row uint32) error {
		if row == 0 {
			return index.ErrNotFound
		}
		return nil
	}), "a live retry offset must still classify the key as deleted")
	require.False(t, check(func(uint32) error {
		return index.ErrNotFound
	}), "all-aborted offsets must leave the key visible")
}

func TestGetDuplicatedRowsSkipsSelectionHoles(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	idx := NewMutIndex(types.T_int32.ToType())

	values := vector.NewVec(types.T_int32.ToType())
	defer values.Free(mp)
	for _, value := range []int32{7, 7, 8} {
		require.NoError(t, vector.AppendFixed(values, value, false, mp))
	}
	require.NoError(t, idx.BatchUpsert(values, 0))

	keys := vector.NewVec(types.T_int32.ToType())
	defer keys.Free(mp)
	require.NoError(t, vector.AppendFixed(keys, int32(7), false, mp))
	rowIDs := vector.NewVec(types.T_Rowid.ToType())
	defer rowIDs.Free(mp)
	require.NoError(t, vector.AppendFixed(rowIDs, types.Rowid{}, true, mp))

	selection := index.RowSelection{}
	selection.AddRange(0, 1)
	selection.AddRange(2, 3)
	blockID := types.Blockid{}
	require.NoError(t, idx.GetDuplicatedRows(
		context.Background(), keys, index.NewZM(types.T_int32, 0), &blockID, rowIDs,
		func() (index.RowSelection, error) { return selection, nil }, nil, mp,
	))
	require.False(t, rowIDs.IsNull(0))
	rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDs, 0)
	require.Equal(t, uint32(0), rowID.GetRowOffset())
}
