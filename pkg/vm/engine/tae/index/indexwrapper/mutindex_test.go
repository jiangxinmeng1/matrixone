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

package indexwrapper

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestMutIndexDeleteAtPreservesOtherPositions(t *testing.T) {
	idx := NewMutIndex(types.T_int32.ToType())
	keys := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer keys.Close()
	keys.Append(int32(7), false)
	keys.Append(int32(7), false)
	require.NoError(t, idx.BatchUpsert(keys.GetDownstreamVector(), 0))

	require.NoError(t, idx.DeleteAt(int32(7), 0))
	rows, err := idx.GetActiveRow(int32(7))
	require.NoError(t, err)
	require.Equal(t, []uint32{1}, rows)
}

func TestGetDuplicatedRowsSkipsIneligibleCandidates(t *testing.T) {
	idx := NewMutIndex(types.T_int32.ToType())
	indexedKeys := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer indexedKeys.Close()
	for i := 0; i < 3; i++ {
		indexedKeys.Append(int32(7), false)
	}
	require.NoError(t, idx.BatchUpsert(indexedKeys.GetDownstreamVector(), 0))

	query := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer query.Close()
	query.Append(int32(7), false)
	rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer rowIDs.Close()
	rowIDs.Append(nil, true)

	objID := types.NewObjectid()
	blkID := types.NewBlockidWithObjectID(&objID, 0)
	err := idx.GetDuplicatedRows(
		context.Background(),
		query.GetDownstreamVector(),
		nil,
		&blkID,
		rowIDs.GetDownstreamVector(),
		func() (index.RowSelection, error) {
			return index.RowSelection{MinRow: 0, MaxRow: 3}, nil
		},
		func(row uint32) error {
			if row > 0 {
				return index.ErrNotFound
			}
			return nil
		},
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.False(t, rowIDs.IsNull(0))
	rid := rowIDs.Get(0).(types.Rowid)
	require.Equal(t, uint32(0), rid.GetRowOffset())

	skippedRowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer skippedRowIDs.Close()
	skippedRowIDs.Append(nil, true)
	err = idx.GetDuplicatedRows(
		context.Background(),
		query.GetDownstreamVector(),
		nil,
		&blkID,
		skippedRowIDs.GetDownstreamVector(),
		func() (index.RowSelection, error) {
			return index.RowSelection{MinRow: 0, MaxRow: 3}, nil
		},
		func(uint32) error { return index.ErrNotFound },
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.True(t, skippedRowIDs.IsNull(0))
}

func TestContainsSkipsIneligibleTombstoneCandidates(t *testing.T) {
	idx := NewMutIndex(types.T_Rowid.ToType())
	objID := types.NewObjectid()
	blkID := types.NewBlockidWithObjectID(&objID, 0)
	rowID := types.NewRowid(&blkID, 42)

	indexedKeys := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer indexedKeys.Close()
	for i := 0; i < 3; i++ {
		indexedKeys.Append(rowID, false)
	}
	require.NoError(t, idx.BatchUpsert(indexedKeys.GetDownstreamVector(), 0))

	query := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer query.Close()
	query.Append(rowID, false)
	err := idx.Contains(
		context.Background(),
		query.GetDownstreamVector(),
		nil,
		&blkID,
		func(row uint32) error {
			if row > 0 {
				return index.ErrNotFound
			}
			return nil
		},
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.True(t, query.IsNull(0))

	allSkipped := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer allSkipped.Close()
	allSkipped.Append(rowID, false)
	err = idx.Contains(
		context.Background(),
		allSkipped.GetDownstreamVector(),
		nil,
		&blkID,
		func(uint32) error { return index.ErrNotFound },
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.False(t, allSkipped.IsNull(0))

	conflicting := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer conflicting.Close()
	conflicting.Append(rowID, false)
	conflictErr := errors.New("prepared tombstone conflict")
	err = idx.Contains(
		context.Background(),
		conflicting.GetDownstreamVector(),
		nil,
		&blkID,
		func(row uint32) error {
			if row == 2 {
				return index.ErrNotFound
			}
			return conflictErr
		},
		common.DefaultAllocator,
	)
	require.ErrorIs(t, err, conflictErr)
	require.False(t, conflicting.IsNull(0))
}
