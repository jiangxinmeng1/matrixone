// Copyright 2022 Matrix Origin
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

package frontend

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type TableInfo_2 struct {
	accountID int32
	dbID      uint64
	tableID   uint64
	state     TableState
	sinkers   []SinkerEntry
	rel       relationFactory
	mu        sync.RWMutex
}


/*
add new sinker,flush snapshot, to last wm of the table
*/
// get candidate
// iteration:1.merge sinkers or update watermark, update sinker
func NewTableInfo_2(
	dbID, tableID uint64,
	rel relationFactory,
	sinkers []SinkerEntry,
) *TableInfo_2 {
	return &TableInfo_2{
		dbID:              dbID,
		tableID:           tableID,
		rel:               rel,
		sinker:            sinker,
		mu:                sync.RWMutex{},
		flushWatermarkFn:  flushWatermarkFn,
		insertWatermarkFn: insertWatermarkFn,
		replayFn:          replayFn,
		deleteFn:          deleteFn,
	}
}

func tableInfoLess(a, b *TableInfo_2) bool {
	return a.tableID < b.tableID
}

func (t *TableInfo_2) Resume() error {
	return nil
}
func (t *TableInfo_2) Pause() error {
	return nil
}
func (t *TableInfo_2) Cancel() error {
	return nil
}
func (t *TableInfo_2) Restart() error {
	return nil
}
func (t *TableInfo_2) GetWatermark() types.TS {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.watermark
}

func (t *TableInfo_2) SetWatermark(watermark types.TS) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.watermark = watermark
}
func (t *TableInfo_2) GetState() TableState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

func (t *TableInfo_2) SetState(state TableState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = state
}

func (t *TableInfo_2) FlushWatermark(
	ctx context.Context,
	internalExecutor ie.InternalExecutor,
	accountID uint64,
	errorCode int,
	errorMsg string,
) error {
	if t.flushWatermarkFn != nil {
		return t.flushWatermarkFn(ctx, t.tableID, t.GetWatermark(), int32(accountID), 0, errorCode, "", errorMsg)
	}
	sql := cdc.CDCSQLBuilder.IndexUpdateWatermarkSQL(
		accountID,
		t.tableID,
		0, //TODO
		t.GetWatermark().ToString(),
		errorCode,
		errorMsg,
	)
	return internalExecutor.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (t *TableInfo_2) InsertIndexWatermark(
	ctx context.Context,
	internalExecutor ie.InternalExecutor,
	accountID uint64,
) error {
	if t.insertWatermarkFn != nil {
		return t.insertWatermarkFn(ctx, t.tableID, int32(accountID), 0)
	}
	sql := cdc.CDCSQLBuilder.IndexInsertLogSQL(
		accountID,
		t.tableID,
		0, //TODO
	)
	return internalExecutor.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (t *TableInfo_2) ReplayWatermark(
	ctx context.Context,
	internalExecutor ie.InternalExecutor,
	accountID uint64,
) error {
	if t.replayFn != nil {
		watermark, _, _, err := t.replayFn(ctx, t.tableID, uint32(accountID), 0)
		if err != nil {
			return err
		}
		t.watermark = watermark
		return nil
	}
	panic("todo")
}

func (t *TableInfo_2) Delete(
	ctx context.Context,
	internalExecutor ie.InternalExecutor,
	accountID uint64,
) error {
	if t.deleteFn != nil {
		return t.deleteFn(ctx, t.tableID, uint32(accountID), 0)
	}
	panic("todo")
}
