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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type TableInfo_2 struct {
	exec      *CDCTaskExecutor2
	tableDef  *plan.TableDef
	accountID int32
	dbID      uint64
	tableID   uint64
	tableName string
	dbName    string
	state     TableState
	inited    atomic.Bool
	sinkers   []*SinkerEntry
	watermark types.TS
	mu        sync.RWMutex
}

func NewTableInfo_2(
	exec *CDCTaskExecutor2,
	accountID int32,
	dbID, tableID uint64,
	dbName, tableName string,
	watermarkUpdater WatermarkUpdater,
) *TableInfo_2 {
	return &TableInfo_2{
		exec:      exec,
		accountID: accountID,
		sinkers:   make([]*SinkerEntry, 0),
		dbID:      dbID,
		tableID:   tableID,
		dbName:    dbName,
		tableName: tableName,
		state:     TableState_Finished,
		mu:        sync.RWMutex{},
	}
}
func (t *TableInfo_2) AddSinker(
	sinkConfig *SinkerConfig,
	dbTblInfo *cdc.DbTableInfo,
	watermarkUpdater WatermarkUpdater,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sinkerEntry := NewSinkerEntry(t.exec.cnUUID, dbTblInfo, t.tableDef, t, sinkConfig, watermarkUpdater)
	t.sinkers = append(t.sinkers, sinkerEntry)
	return nil
}

func (t *TableInfo_2) DeleteSinker(
	ctx context.Context,
	indexName string,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, sinker := range t.sinkers {
		if sinker.indexName == indexName {
			sinker.Delete()
		}
		t.sinkers = append(t.sinkers[:i], t.sinkers[i+1:]...)
		return nil
	}
	return moerr.NewInternalError(ctx, "sinker not found")
}

func (t *TableInfo_2) IsInitedAndFinished() bool {
	if !t.inited.Load() {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	hasActiveSinker := false
	for _, sinker := range t.sinkers {
		if !sinker.PermanentError() {
			hasActiveSinker = true
			break
		}
	}
	return hasActiveSinker && t.state == TableState_Finished
}

func (t *TableInfo_2) GetMinWaterMark() types.TS {
	t.mu.RLock()
	defer t.mu.RUnlock()
	minWatermark := t.watermark
	for _, sinker := range t.sinkers {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.PermanentError() {
			continue
		}
		if sinker.watermark.LT(&minWatermark) {
			minWatermark = sinker.watermark
		}
	}
	return minWatermark
}

func (t *TableInfo_2) GetSyncTask(ctx context.Context, toTS types.TS) *Iteration {
	t.mu.Lock()
	defer t.mu.Unlock()
	dirtySinker := t.getNewSinkersLocked()
	if dirtySinker != nil {
		if dirtySinker.watermark.GE(&t.watermark) {
			panic("logic error")
		}
		t.state = TableState_Running
		return &Iteration{
			table:   t,
			sinkers: []*SinkerEntry{dirtySinker},
			to:      t.watermark,
			from:    dirtySinker.watermark,
		}
	}
	if t.watermark.GE(&toTS) {
		panic("logic error")
	}
	t.state = TableState_Running
	return &Iteration{
		table:   t,
		sinkers: t.sinkers,
		to:      toTS,
		from:    t.watermark,
	}
}

// TODO
func toErrorCode(err error) int {
	return 0
}

func (t *TableInfo_2) onIterationFinished(iter *Iteration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// init sinker
	if len(iter.sinkers) == 1 && !iter.sinkers[0].inited.Load() {
		iter.sinkers[0].inited.Store(true)
		t.inited.Store(true)
		if t.watermark.LT(&iter.to) {
			t.watermark = iter.to
		}
		return
	}
	if t.state != TableState_Running {
		panic("logic error")
	}
	// dirty sinkers
	if t.watermark.EQ(&iter.to) {
		if len(iter.sinkers) != 1 {
			panic("logic error")
		}
		sinker := iter.sinkers[0]
		var errCode int
		var errMsg string
		if iter.err[0] != nil {
			sinker.err = iter.err[0]
			errCode = toErrorCode(iter.err[0])
			errMsg = iter.err[0].Error()
		} else {
			sinker.watermark = iter.to
		}
		//TODO: async
		sinker.watermarkUpdater.Update(sinker.watermark, errCode, errMsg, t.tableID, t.accountID, sinker.indexName)
		t.state = TableState_Finished
		return
	}
	// all sinkers
	if t.watermark.LT(&iter.to) {
		panic("logic error")
	}
	t.state = TableState_Finished
	t.watermark = iter.to
	for i, sinker := range iter.sinkers {
		var errCode int
		var errMsg string
		if iter.err[i] != nil {
			sinker.err = iter.err[i]
			errCode = toErrorCode(iter.err[i])
			errMsg = iter.err[i].Error()
		} else {
			sinker.watermark = iter.to
		}
		//TODO: async
		sinker.watermarkUpdater.Update(sinker.watermark, errCode, errMsg, t.tableID, t.accountID, sinker.indexName)
	}
}

func (t *TableInfo_2) getNewSinkersLocked() *SinkerEntry {
	for _, sinker := range t.sinkers {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.watermark.LE(&t.watermark) {
			return sinker
		}
	}
	return nil
}
