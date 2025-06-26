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
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type SinkerConfig struct {
	sinkerType string
	accountID  int32
	tableID    uint64
	dbID       uint64
	indexName  string
}

func NewSinker(
	cnUUID string,
	dbTblInfo *cdc.DbTableInfo,
	tableDef *plan.TableDef,
	sinkerConfig *SinkerConfig,
) cdc.Sinker {
	panic("todo")
}

type WatermarkUpdater interface {
	Update(watermark types.TS, errCode int, errMsg string, tableID uint64, accountID int32, indexName string) error
	Insert(tableID uint64, accountID int32, indexName string) error
	Delete(tableID uint64, accountID int32, indexName string) error
}

type SinkerEntry struct {
	tableInfo        *TableInfo_2
	sinker           cdc.Sinker
	sinkerType       string
	watermark        types.TS
	err              error
	watermarkUpdater WatermarkUpdater
}

func NewSinkerEntry(
	cnUUID string,
	dbTblInfo *cdc.DbTableInfo,
	tableDef *plan.TableDef,
	tableInfo *TableInfo_2,
	sinkerConfig *SinkerConfig,
	watermarkUpdater WatermarkUpdater,
) *SinkerEntry {
	sinker := NewSinker(cnUUID, dbTblInfo, tableDef, sinkerConfig)
	sinkerEntry := &SinkerEntry{
		tableInfo:        tableInfo,
		sinker:           sinker,
		sinkerType:       sinkerConfig.sinkerType,
		watermarkUpdater: watermarkUpdater,
	}
	sinkerEntry.init()
	return sinkerEntry
}

func (sinkerEntry *SinkerEntry) init() {
	/*
		1. sink snapshot
	*/
}
