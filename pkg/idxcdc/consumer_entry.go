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

package idxcdc

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type SinkerState int8

const (
	SinkerState_Invalid SinkerState = iota
	SinkerState_Running
	SinkerState_Finished
)
type SinkerEntry struct {
	tableInfo  *TableInfo_2
	indexName  string
	inited     atomic.Bool
	consumer     Consumer
	consumerType int8
	watermark  types.TS
	err        error
}

func NewSinkerEntry(
	cnUUID string,
	tableDef *plan.TableDef,
	tableInfo *TableInfo_2,
	sinkerConfig *ConsumerInfo,
	watermark types.TS,
	iterationErr error,
) (*SinkerEntry, error) {
	consumer, err := NewConsumer(cnUUID, tableDef, sinkerConfig)
	if err != nil {
		return nil, err
	}
	sinkerEntry := &SinkerEntry{
		tableInfo:  tableInfo,
		indexName:  sinkerConfig.IndexName,
		consumer:     consumer,
		consumerType: sinkerConfig.ConsumerType,
		watermark:  watermark,
		err:        iterationErr,
	}
	sinkerEntry.init()
	return sinkerEntry, nil
}

func (sinkerEntry *SinkerEntry) init() {
	if sinkerEntry.watermark.IsEmpty() {
		maxWatermark:=sinkerEntry.tableInfo.GetMaxWaterMark()
		if maxWatermark.IsEmpty(){
			timeStamp:=sinkerEntry.tableInfo.exec.txnEngine.LatestLogtailAppliedTime()
			maxWatermark=types.TimestampToTS(timeStamp)
		}
		iter:=&Iteration{
			table:   sinkerEntry.tableInfo,
			sinkers: []*SinkerEntry{sinkerEntry},
			to:      maxWatermark,
			from:    types.TS{},
		}
		sinkerEntry.tableInfo.exec.worker.Submit(iter)
	}else {
		sinkerEntry.inited.Store(true)
	}
}

// TODO
func (sinkerEntry *SinkerEntry) PermanentError() bool {
	return false
}
