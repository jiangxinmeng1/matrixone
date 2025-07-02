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

package cdc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type DataRetriever interface {
	//SNAPSHOT = 0, TAIL = 1
	//TAIL can use INSERT, SNAPSHOT need REPLACE INTO
	//in SNAPSHOT, deleteBatch is nil
	  Next() (insertBatch *AtomicBatch, deleteBatch *AtomicBatch, noMoreData bool, err error)
	  UpdateWatermarker(executor.TxnExecutor,executor.StatementOption)error
	  GetDataType() int8
  }
type CDCData struct {
	insertBatch *AtomicBatch
	deleteBatch *AtomicBatch
	noMoreData  bool
	err         error
}

const (
	CDCDataType_Snapshot = iota
	CDCDataType_Tail
)

type DataRetrieverImpl struct {
	*SinkerEntry
	*Iteration
	txn          client.TxnOperator
	insertDataCh <-chan CDCData
	ackChan      chan<- struct{}
	typ          int8
}

func (r *DataRetrieverImpl) Next() (insertBatch, deleteBatch *AtomicBatch, noMoreDate bool, err error) {
	data := <-r.insertDataCh
	defer func() {
		r.ackChan <- struct{}{}
	}()
	return data.insertBatch, data.deleteBatch, data.noMoreData, data.err
}

func (r *DataRetrieverImpl) UpdateWatermark(exec executor.TxnExecutor, opts executor.StatementOption) error {
	if r.typ == CDCDataType_Snapshot {
		return nil
	}
	updateWatermarkSQL := CDCSQLBuilder.IndexUpdateWatermarkSQL(
		r.tableInfo.accountID,
		r.tableInfo.tableID,
		r.indexName,
		r.to,
		types.TS{},
		0,
		"",
	)
	_, err := exec.Exec(updateWatermarkSQL, opts)
	return err
}

func (r *DataRetrieverImpl) GetType() int8 {
	return r.typ
}
