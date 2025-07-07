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
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// 1. init sinker
// 2. dirty sinkers
// 3. all sinkers
type Iteration struct {
	ctx     context.Context
	table   *TableInfo_2
	txnFN   func() client.TxnOperator
	sinkers []*SinkerEntry
	from    types.TS
	to      types.TS
	err     []error
	startAt time.Time
	endAt   time.Time
}

func (iter *Iteration) Run() {
	txn:=iter.txnFN()
	iter.startAt=time.Now()
	table, err := iter.table.exec.getRelation(
		iter.ctx,
		txn,
		iter.table.dbName,
		iter.table.tableName,
	)
	if err != nil {
		iter.err = make([]error, len(iter.sinkers))
		for i := range iter.sinkers {
			iter.err[i] = err
		}
		return
	}
	defer txn.Commit(iter.ctx)
	iter.err = CollectChanges_2(
		iter.ctx,
		iter,
		table,
		iter.from,
		iter.to,
		iter.sinkers,
		false,
		iter.table.exec.packer,
		iter.table.exec.mp,
	)
	iter.endAt=time.Now()
	err=iter.insertAsyncIndexIterations()
	if err!=nil{
		logutil.Errorf("insert async index iterations failed, err: %v", err)
	}
}

func (iter *Iteration) insertAsyncIndexIterations()error{
	indexNames:=""
	for _,sinker:=range iter.sinkers{
		indexNames= fmt.Sprintf("%s%s, ",indexNames,sinker.indexName)
	}

	errorStr:=""
	for _,err:=range iter.err{
		errorStr= fmt.Sprintf("%s%s, ",errorStr,err.Error())
	}

	sql := cdc.CDCSQLBuilder.AsyncIndexIterationsInsertSQL(
		iter.table.accountID,
		iter.table.tableID,
		indexNames,
		iter.from,
		iter.to,
		errorStr,
		iter.startAt,
		iter.endAt,
	)
	txn:=iter.txnFN()
	_,err:=ExecWithResult(iter.ctx,sql,iter.table.exec.cnUUID,txn)
	if err!=nil{
		return err
	}
	return txn.Commit(iter.ctx)
}
