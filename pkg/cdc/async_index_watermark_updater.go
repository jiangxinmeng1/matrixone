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
	"context"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func ExecWithResult(
	ctx context.Context,
	sql string,
	cnUUID string,
	txn client.TxnOperator,
) (executor.Result, error){
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	return exec.Exec(ctx,sql,opts)
}

func UpdateResult(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tableID uint64,
	accountID uint32,
	indexName string,
	newWatermark types.TS,
	errCode int,
	errMsg string,
) (err error) {
	sql := cdc.CDCSQLBuilder.IndexUpdateWatermarkSQL(
		accountID,
		tableID,
		indexName,
		newWatermark,
		types.TS{},
		errCode,
		errMsg,
	)
	if _,err= ExecWithResult(ctx, sql, cnUUID, txn);err!=nil{
		return err
	}
	
}
