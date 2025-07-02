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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// 1. init sinker
// 2. dirty sinkers
// 3. all sinkers
type Iteration struct {
	ctx     context.Context
	table   *TableInfo_2
	sinkers []*SinkerEntry
	from    types.TS
	to      types.TS
	err     []error
}

func (iter *Iteration) Run() {
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
	sinker := make([]Consumer, 0)
	for _, sinkerEntry := range iter.sinkers {
		sinker = append(sinker, sinkerEntry.consumer)
	}
	iter.err = CollectChanges_2(
		iter.ctx,
		table,
		iter.from,
		iter.to,
		sinker,
		false,
		iter.table.exec.packer,
		iter.table.exec.mp,
	)
}
