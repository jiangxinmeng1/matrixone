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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TODO: if insert or delete rollback
type WatermarkUpdater interface {
	Update(watermark types.TS, errCode int, errMsg string, tableID uint64, accountID int32, indexName string) error
	Insert(tableID uint64, accountID int32, indexName string) error
	Delete(tableID uint64, accountID int32, indexName string) error
	Select(tableID uint64, accountID int32, indexName string) (watermark types.TS, errorCode int, errMsg string, err error)
}

/*
1. scan table
2. iteration finished
*/

func InsertAsyncIndexLogTableState(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	tableName string,
	dbName string,
	indexName string,
	sinkerConfig *SinkerInfo,
){}

func DeleteAsyncIndexLogTableState(
	ctx context.Context,
	accountIDs []int32,
	tableIDs []uint64,
	indexNames []string,
){}

func UpdateAsyncIndexLogDropAt(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	indexName string,
	deleteAt types.TS,
){}

func UpdateAsyncIndexLogTableState(
	ctx context.Context,
	accountIDs []uint32,
	tableIDs []uint64,
	indexNames []string,
)

func UpdateAsyncIndexLogIterationResult(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	indexNames []string,
	newWatermark types.TS,
	errorCode []int,
	errorMsgs []string,
)

func InsertAsyncIndexIterations(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	indexNames []string,
	fromTS types.TS,
	toTS types.TS,
	errorCode []int,
	errorMsgs []string,
	startAt types.TS,
	endAt types.TS,
)

func DeleteAsyncIndexIterations(
	ctx context.Context,
	accountIDs []uint32,
	tableIDs []uint64,
	indexNames []string,
	fromTSes []types.TS,
	toTSes []types.TS,
)