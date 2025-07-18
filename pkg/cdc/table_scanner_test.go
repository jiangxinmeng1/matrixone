// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func TestGetTableScanner(t *testing.T) {
	gostub.Stub(&getSqlExecutor, func(cnUUID string) executor.SQLExecutor {
		return &mock_executor.MockSQLExecutor{}
	})
	assert.NotNil(t, GetTableDetector("cnUUID"))
}

func TestTableScanner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bat := batch.New([]string{"tblId", "tblName", "dbId", "dbName", "createSql", "accountId"})
	bat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"tblName"}, nil)
	bat.Vecs[2] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[3] = testutil.MakeVarcharVector([]string{"dbName"}, nil)
	bat.Vecs[4] = testutil.MakeVarcharVector([]string{"createSql"}, nil)
	bat.Vecs[5] = testutil.MakeUint32Vector([]uint32{1}, nil)
	bat.SetRowCount(1)
	res := executor.Result{
		Mp:      testutil.TestUtilMp,
		Batches: []*batch.Batch{bat},
	}

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(res, nil)

	td := &TableDetector{
		Mutex:                sync.Mutex{},
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]func(map[uint32]TblMap)),
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		exec:                 mockSqlExecutor,
	}

	td.Register("id1", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) {})
	assert.Equal(t, 1, len(td.Callbacks))
	td.Register("id2", 2, []string{"db2"}, []string{"tbl2"}, func(mp map[uint32]TblMap) {})
	assert.Equal(t, 2, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))

	td.Register("id3", 1, []string{"db1"}, []string{"tbl1"}, func(mp map[uint32]TblMap) {})
	assert.Equal(t, 3, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))
	assert.Equal(t, 2, len(td.SubscribedDbNames["db1"]))
	assert.Equal(t, []string{"id1", "id3"}, td.SubscribedDbNames["db1"])

	td.UnRegister("id1")
	assert.Equal(t, 2, len(td.Callbacks))
	assert.Equal(t, 2, len(td.SubscribedAccountIds))
	assert.Equal(t, 1, len(td.SubscribedDbNames["db1"]))
	assert.Equal(t, []string{"id3"}, td.SubscribedDbNames["db1"])

	td.UnRegister("id2")
	assert.Equal(t, 1, len(td.Callbacks))
	assert.Equal(t, 1, len(td.SubscribedAccountIds))

	td.UnRegister("id3")
	assert.Equal(t, 0, len(td.Callbacks))
	assert.Equal(t, 0, len(td.SubscribedAccountIds))
	assert.Equal(t, 0, len(td.SubscribedDbNames))

	td.Register("id4", 1, []string{"db4"}, []string{"tbl4"}, func(mp map[uint32]TblMap) {})
	assert.Equal(t, 1, len(td.Callbacks))
	assert.Equal(t, 1, len(td.SubscribedAccountIds))

	err := td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(td.Mp))

	mockSqlExecutor.EXPECT().Exec(
		gomock.Any(),
		CDCSQLBuilder.CollectTableInfoSQL("1", "'db4'", "'tbl4'"),
		executor.Options{},
	).Return(executor.Result{}, moerr.NewInternalErrorNoCtx("mock error")).AnyTimes()

	err = td.scanTable()
	assert.Error(t, err)
	assert.Equal(t, 1, len(td.Mp))

	td.UnRegister("id4")
	assert.Equal(t, 0, len(td.Callbacks))
	assert.Equal(t, 0, len(td.SubscribedAccountIds))

	err = td.scanTable()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(td.Mp))
}

func Test_CollectTableInfoSQL(t *testing.T) {
	var builder cdcSQLBuilder
	sql := builder.CollectTableInfoSQL("1,2,3", "*", "*")
	sql = strings.ToUpper(sql)
	t.Log(sql)
	expected := "SELECT  REL_ID,  RELNAME,  RELDATABASE_ID,  " +
		"RELDATABASE,  REL_CREATESQL,  ACCOUNT_ID " +
		"FROM `MO_CATALOG`.`MO_TABLES` " +
		"WHERE  ACCOUNT_ID IN (1,2,3)  AND RELKIND = 'R'  " +
		"AND RELDATABASE NOT IN ('INFORMATION_SCHEMA','MO_CATALOG','MO_DEBUG','MO_TASK','MYSQL','SYSTEM','SYSTEM_METRICS')"
	assert.Equal(t, expected, sql)

	sql = builder.CollectTableInfoSQL("0", "'source_db'", "'orders'")
	expected = "SELECT  REL_ID,  RELNAME,  RELDATABASE_ID,  RELDATABASE,  REL_CREATESQL,  ACCOUNT_ID FROM `MO_CATALOG`.`MO_TABLES` WHERE  ACCOUNT_ID IN (0)  AND RELDATABASE IN ('SOURCE_DB')  AND RELNAME IN ('ORDERS')  AND RELKIND = 'R'  AND RELDATABASE NOT IN ('INFORMATION_SCHEMA','MO_CATALOG','MO_DEBUG','MO_TASK','MYSQL','SYSTEM','SYSTEM_METRICS')"
	assert.Equal(t, strings.ToUpper(expected), strings.ToUpper(sql))
}
