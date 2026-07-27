// Copyright 2026 Matrix Origin
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

package v4_0_5

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upgDaemonTaskEpoch,
	upgDaemonTaskFenceToken,
	upgDaemonTaskFenceExpireAt,
}

var upgDaemonTaskEpoch = addDaemonTaskColumn(
	"task_epoch",
	"bigint unsigned not null default 0",
)

var upgDaemonTaskFenceToken = addDaemonTaskColumn(
	"task_fence_token",
	"varchar(64) not null default ''",
)

var upgDaemonTaskFenceExpireAt = addDaemonTaskColumn(
	"task_fence_expire_at",
	"bigint not null default 0",
)

func addDaemonTaskColumn(name, definition string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MOTaskDB,
		TableName: catalog.MOSysDaemonTask,
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf(
			"alter table %s.%s add column %s %s",
			catalog.MOTaskDB,
			catalog.MOSysDaemonTask,
			name,
			definition,
		),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			info, err := versions.CheckTableColumn(
				txn,
				accountID,
				catalog.MOTaskDB,
				catalog.MOSysDaemonTask,
				name,
			)
			if err != nil {
				return false, err
			}
			return info.IsExits, nil
		},
	}
}
