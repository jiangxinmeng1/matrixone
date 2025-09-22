// Copyright 2021 Matrix Origin
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

package iscp

import "fmt"

const (
	SelectMOISCPLogByTableSqlTemplate = `SELECT drop_at,watermark, job_id from mo_catalog.mo_iscp_log WHERE ` +
		`account_id = %d ` +
		`AND table_id = %d ` +
		`AND job_name = '%s'`
)

const (
	SelectMOISCPLogByTableSqlTemplate_Idx = iota

	ISCPTemplateCount
)

var ISCPSQLTemplates = [ISCPTemplateCount]struct {
	SQL         string
	OutputAttrs []string
}{
	SelectMOISCPLogByTableSqlTemplate_Idx: {
		SQL: SelectMOISCPLogByTableSqlTemplate,
		OutputAttrs: []string{
			"drop_at",
			"watermark",
			"job_id",
		},
	},
}

func ISCPLogSelectByTableSQL(
	accountID uint32,
	tableID uint64,
	jobName string,
) string {
	return fmt.Sprintf(
		ISCPSQLTemplates[SelectMOISCPLogByTableSqlTemplate_Idx].SQL,
		accountID,
		tableID,
		jobName,
	)
}
