// Copyright 2021 - 2022 Matrix Origin
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

package live_dump

import (
	"strings"
	"testing"
)

func TestSQLString(t *testing.T) {
	got := sqlString(`dump-table -o "a'b"`)
	want := `'dump-table -o "a''b"'`
	if got != want {
		t.Fatalf("sqlString() = %q, want %q", got, want)
	}
}

func TestTableDumpDir(t *testing.T) {
	tbl := tableInfo{
		accountID: 7,
		dbID:      9001,
		tableID:   272535,
	}
	got := tableDumpDir("./dump_out/", tbl)
	want := "./dump_out/tables/account_7/db_9001/table_272535"
	if got != want {
		t.Fatalf("tableDumpDir() = %q, want %q", got, want)
	}
}

func TestParseSnapshotTS(t *testing.T) {
	tests := []struct {
		name string
		resp string
		want string
	}{
		{
			name: "message only",
			resp: "msg: 1782184172426822147-0",
			want: "1782184172426822147-0",
		},
		{
			name: "run factory suffix",
			resp: "msg: 1782184172426822147-0get-ts",
			want: "1782184172426822147-0",
		},
		{
			name: "console string",
			resp: "\nmsg: 1782184172426822147-0get-ts\n\n",
			want: "1782184172426822147-0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSnapshotTS(tt.resp)
			if err != nil {
				t.Fatalf("parseSnapshotTS() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseSnapshotTS() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInspectResponseError(t *testing.T) {
	if err := inspectResponseError("\nmsg: apply-table-data\n\n"); err != nil {
		t.Fatalf("inspectResponseError() unexpected error = %v", err)
	}
	resp := "\nmsg: Failed\n\npanic in inspect dn"
	err := inspectResponseError(resp)
	if err == nil {
		t.Fatalf("inspectResponseError() expected error")
	}
	if got := err.Error(); got != strings.TrimSpace(resp) {
		t.Fatalf("inspectResponseError() = %q, want %q", got, strings.TrimSpace(resp))
	}
}
