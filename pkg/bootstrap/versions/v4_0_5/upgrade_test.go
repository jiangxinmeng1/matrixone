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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
)

func TestIcebergOrphanCleanupTenantUpgradeEntries(t *testing.T) {
	if len(tenantUpgEntries) != 4 {
		t.Fatalf("expected 4 Iceberg tenant upgrades, got %d", len(tenantUpgEntries))
	}
	allocator := tenantUpgEntries[0]
	if allocator.UpgType != versions.MODIFY_COLUMN || allocator.TableName != "mo_iceberg_catalogs" {
		t.Fatalf("unexpected catalog allocator upgrade: %+v", allocator)
	}
	allocatorSQL := strings.ToLower(allocator.UpgSql)
	for _, want := range []string{"alter table", "modify catalog_id", "auto_increment"} {
		if !strings.Contains(allocatorSQL, want) {
			t.Fatalf("catalog allocator upgrade SQL missing %q: %s", want, allocator.UpgSql)
		}
	}
	if strings.Contains(allocatorSQL, "drop primary key") {
		t.Fatalf("catalog allocator upgrade should preserve the account-scoped primary key: %s", allocator.UpgSql)
	}
	for _, entry := range tenantUpgEntries[1:] {
		if entry.UpgType != versions.ADD_COLUMN {
			t.Fatalf("%s should be ADD_COLUMN", entry.TableName)
		}
		lower := strings.ToLower(entry.UpgSql)
		for _, want := range []string{"alter table", "mo_catalog.mo_iceberg_orphan_files", "add column"} {
			if !strings.Contains(lower, want) {
				t.Fatalf("orphan cleanup upgrade SQL missing %q: %s", want, entry.UpgSql)
			}
		}
		if strings.Contains(lower, "drop ") {
			t.Fatalf("orphan cleanup upgrade SQL must not drop objects: %s", entry.UpgSql)
		}
	}
}

func TestIcebergOrphanCleanupVersionHandleMetadata(t *testing.T) {
	meta := Handler.Metadata()
	if meta.Version != "4.0.5" ||
		meta.MinUpgradeVersion != "4.0.4" ||
		meta.UpgradeCluster != versions.Yes ||
		meta.UpgradeTenant != versions.Yes {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if meta.VersionOffset != uint32(len(tenantUpgEntries)+len(clusterUpgEntries)) {
		t.Fatalf("unexpected version offset: %+v", meta)
	}
	err := Handler.HandleCreateFrameworkDeps(nil)
	if err == nil || !strings.Contains(err.Error(), "Only v1.2.0") {
		t.Fatalf("unexpected framework deps error: %v", err)
	}
}

func TestDaemonTaskOwnershipFenceClusterUpgradeEntries(t *testing.T) {
	if len(clusterUpgEntries) != 3 {
		t.Fatalf("expected 3 daemon ownership upgrades, got %d", len(clusterUpgEntries))
	}
	for i, name := range []string{"task_epoch", "task_fence_token", "task_fence_expire_at"} {
		entry := clusterUpgEntries[i]
		if entry.UpgType != versions.ADD_COLUMN {
			t.Fatalf("%s should be ADD_COLUMN", name)
		}
		lower := strings.ToLower(entry.UpgSql)
		for _, want := range []string{"alter table", "mo_task.sys_daemon_task", "add column", name} {
			if !strings.Contains(lower, want) {
				t.Fatalf("%s upgrade SQL missing %q: %s", name, want, entry.UpgSql)
			}
		}
	}
}
