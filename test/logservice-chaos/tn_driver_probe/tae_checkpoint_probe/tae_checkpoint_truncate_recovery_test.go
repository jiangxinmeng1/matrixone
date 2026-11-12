// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tae_checkpoint_probe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

// Run only against an isolated, persisted TAE directory and its original LogService shard.
func TestAdvanceTruncateAfterCheckpointCrash(t *testing.T) {
	dir, addresses := os.Getenv("MO_CHAOS_TAE_DIR"), os.Getenv("MO_CHAOS_ADDRS")
	if dir == "" || addresses == "" {
		t.Skip("requires the persisted TAE directory and LogService addresses")
	}
	beforeRows, err := strconv.Atoi(os.Getenv("MO_CHAOS_EXPECTED_ROWS"))
	require.NoError(t, err)
	require.Positive(t, beforeRows)
	const sid = "tae-checkpoint-truncate-recovery"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cfg := logservice.ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(addresses, ",")}
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		return logservice.NewClient(ctx, sid, cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, err := factory()
	require.NoError(t, err)
	defer client.Close()
	oldPSN, oldDSN, err := client.GetWALTruncate(ctx)
	require.NoError(t, err)
	fmt.Printf("truncate_before psn=%d dsn=%d\n", oldPSN, oldDSN)
	opts := config.WithLongScanAndCKPOpts(nil)
	options.WithWalClientFactory(factory)(opts)
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	defer func() { require.NoError(t, tae.Close()) }()
	txn, database := tae.GetTestDB()
	it := database.MakeRelationIt()
	var schema *catalog.Schema
	for it.Valid() {
		var ok bool
		schema, ok = it.GetRelation().Schema(false).(*catalog.Schema)
		require.True(t, ok)
		it.Next()
	}
	require.NoError(t, it.GetError())
	require.NoError(t, it.Close())
	require.NotNil(t, schema)
	require.NoError(t, txn.Commit(ctx))
	tae.BindSchema(schema)
	tae.CheckRowsByScan(beforeRows, false)
	batch := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 10, beforeRows)
	tae.DoAppend(batch)
	batch.Close()
	tae.CheckRowsByScan(beforeRows+10, false)
	require.NoError(t, tae.DB.ForceCheckpoint(ctx, tae.TxnMgr.Now()))
	checkpoint := tae.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, checkpoint)
	fmt.Printf("truncate_new_checkpoint rows=%d checkpoint_lsn=%d wal_lsn=%d\n",
		beforeRows+10, checkpoint.LSN(), tae.Wal.GetLSNWatermark())
	deadline := time.Now().Add(30 * time.Second)
	for {
		psn, dsn, err := client.GetWALTruncate(ctx)
		require.NoError(t, err)
		if psn > oldPSN && dsn > oldDSN {
			fmt.Printf("truncate_after psn=%d dsn=%d\n", psn, dsn)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("truncate did not advance after new checkpoint: old=(%d,%d) current=(%d,%d)", oldPSN, oldDSN, psn, dsn)
		}
		time.Sleep(100 * time.Millisecond)
	}
	tae.Restart(ctx)
	tae.CheckRowsByScan(beforeRows+10, false)
}
