package tae_checkpoint_probe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestConcurrentTxnCheckpointAcrossLeaderFailure(t *testing.T) {
	const sid = "tae-concurrent-checkpoint-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	addresses := os.Getenv("MO_CHAOS_ADDRS")
	var factory logservicedriver.LogServiceClientFactory
	if addresses != "" {
		require.Equal(t, "198.18.51.10:60601,198.18.51.11:60611,198.18.51.12:60621", addresses)
		cfg := logservice.ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(addresses, ",")}
		factory = func() (logservice.Client, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			return logservice.NewClient(ctx, sid, cfg)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	opts := config.WithLongScanAndCKPOpts(nil)
	if factory != nil {
		options.WithWalClientFactory(factory)(opts)
	}
	tae := testutil.NewTestEngine(ctx, "tae_checkpoint_concurrent_probe", t, opts)
	defer func() { require.NoError(t, tae.Close()) }()
	schema := catalog.MockSchemaAll(18, 2)
	tae.BindSchema(schema)
	initial := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 50, 0)
	defer initial.Close()
	tae.CreateRelAndAppend2(initial, true)
	tae.CheckRowsByScan(50, false)

	type result struct {
		commits []time.Time
		err     error
	}
	stop := make(chan struct{})
	done := make(chan result, 1)
	go func() {
		out := result{}
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				done <- out
				return
			default:
			}
			batch := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 10, 50+i*10)
			txn, err := tae.StartTxn(nil)
			if err != nil {
				batch.Close()
				out.err = err
				done <- out
				return
			}
			database, err := txn.GetDatabase(testutil.DefaultTestDB)
			if err != nil {
				_ = txn.Rollback(ctx)
				batch.Close()
				out.err = err
				done <- out
				return
			}
			rel, err := database.GetRelationByName(schema.Name)
			if err == nil {
				err = rel.Append(ctx, batch)
			}
			if err == nil {
				err = txn.Commit(ctx)
			} else {
				_ = txn.Rollback(ctx)
			}
			batch.Close()
			if err != nil {
				out.err = err
				done <- out
				return
			}
			out.commits = append(out.commits, time.Now())
			fmt.Printf("tae_writer_commit count=%d rows=%d wal_lsn=%d\n", len(out.commits), 50+len(out.commits)*10, tae.Wal.GetLSNWatermark())
			time.Sleep(30 * time.Millisecond)
		}
		done <- out
	}()
	var stopOnce, waitOnce sync.Once
	var written result
	stopWriter := func() {
		stopOnce.Do(func() { close(stop) })
		waitOnce.Do(func() { written = <-done })
	}
	defer stopWriter()

	if ready, release := os.Getenv("MO_CHAOS_READY"), os.Getenv("MO_CHAOS_RELEASE"); ready != "" {
		require.NotEmpty(t, release)
		require.NoError(t, os.WriteFile(ready, []byte("checkpoint-ready\n"), 0o644))
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if _, err := os.Stat(release); err == nil {
				break
			}
			if time.Now().After(deadline) {
				stopWriter()
				t.Fatal("checkpoint release timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	start := time.Now()
	checkpointCtx, checkpointCancel := context.WithTimeout(ctx, 2*time.Minute)
	err := tae.DB.ForceCheckpoint(checkpointCtx, tae.TxnMgr.Now())
	checkpointCancel()
	end := time.Now()
	stopWriter()
	require.NoError(t, written.err)
	require.NoError(t, err)
	checkpoint := tae.BGCheckpointRunner.MaxIncrementalCheckpoint()
	require.NotNil(t, checkpoint)
	require.True(t, checkpoint.IsFinished())
	require.NotEmpty(t, tae.BGCheckpointRunner.GetCheckpointMetaFiles())
	during := 0
	for _, at := range written.commits {
		if !at.Before(start) && !at.After(end) {
			during++
		}
	}
	fmt.Printf("tae_overlap checkpoint_lsn=%d commits=%d during=%d duration_ms=%d meta_files=%d\n",
		checkpoint.LSN(), len(written.commits), during, end.Sub(start).Milliseconds(), len(tae.BGCheckpointRunner.GetCheckpointMetaFiles()))
	require.Greater(t, during, 0, "checkpoint and transaction commits did not overlap")
	total := 50 + len(written.commits)*10
	tae.CheckRowsByScan(total, false)
	// A post-checkpoint commit must be recovered from WAL after the file is loaded.
	post := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 10, total)
	tae.DoAppend(post)
	post.Close()
	total += 10
	fmt.Printf("tae_post_checkpoint rows=%d wal_lsn=%d\n", total, tae.Wal.GetLSNWatermark())
	tae.Restart(ctx)
	tae.CheckRowsByScan(total, false)
	fmt.Printf("tae_reopened rows=%d checkpoint_lsn=%d wal_lsn=%d\n",
		total, tae.BGCheckpointRunner.MaxCheckpoint().LSN(), tae.Wal.GetLSNWatermark())
}

func TestReopenConcurrentCheckpointAfterColdRestart(t *testing.T) {
	dir := os.Getenv("MO_CHAOS_TAE_DIR")
	if dir == "" {
		t.Skip("requires the persisted TAE directory from the remote integration run")
	}
	require.Equal(t, "/tmp/mo-ut-workspace/tae_checkpoint_concurrent_probe/TestConcurrentTxnCheckpointAcrossLeaderFailure", dir)
	expectedRows, err := strconv.Atoi(os.Getenv("MO_CHAOS_EXPECTED_ROWS"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, expectedRows, 50)
	addresses := os.Getenv("MO_CHAOS_ADDRS")
	require.Equal(t, "198.18.51.10:60601,198.18.51.11:60611,198.18.51.12:60621", addresses)
	const sid = "tae-concurrent-checkpoint-reopen-probe"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	cfg := logservice.ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: strings.Split(addresses, ",")}
	factory := func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		return logservice.NewClient(ctx, sid, cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	opts := config.WithLongScanAndCKPOpts(nil)
	options.WithWalClientFactory(factory)(opts)
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	defer func() { require.NoError(t, tae.Close()) }()
	txn, database := tae.GetTestDB()
	it := database.MakeRelationIt()
	var schemas []*catalog.Schema
	for it.Valid() {
		schema, ok := it.GetRelation().Schema(false).(*catalog.Schema)
		require.True(t, ok)
		schemas = append(schemas, schema)
		it.Next()
	}
	require.NoError(t, it.GetError())
	require.NoError(t, it.Close())
	require.Len(t, schemas, 1)
	schema := schemas[0]
	require.NoError(t, txn.Commit(ctx))
	tae.BindSchema(schema)
	tae.CheckRowsByScan(expectedRows, false)
	checkpoint := tae.BGCheckpointRunner.MaxCheckpoint()
	require.NotNil(t, checkpoint)
	fmt.Printf("tae_cold_reopened rows=%d checkpoint_lsn=%d wal_lsn=%d\n",
		expectedRows, checkpoint.LSN(), tae.Wal.GetLSNWatermark())
	post := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 10, expectedRows)
	tae.DoAppend(post)
	post.Close()
	tae.Restart(ctx)
	tae.CheckRowsByScan(expectedRows+10, false)
	fmt.Printf("tae_cold_after_write rows=%d checkpoint_lsn=%d wal_lsn=%d\n",
		expectedRows+10, tae.BGCheckpointRunner.MaxCheckpoint().LSN(), tae.Wal.GetLSNWatermark())
}
