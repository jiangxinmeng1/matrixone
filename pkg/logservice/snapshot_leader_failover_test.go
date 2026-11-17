package logservice

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotImportAfterThreeReplicaLeaderFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := make([]bool, 3)
	defer func() {
		for i, s := range services {
			if !closed[i] {
				require.NoError(t, s.Close())
			}
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID(), 3: s3.ID()}
	for i, s := range services {
		require.NoError(t, s.store.startReplica(1, uint64(i+1), peers, false))
	}
	var leader uint64
	require.Eventually(t, func() bool {
		for _, s := range services {
			id, _, ok, err := s.store.nh.GetLeaderID(1)
			if err == nil && ok && id != 0 {
				leader = id
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := NewClient(ctx, "", ClientConfig{
		LogShardID: 1, TNReplicaID: 100,
		ServiceAddresses: []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr(), cfg3.LogServiceServiceAddr()},
		MaxMessageSize:   defaultMaxMessageSize,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	appendOne := func(i int) (uint64, []byte) {
		record := client.GetLogRecord(8)
		copy(record.Payload(), []byte(fmt.Sprintf("wal-%04d", i)))
		start := time.Now()
		for attempts := 1; ; attempts++ {
			lsn, err := client.(*managedClient).AppendWAL(ctx, record)
			if err == nil {
				if attempts > 1 {
					t.Logf("WAL %d recovered after %d attempts in %s", i, attempts, time.Since(start))
				}
				return lsn, bytes.Clone(record.Data)
			}
			if time.Since(start) >= 20*time.Second || ctx.Err() != nil {
				t.Fatalf("WAL %d failed after %d attempts in %s: %v", i, attempts, time.Since(start), err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for i := 0; i < 10; i++ {
		appendOne(i)
	}
	require.NoError(t, client.Truncate(ctx, 4))
	survivor := int(leader % 3)
	require.NotEqual(t, int(leader-1), survivor)
	require.NoError(t, services[survivor].store.processShardTruncateLog(ctx, 1))
	snapshotIndex := services[survivor].store.snapshotMgr.NewestIndex(1, uint64(survivor+1))
	require.NotZero(t, snapshotIndex)
	written := make(map[uint64][]byte)
	for i := 10; i < 20; i++ {
		lsn, payload := appendOne(i)
		written[lsn] = payload
	}
	dead := int(leader - 1)
	// Pin the existing RPC connection to the node about to stop. Reconnects
	// still use the normal three-address managed client discovery path.
	managed := client.(*managedClient)
	managed.resetClient()
	bound, err := connectToLogServiceAddress(ctx, "", configs[dead].LogServiceServiceAddr(), managed.cfg)
	require.NoError(t, err)
	managed.client = bound
	require.NoError(t, services[dead].Close())
	closed[dead] = true
	readStart := time.Now()
	preFailoverRecords, _, err := client.Read(ctx, snapshotIndex+1, math.MaxUint64)
	require.NoError(t, err)
	t.Logf("read on pre-failover client after leader close took %s", time.Since(readStart))
	require.GreaterOrEqual(t, len(preFailoverRecords), len(written))
	require.Eventually(t, func() bool {
		id, _, ok, err := services[survivor].store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0 && id != leader
	}, 10*time.Second, 20*time.Millisecond)
	for i := 20; i < 40; i++ {
		lsn, payload := appendOne(i)
		written[lsn] = payload
	}
	require.NoError(t, client.TruncateWAL(ctx, snapshotIndex, 10))
	require.Eventually(t, func() bool {
		_ = services[survivor].store.processShardTruncateLog(ctx, 1)
		return services[survivor].store.snapshotMgr.Count(1, uint64(survivor+1)) == 0 &&
			services[survivor].store.shardSnapshotInfo.getTruncatedLsn(1) >= snapshotIndex
	}, 10*time.Second, 100*time.Millisecond)
	records, _, err := client.Read(ctx, snapshotIndex+1, math.MaxUint64)
	require.NoError(t, err)
	checkRecords := func(source string, records []LogRecord) {
		missing := make(map[uint64][]byte, len(written))
		for lsn, payload := range written {
			missing[lsn] = payload
		}
		for _, record := range records {
			if want, ok := missing[record.Lsn]; ok {
				require.True(t, bytes.Equal(want, record.Data), "%s: WAL %d changed", source, record.Lsn)
				delete(missing, record.Lsn)
			}
		}
		require.Empty(t, missing, "%s: acknowledged WAL missing", source)
	}
	checkRecords("client", records)
	for i, s := range services {
		if closed[i] {
			continue
		}
		local, _, err := s.store.queryLog(ctx, 1, snapshotIndex+1, math.MaxUint64)
		require.NoError(t, err)
		checkRecords(fmt.Sprintf("replica-%d", i+1), local)
	}
	psn, dsn, err := client.GetWALTruncate(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshotIndex, psn)
	require.Equal(t, uint64(10), dsn)
	lsn, payload := appendOne(40)
	written[lsn] = payload
	services[dead] = restartService3(t, configs[dead], logger)
	closed[dead] = false
	var lastState walTruncateState
	var lastErr error
	var lastRecordErr error
	var matched int
	catchUp := false
	restartAt := time.Now()
	for time.Since(restartAt) < 30*time.Second && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		state, err := services[dead].store.getWALTruncate(attempt, 1)
		lastState, lastErr = state, err
		if err != nil || state.psn != snapshotIndex || state.dsn != 10 {
			stop()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		local, _, err := services[dead].store.queryLog(attempt, 1, snapshotIndex+1, math.MaxUint64)
		lastRecordErr = err
		stop()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		seen := make(map[uint64]bool, len(written))
		for _, record := range local {
			if want, ok := written[record.Lsn]; ok && bytes.Equal(want, record.Data) {
				seen[record.Lsn] = true
			}
		}
		matched = len(seen)
		if matched == len(written) {
			catchUp = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("restarted replica %d caught up=%v after %s", dead+1, catchUp, time.Since(restartAt))
	if !catchUp {
		for i, s := range services {
			leaderID, term, leaderErr := s.store.leaderState(1)
			state, stateErr := s.store.getWALTruncate(ctx, 1)
			index, indexErr := s.store.read(ctx, 1, indexQuery{})
			t.Logf("replica %d: leader=%d term=%d leaderErr=%v state=%+v stateErr=%v index=%v indexErr=%v",
				i+1, leaderID, term, leaderErr, state, stateErr, index, indexErr)
		}
	}
	require.True(t, catchUp, "restarted replica: state=%+v stateErr=%v readErr=%v matched=%d/%d started=%v metadata=%v",
		lastState, lastErr, lastRecordErr, matched, len(written),
		services[dead].store.startedReplicaIDs(), services[dead].store.getShards())
}
