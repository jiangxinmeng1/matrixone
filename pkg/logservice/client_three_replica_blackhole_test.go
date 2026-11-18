package logservice

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestThreeReplicaClientRecoversFromBlackholedConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := make([]bool, len(services))
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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var leader uint64
	require.Eventually(t, func() bool {
		id, _, ok, err := s1.store.nh.GetLeaderID(1)
		leader = id
		return err == nil && ok && id != 0
	}, 10*time.Second, 20*time.Millisecond)
	cfg := ClientConfig{
		LogShardID: 1, TNReplicaID: 100,
		ServiceAddresses: []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr(), cfg3.LogServiceServiceAddr()},
		MaxMessageSize:   defaultMaxMessageSize,
	}
	base, err := NewClient(ctx, "", cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, base.Close()) }()
	client := base.(*managedClient)
	record := base.GetLogRecord(16)
	copy(record.Payload(), "blackhole-record")
	lsn, err := client.AppendWAL(ctx, record)
	require.NoError(t, err)

	proxy, drop, cleanup := startDroppingResponseProxy(t, configs[leader-1].LogServiceServiceAddr())
	defer cleanup()
	pinProxy := func() {
		drop.Store(false)
		client.resetClient()
		bound, err := connectToLogServiceAddress(ctx, "", proxy, client.cfg)
		require.NoError(t, err)
		client.client = bound
		drop.Store(true)
	}
	pinProxy()
	started := time.Now()
	records, _, err := base.Read(ctx, lsn, 1<<20)
	require.NoError(t, err)
	t.Logf("Read recovered in %s", time.Since(started))
	require.Less(t, time.Since(started), 20*time.Second)
	found := false
	for _, got := range records {
		if got.Lsn == lsn {
			require.True(t, bytes.Equal(record.Data, got.Data))
			found = true
		}
	}
	require.True(t, found)

	pinProxy()
	started = time.Now()
	_, _, err = base.GetWALTruncate(ctx)
	require.Error(t, err)
	t.Logf("GetWALTruncate failed after %s", time.Since(started))
	require.Less(t, time.Since(started), 20*time.Second)
	started = time.Now()
	_, _, err = base.GetWALTruncate(ctx)
	require.NoError(t, err)
	t.Logf("GetWALTruncate recovered in %s", time.Since(started))
	require.Less(t, time.Since(started), 5*time.Second)

	pinProxy()
	truncateResult := make(chan error, 1)
	go func() { truncateResult <- base.TruncateWAL(ctx, lsn, 1) }()
	survivor := int(leader % 3)
	require.Eventually(t, func() bool {
		state, err := services[survivor].store.getWALTruncate(ctx, 1)
		return err == nil && state.psn == lsn && state.dsn == 1
	}, 5*time.Second, 20*time.Millisecond, "truncate must commit before stopping the leader")
	dead := int(leader - 1)
	require.NoError(t, services[dead].Close())
	closed[dead] = true
	firstErr := <-truncateResult
	require.Error(t, firstErr, "the dropped response must leave the first attempt uncertain")
	t.Logf("committed truncate returned an uncertain result after leader close: %v", firstErr)
	require.Eventually(t, func() bool {
		id, _, ok, err := services[survivor].store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0 && id != leader
	}, 10*time.Second, 20*time.Millisecond)
	psn, dsn, err := base.GetWALTruncate(ctx)
	require.NoError(t, err)
	require.Equal(t, lsn, psn)
	require.Equal(t, uint64(1), dsn)
	require.True(t, moerr.IsMoErrCode(base.TruncateWAL(ctx, lsn, 1), moerr.ErrInvalidTruncateLsn),
		"repeating a committed truncate must report the existing watermark")
}
