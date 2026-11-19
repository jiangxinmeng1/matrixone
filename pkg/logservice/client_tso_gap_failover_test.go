package logservice

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestThreeReplicaClientReadsAcrossTSOGapAfterLeaderFailure(t *testing.T) {
	runThreeReplicaClientReadsAcrossTSOGapAfterLeaderFailure(t, false)
}

func TestThreeReplicaDiscoveryClientReadsAcrossTSOGapAfterLeaderFailure(t *testing.T) {
	runThreeReplicaClientReadsAcrossTSOGapAfterLeaderFailure(t, true)
}

func runThreeReplicaClientReadsAcrossTSOGapAfterLeaderFailure(t *testing.T, discover bool) {
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
		MaxMessageSize: defaultMaxMessageSize,
	}
	if discover {
		// The discovery endpoint survives the first leader failure, as a
		// reverse proxy in the TN deployment would.
		cfg.DiscoveryAddress = configs[leader%3].LogServiceServiceAddr()
		require.Eventually(t, func() bool {
			_, ready, err := GetShardInfo("", cfg.DiscoveryAddress, cfg.LogShardID)
			return err == nil && ready
		}, 10*time.Second, 20*time.Millisecond)
	} else {
		cfg.ServiceAddresses = []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr(), cfg3.LogServiceServiceAddr()}
	}
	c, err := NewClient(ctx, "", cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	if discover {
		managed := c.(*managedClient)
		leaderAddress := configs[leader-1].LogServiceServiceAddr()
		if managed.client.addr != leaderAddress {
			managed.resetClient()
			managed.client, err = connectToLogServiceAddress(ctx, "", leaderAddress, cfg)
			require.NoError(t, err)
		}
		require.Equal(t, leaderAddress, managed.client.addr)
	}
	appendWAL := func(value string) (uint64, []byte) {
		record := c.GetLogRecord(len(value))
		copy(record.Payload(), value)
		lsn, err := c.(interface {
			AppendWAL(context.Context, LogRecord) (uint64, error)
		}).AppendWAL(ctx, record)
		require.NoError(t, err)
		return lsn, record.Data
	}
	first, firstData := appendWAL("before-tso-gap")
	for i := 0; i < 100; i++ {
		_, err := c.GetTSOTimestamp(ctx, 1)
		require.NoError(t, err)
	}
	second, secondData := appendWAL("after-tso-gap")
	require.GreaterOrEqual(t, second-first, uint64(100))
	dead := int(leader - 1)
	require.NoError(t, services[dead].Close())
	closed[dead] = true
	survivor := int(leader % 3)
	require.Eventually(t, func() bool {
		id, _, ok, err := services[survivor].store.nh.GetLeaderID(1)
		return err == nil && ok && id != 0 && id != leader
	}, 10*time.Second, 20*time.Millisecond)

	records, _, err := c.Read(ctx, first, 1<<20)
	require.NoError(t, err)
	found := make(map[uint64][]byte)
	for _, record := range records {
		if record.Lsn == first || record.Lsn == second {
			found[record.Lsn] = record.Data
		}
	}
	require.True(t, bytes.Equal(firstData, found[first]))
	require.True(t, bytes.Equal(secondData, found[second]))
	require.NoError(t, c.TruncateWAL(ctx, first, 1))
	psn, dsn, err := c.GetWALTruncate(ctx)
	require.NoError(t, err)
	require.Equal(t, first, psn)
	require.Equal(t, uint64(1), dsn)
	records, _, err = c.Read(ctx, first+1, 1<<20)
	require.NoError(t, err)
	foundSecond := false
	for _, record := range records {
		if record.Lsn == second {
			require.True(t, bytes.Equal(secondData, record.Data))
			foundSecond = true
		}
	}
	require.True(t, foundSecond)
	third, thirdData := appendWAL("after-leader-failure")
	require.Greater(t, third, second)
	records, _, err = c.Read(ctx, third, 1<<20)
	require.NoError(t, err)
	foundThird := false
	for _, record := range records {
		if record.Lsn == third {
			require.True(t, bytes.Equal(thirdData, record.Data))
			foundThird = true
		}
	}
	require.True(t, foundThird)
}
