package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestAuditRaftDuplicateTargetAfterSingletonRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	configs := []Config{getServiceTestConfig(), getServiceTestConfig(), getServiceTestConfig()}
	seeds := []string{
		getTestGossipAddress(configs[0].GossipPort),
		getTestGossipAddress(configs[1].GossipPort),
		getTestGossipAddress(configs[2].GossipPort),
	}
	services := make([]*Service, 0, len(configs))
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		configs[i].DataDir = t.TempDir()
		cfg := configs[i]
		service, err := NewService(cfg, newFS(), nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, service.Close())
			vfs.ReportLeakedFD(cfg.FS, t)
		}()
		services = append(services, service)
	}
	a, b, c := services[0], services[1], services[2]
	require.NoError(t, a.store.startReplica(1, 1,
		map[uint64]dragonboat.Target{1: a.ID()}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var membership *dragonboat.Membership
	waitMembers := func(n int) {
		require.Eventually(t, func() bool {
			var err error
			membership, err = a.store.nh.SyncGetShardMembership(ctx, 1)
			return err == nil && len(membership.Nodes) == n
		}, 5*time.Second, 20*time.Millisecond)
	}
	waitMembers(1)
	require.NoError(t, a.store.addReplica(1, 2, b.ID(), membership.ConfigChangeID))
	require.NoError(t, b.store.startReplica(1, 2, nil, true))
	waitMembers(2)
	err := a.store.addReplica(1, 3, b.ID(), membership.ConfigChangeID)
	t.Logf("second add with same target returned %v", err)
	require.ErrorIs(t, err, dragonboat.ErrRejected)
	membership, err = a.store.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	require.Len(t, membership.Nodes, 2)
	// The unused third store is a valid target for the same member ID.
	require.NoError(t, a.store.addReplica(1, 3, c.ID(), membership.ConfigChangeID))
	require.NoError(t, c.store.startReplica(1, 3, nil, true))
	waitMembers(3)
}
