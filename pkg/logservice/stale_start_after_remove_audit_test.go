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

// A stale HAKeeper StartReplica command can arrive after the other voters have
// removed the target. Check whether Dragonboat starts the old local replica and
// whether that replica can rejoin the authoritative membership.
func TestAuditStaleStartAfterReplicaRemoval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	defer func() {
		for i, service := range services {
			require.NoError(t, service.Close())
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()

	peers := map[uint64]dragonboat.Target{11: s1.ID(), 12: s2.ID(), 13: s3.ID()}
	for i, service := range services {
		require.NoError(t, service.store.startReplica(1, uint64(11+i), peers, false))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitLeader(t, []*store{s1.store, s2.store, s3.store}, 10*time.Second)
	byID := map[uint64]*store{11: s1.store, 12: s2.store, 13: s3.store}
	// Take a offline before removing it. The survivors elect their own leader
	// and commit the configuration change without a participating vote from a.
	require.NoError(t, s1.store.stopReplica(1, 11))
	s1.store.removeMetadata(1, 11)
	var leaderID uint64
	require.Eventually(t, func() bool {
		for _, survivorID := range []uint64{12, 13} {
			id, _, ok, err := byID[survivorID].nh.GetLeaderID(1)
			if err == nil && ok && (id == 12 || id == 13) {
				leaderID = id
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
	leader := byID[leaderID]
	membership, err := leader.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)

	// Have the remaining voters commit removal while a is unable to apply the
	// configuration change.
	require.NoError(t, leader.removeReplica(1, 11, membership.ConfigChangeID))

	membership, err = leader.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	_, stillMember := membership.Nodes[11]
	require.False(t, stillMember)

	startErr := s1.store.startReplica(1, 11, nil, true)
	t.Logf("stale StartReplica returned: %v", startErr)
	if startErr == nil {
		defer func() {
			if err := s1.store.stopReplica(1, 11); err != nil && err != dragonboat.ErrShardNotReady {
				t.Errorf("stop stale replica: %v", err)
			}
		}()
	}

	// The remaining quorum must continue without the removed replica regardless
	// of whether the stale local start is rejected or briefly succeeds.
	require.Eventually(t, func() bool {
		_, _, ok, err := s2.store.nh.GetLeaderID(1)
		return err == nil && ok
	}, 5*time.Second, 50*time.Millisecond)
	current, err := s2.store.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	_, stillMember = current.Nodes[11]
	require.False(t, stillMember)
}
