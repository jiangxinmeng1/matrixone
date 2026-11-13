package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestBootstrapLeaderTakeoverAfterCommandsCommitted(t *testing.T) {
	runHAKeeperClusterTest(t, func(t *testing.T, services []*Service) {
		stores := []*store{services[0].store, services[1].store, services[2].store}
		require.NoError(t, stores[0].setInitialClusterInfo(1, 1, 3, hakeeper.K8SIDRangeEnd+10, nil, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, s := range stores {
			_, err := s.addLogStoreHeartbeat(ctx, s.getHeartbeatMessage())
			require.NoError(t, err)
		}
		_, err := stores[0].addTNStoreHeartbeat(ctx, pb.TNStoreHeartbeat{UUID: "diagnostic-tn"})
		require.NoError(t, err)
		var leader *store
		var leaderIndex int
		var term uint64
		for i, s := range stores {
			ok, currentTerm, err := s.isLeaderHAKeeper()
			require.NoError(t, err)
			if ok {
				leader, leaderIndex, term = s, i, currentTerm
				break
			}
		}
		require.NotNil(t, leader)
		state, err := leader.getCheckerState()
		require.NoError(t, err)
		leader.bootstrap(term, state)
		state, err = leader.getCheckerState()
		require.NoError(t, err)
		require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		require.NoError(t, leader.stopReplica(hakeeper.DefaultHAKeeperShardID, uint64(leaderIndex+1)))
		require.Eventually(t, func() bool {
			for i, s := range stores {
				if i == leaderIndex {
					continue
				}
				ok, _, err := s.isLeaderHAKeeper()
				if err == nil && ok {
					state, err := s.getCheckerState()
					if err != nil || state.State != pb.HAKeeperBootstrapCommandsReceived {
						return false
					}
					if err := s.checkBootstrap(state); err != nil {
						return false
					}
					state, err = s.getCheckerState()
					return err == nil && state.State == pb.HAKeeperBootstrapCommandsReceived && !s.bootstrapCheckDeadline.IsZero()
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond)
	})
}
