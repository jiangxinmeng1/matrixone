package logservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestAuditThreeVoterShardRepairChoosesSameStoreTwice(t *testing.T) {
	const shardID = uint64(1)
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{
		ShardID: shardID, NumberOfReplicas: 3,
	}}}
	// After two sequential replica removals, the remaining member still has
	// a 1/1 quorum and can admit two replacements on distinct healthy stores.
	shard := pb.LogShardInfo{
		ShardID: shardID, Replicas: map[uint64]string{1: "a"},
		Epoch: 3, LeaderID: 1, Term: 3,
	}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{shardID: shard},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 0, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
			"b": {Tick: 0},
			"c": {Tick: 0},
		},
	}
	cfg := hakeeper.Config{}
	cfg.Fill()
	checker := NewLogServiceChecker(
		hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(10), cluster,
			pb.TaskTableUser{}, 0),
		state, pb.TNState{}, operator.ExecutingReplicas{}, operator.ExecutingReplicas{},
		0, pb.Locality{}, false,
	)
	ops := checker.Check()
	require.Len(t, ops, 2)
	targets := make([]string, 0, len(ops))
	for _, op := range ops {
		require.Len(t, op.OpSteps(), 1)
		step, ok := op.OpSteps()[0].(operator.AddLogService)
		require.True(t, ok)
		targets = append(targets, step.UUID)
	}
	t.Logf("two replacement votes scheduled on stores %v", targets)
	require.Equal(t, []string{"b", "b"}, targets)
	commands := operator.NewController().Dispatch(ops, state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Len(t, commands, 2)
	require.Equal(t, "b", commands[0].ConfigChange.Replica.UUID)
	require.Equal(t, "b", commands[1].ConfigChange.Replica.UUID)
}
