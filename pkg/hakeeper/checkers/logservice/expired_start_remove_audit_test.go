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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// A store whose heartbeat has expired cannot execute a StartReplica command.
// If its last heartbeat also reported no local replica, recovery should remove
// its old membership without queuing a conflicting start for that generation.
func TestAuditExpiredMissingVoterDoesNotStartAndRemove(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID: 1, Epoch: 7, LeaderID: 2,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
		}},
		Stores: map[string]pb.LogStoreInfo{
			"a": {Tick: 1},
			"b": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{ReplicaID: 2, LogShardInfo: pb.LogShardInfo{ShardID: 1}}}},
			"c": {Tick: 10000, Replicas: []pb.LogReplicaInfo{{ReplicaID: 3, LogShardInfo: pb.LogShardInfo{ShardID: 1}}}},
		},
	}
	normal, _ := parseLogShards(cluster, state, []string{"a"}, 0)
	require.Equal(t, []replica{{uuid: "a", shardID: 1, replicaID: 1}}, normal.toRemove[1])
	require.Equal(t, []replica{{uuid: "a", shardID: 1, replicaID: 1}}, normal.toStart,
		"reproduces scheduling StartReplica for the same expired voter being removed")

	cfg := hakeeper.Config{}
	cfg.Fill()
	checker := NewLogServiceChecker(
		hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(100), cluster,
			pb.TaskTableUser{}, 10000),
		state, pb.TNState{}, operator.ExecutingReplicas{}, operator.ExecutingReplicas{},
		0, pb.Locality{}, false,
	)
	ops := checker.Check()
	require.Len(t, ops, 2)
	var started, removed bool
	for _, op := range ops {
		for _, step := range op.OpSteps() {
			switch step := step.(type) {
			case operator.StartLogService:
				started = step.UUID == "a" && step.ReplicaID == 1
			case operator.RemoveLogService:
				removed = step.UUID == "a" && step.ReplicaID == 1
			}
		}
	}
	require.True(t, started)
	require.True(t, removed)
}
