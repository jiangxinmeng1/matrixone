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

package hakeeper

import (
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

// This captures the scheduling race where StartReplica was queued for an
// expired store, the remaining voters removed that member, then the store
// heartbeated again. The stale command should not be delivered after removal.
func TestAuditLogHeartbeatReturnsStartAfterReplicaRemoval(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	rsm.state.LogState.Shards[1] = pb.LogShardInfo{
		ShardID: 1, Epoch: 7, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
		NonVotingReplicas: make(map[uint64]string),
	}
	start := pb.ScheduleCommand{
		UUID: "a", ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{
			UUID: "a", ShardID: 1, ReplicaID: 1,
		}},
	}
	_, err := rsm.Update(sm.Entry{Index: 10, Cmd: GetUpdateCommandsCmd(1, []pb.ScheduleCommand{start})})
	require.NoError(t, err)

	// A survivor reports the committed Raft membership after replica 1 was
	// removed while store a was offline.
	survivor := pb.LogStoreHeartbeat{UUID: "b", Replicas: []pb.LogReplicaInfo{{
		LogShardInfo: pb.LogShardInfo{
			ShardID: 1, Epoch: 8, Replicas: map[uint64]string{2: "b", 3: "c"},
			NonVotingReplicas: make(map[uint64]string),
		}, ReplicaID: 2,
	}}}
	data, err := survivor.Marshal()
	require.NoError(t, err)
	_, err = rsm.Update(sm.Entry{Index: 11, Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)

	// Store a returns, but reports no local replica. The stale StartReplica
	// should have been discarded, but the current state machine returns it even
	// though authoritative membership excludes its replica ID.
	recovered := pb.LogStoreHeartbeat{UUID: "a"}
	data, err = recovered.Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{Index: 12, Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	require.Len(t, batch.Commands, 1)
	require.Equal(t, pb.StartReplica, batch.Commands[0].ConfigChange.ChangeType)
	require.Equal(t, uint64(1), batch.Commands[0].ConfigChange.Replica.ReplicaID)
}
