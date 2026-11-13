package logservice

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverFromSnapshotReplacesLeaseHistory(t *testing.T) {
	// Simulate an existing state machine receiving a later snapshot whose
	// lease history no longer contains the entries held by this replica.
	target := newStateMachine(1, 1).(*stateMachine)
	target.state.Index = 10
	target.state.LeaseHolderID = 101
	target.state.LeaseHistory = map[uint64]uint64{3: 100, 7: 101}

	source := newStateMachine(1, 2).(*stateMachine)
	source.state.Index = 12
	source.state.LeaseHolderID = 201
	source.state.LeaseHistory = map[uint64]uint64{5: 200, 11: 201}
	var snapshot bytes.Buffer
	require.NoError(t, source.SaveSnapshot(&snapshot, nil, nil))
	require.NoError(t, target.RecoverFromSnapshot(&snapshot, nil, nil))
	require.Equal(t, source.state, target.state)
	require.Equal(t, map[uint64]uint64{5: 200, 11: 201}, target.state.LeaseHistory)
	lease, _ := target.getLeaseHistory(9)
	require.Equal(t, uint64(200), lease)

	// A malformed replacement must leave the existing state untouched.
	require.Error(t, target.RecoverFromSnapshot(bytes.NewReader([]byte{0xff}), nil, nil))
	require.Equal(t, source.state, target.state)
}
