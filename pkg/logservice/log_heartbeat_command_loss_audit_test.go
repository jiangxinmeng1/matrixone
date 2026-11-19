package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestAuditLogHeartbeatDropsUnacknowledgedReplicaCommand(t *testing.T) {
	runServiceTest(t, true, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		activateCommandDelivery(t, ctx, s)
		hb := s.store.getHeartbeatMessage()
		hb.UUID = "log-command-loss"
		req := pb.Request{Method: pb.LOG_HEARTBEAT, LogHeartbeat: &hb}
		registered := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), registered.ErrorCode)
		state, err := s.store.getCheckerStateWithContext(ctx)
		require.NoError(t, err)
		commands := operator.NewController().Dispatch(
			[]*operator.Operator{operator.CreateStartReplica("audit", hb.UUID, 1, 10)},
			state.LogState, state.TNState, state.CNState, state.ProxyState)
		require.Len(t, commands, 1)
		command := commands[0]
		require.False(t, command.Bootstrapping)
		require.NoError(t, s.store.addScheduleCommands(ctx, 1, commands))
		first := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), first.ErrorCode)
		require.Equal(t, []pb.ScheduleCommand{command}, first.CommandBatch.Commands)
		// Simulate loss of the committed heartbeat's RPC response. The store
		// never sees the command, and its next heartbeat has no ack to send.
		second := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), second.ErrorCode)
		require.Empty(t, second.CommandBatch.Commands)
		stored, err := s.store.getCommandBatch(ctx, command.UUID)
		require.NoError(t, err)
		require.Empty(t, stored.Commands)
	})
}
