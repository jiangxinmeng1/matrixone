package logservicedriver

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	storeDriver "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/stretchr/testify/require"
)

func TestReplayAcrossTSOEntriesOnSameLogShard(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()
	cfg := NewConfig("", WithConfigOptClientConfig("", ccfg), WithConfigOptMaxClient(2))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	replay := func(d *LogServiceDriver, applied *[]uint64) {
		require.NoError(t, d.Replay(ctx, func(e *entry.Entry) storeDriver.ReplayEntryState {
			*applied = append(*applied, e.DSN)
			return storeDriver.RE_Nomal
		}, func() storeDriver.ReplayMode { return storeDriver.ReplayMode_ReplayForWrite }, nil))
	}
	d := NewLogServiceDriver(&cfg)
	replay(d, new([]uint64))
	appendOne := func(payload string) *entry.Entry {
		e := entry.MockEntryWithPayload([]byte(payload))
		require.NoError(t, d.Append(e))
		require.NoError(t, e.WaitDone())
		return e
	}
	first := appendOne("before-tso-gap")
	defer first.Entry.Free()

	c, err := logservice.NewClient(ctx, "", *ccfg)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		_, err := c.GetTSOTimestamp(ctx, 1)
		require.NoError(t, err)
	}
	require.NoError(t, c.Close())
	second := appendOne("after-tso-gap")
	defer second.Entry.Free()
	require.NoError(t, d.Close())

	d = NewLogServiceDriver(&cfg)
	var applied []uint64
	replay(d, &applied)
	require.Equal(t, []uint64{1, 2}, applied)
	d.sequence.mu.RLock()
	gap := d.sequence.psns.Maximum() - d.sequence.psns.Minimum()
	d.sequence.mu.RUnlock()
	require.GreaterOrEqual(t, gap, uint64(200), "TSO commands must create a real Raft PSN gap")
	require.NoError(t, d.Truncate(1))
	c, err = logservice.NewClient(ctx, "", *ccfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, dsn, err := c.GetWALTruncate(ctx)
		return err == nil && dsn == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, c.Close())
	require.NoError(t, d.Close())

	d = NewLogServiceDriver(&cfg)
	defer func() { require.NoError(t, d.Close()) }()
	applied = nil
	replay(d, &applied)
	require.Equal(t, []uint64{2}, applied)
}
