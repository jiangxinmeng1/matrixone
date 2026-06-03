// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_RestartFlusher(t *testing.T) {
	var cfg FlushCfg
	cfg.ForceFlushTimeout = time.Millisecond * 7
	cfg.ForceFlushCheckInterval = time.Millisecond * 9
	cfg.FlushInterval = time.Millisecond * 11
	cfg.CronPeriod = time.Millisecond * 2
	f := NewFlusher(
		nil, nil, nil, nil, false,
		WithFlusherInterval(cfg.FlushInterval),
		WithFlusherCronPeriod(cfg.CronPeriod),
		WithFlusherForceTimeout(cfg.ForceFlushTimeout),
		WithFlusherForceCheckInterval(cfg.ForceFlushCheckInterval),
	)
	f.Start()

	fCfg := f.GetCfg()
	assert.Equal(t, cfg, fCfg)
	assert.False(t, f.IsNoop())

	f.Stop()
	assert.True(t, f.IsNoop())

	ctx := context.Background()
	var ts types.TS

	assert.Equal(t, ErrFlusherStopped, f.FlushTable(ctx, 0, 0, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlush(ctx, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlushWithInterval(ctx, ts, time.Millisecond))
	f.ChangeForceCheckInterval(time.Millisecond)
	f.ChangeForceFlushTimeout(time.Millisecond)

	f.Restart(WithFlusherCfg(cfg))
	assert.False(t, f.IsNoop())
	fCfg = f.GetCfg()
	assert.Equal(t, cfg, fCfg)
}

func TestForeachAobjBeforeSkipsOldPersistedCEntry(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()

	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()

	txn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := c.CreateDBEntry("db", "", "", txn)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), txn, nil)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(context.Background()))

	dataAobj := catalog.NewInMemoryObject(table, types.BuildTS(100, 0), false)
	tombAobj := catalog.NewInMemoryObject(table, types.BuildTS(100, 0), true)
	table.Lock()
	table.AddEntryLocked(dataAobj)
	table.AddEntryLocked(tombAobj)
	table.Unlock()

	catalog.MockCreatedObjectEntry2List(table, c, false, types.BuildTS(120, 0))
	catalog.MockCreatedObjectEntry2List(table, c, true, types.BuildTS(120, 0))

	ts := types.BuildTS(300, 0)
	lastCkp := types.BuildTS(150, 0)
	var dataSeen, tombSeen []*catalog.ObjectEntry
	foreachAobjBefore(
		context.Background(),
		table,
		ts,
		lastCkp,
		func(entry *catalog.ObjectEntry) {
			dataSeen = append(dataSeen, entry)
		},
		func(entry *catalog.ObjectEntry) {
			tombSeen = append(tombSeen, entry)
		},
	)

	require.Equal(t, []*catalog.ObjectEntry{dataAobj}, dataSeen)
	require.Equal(t, []*catalog.ObjectEntry{tombAobj}, tombSeen)
}
