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

package txnimpl

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type baseTable struct {
	txnTable    *txnTable
	schema      *catalog.Schema
	isTombstone bool

	tableSpace *tableSpace
}

// duplicatedRowIDs keeps candidates from appendable and non-appendable
// objects separate. A merge output can be found before the replacement row in
// an appendable object, but its row may be removed by a transferred tombstone.
// Keeping both candidates until tombstone filtering is complete lets the
// caller fall back to the live replacement row instead of treating the key as
// absent.
type duplicatedRowIDs struct {
	appendable         containers.Vector
	nonAppendable      containers.Vector
	appendableByKey    [][]int
	nonAppendableByKey [][]int
}

func newDuplicatedRowIDs(
	pool *containers.VectorPool,
	length int,
) (*duplicatedRowIDs, error) {
	rowIDs := &duplicatedRowIDs{
		appendable:         pool.GetVector(&objectio.RowidType),
		nonAppendable:      pool.GetVector(&objectio.RowidType),
		appendableByKey:    make([][]int, length),
		nonAppendableByKey: make([][]int, length),
	}
	return rowIDs, nil
}

func (r *duplicatedRowIDs) Close() {
	if r == nil {
		return
	}
	if r.appendable != nil {
		r.appendable.Close()
		r.appendable = nil
	}
	if r.nonAppendable != nil {
		r.nonAppendable.Close()
		r.nonAppendable = nil
	}
}

func (r *duplicatedRowIDs) add(appendable bool, key int, rowID types.Rowid) {
	var rows containers.Vector
	var keys *[][]int
	if appendable {
		rows = r.appendable
		keys = &r.appendableByKey
	} else {
		rows = r.nonAppendable
		keys = &r.nonAppendableByKey
	}
	idx := rows.Length()
	rows.Append(rowID, false)
	(*keys)[key] = append((*keys)[key], idx)
}

func (r *duplicatedRowIDs) HasCandidate() bool {
	for i := range r.appendableByKey {
		for _, idx := range r.appendableByKey[i] {
			if !r.appendable.IsNull(idx) {
				return true
			}
		}
		for _, idx := range r.nonAppendableByKey[i] {
			if !r.nonAppendable.IsNull(idx) {
				return true
			}
		}
	}
	return false
}

func (r *duplicatedRowIDs) firstLive(rows containers.Vector, indexes []int) (types.Rowid, bool) {
	for _, idx := range indexes {
		if !rows.IsNull(idx) {
			return vector.GetFixedAtNoTypeCheck[types.Rowid](rows.GetDownstreamVector(), idx), true
		}
	}
	return types.EmptyRowid, false
}

// Merge chooses an effective row only after all object candidates have been
// checked against tombstones. Appendable rows are preferred because they are
// the replacement rows created after a merge source was written. Multiple
// candidates from the same object class are retained until that filtering is
// complete; the first surviving candidate preserves the existing object-list
// traversal order.
func (r *duplicatedRowIDs) Merge(pool *containers.VectorPool) containers.Vector {
	rowIDs := pool.GetVector(&objectio.RowidType)
	for i := range r.appendableByKey {
		if rowID, ok := r.firstLive(r.appendable, r.appendableByKey[i]); ok {
			rowIDs.Append(rowID, false)
			continue
		}
		if rowID, ok := r.firstLive(r.nonAppendable, r.nonAppendableByKey[i]); ok {
			rowIDs.Append(rowID, false)
			continue
		}
		rowIDs.Append(nil, true)
	}
	return rowIDs
}

func newBaseTable(schema *catalog.Schema, isTombstone bool, txnTable *txnTable) *baseTable {
	return &baseTable{
		schema:      schema,
		txnTable:    txnTable,
		isTombstone: isTombstone,
	}
}
func (tbl *baseTable) collectCmd(cmdMgr *commandManager) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.CollectCmd(cmdMgr)
	}
	return
}
func (tbl *baseTable) Close() error {
	if tbl.tableSpace != nil {
		err := tbl.tableSpace.Close()
		if err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	return nil
}
func (tbl *baseTable) DedupWorkSpace(key containers.Vector) (err error) {
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}
func (tbl *baseTable) approxSize() int {
	if tbl == nil || tbl.tableSpace == nil || tbl.tableSpace.node == nil {
		return 0
	}
	return tbl.tableSpace.node.data.ApproxSize()
}
func (tbl *baseTable) BatchDedupLocal(bat *containers.Batch) error {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return nil
	}
	return tbl.DedupWorkSpace(bat.GetVectorByName(tbl.schema.GetPrimaryKey().Name))
}

func (tbl *baseTable) addObjsWithMetaLoc(ctx context.Context, stats objectio.ObjectStats) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	if tbl.tableSpace != nil && tbl.tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.Extra.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	schema := tbl.schema
	if schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.txnTable.store.txn.GetDedupType()
		if !dedupType.SkipSourcePersisted() {
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				vectors, closeFunc, err = ioutil.LoadColumns2(
					ctx,
					[]uint16{uint16(schema.GetSingleSortKeyIdx())},
					nil,
					tbl.txnTable.store.rt.Fs,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
				err = tbl.txnTable.dedup(ctx, vectors[0], tbl.isTombstone)
				if err != nil {
					return
				}
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl.txnTable, tbl.isTombstone)
	}
	return tbl.tableSpace.AddDataFiles(pkVecs, stats)
}
func (tbl *baseTable) getRowsByPK(ctx context.Context, pks containers.Vector) (rowIDs *duplicatedRowIDs, err error) {
	var it *catalog.VisibleCommittedObjectIt
	if tbl.isTombstone {
		it = tbl.txnTable.entry.MakeTombstoneVisibleObjectIt(tbl.txnTable.store.txn)
	} else {
		it = tbl.txnTable.entry.MakeDataVisibleObjectIt(tbl.txnTable.store.txn)
	}
	defer it.Release()
	rowIDs, err = newDuplicatedRowIDs(
		tbl.txnTable.store.rt.VectorPool.Small,
		pks.Length(),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// GetByFilter intentionally continues with the candidates returned before
		// a WW conflict so it can still resolve the visible row after waiting.
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			rowIDs.Close()
			rowIDs = nil
		}
	}()
	pkType := pks.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	for it.Next() {
		obj := it.Item()
		if isEmptyDroppedAppendableObject(obj) {
			continue
		}
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}
		if obj.HasCommittedPersistedData() {
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}
		objectRowIDs := tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		err = vector.AppendMultiFixed[types.Rowid](
			objectRowIDs.GetDownstreamVector(),
			types.EmptyRowid,
			true,
			pks.Length(),
			common.WorkspaceAllocator,
		)
		if err == nil {
			err = obj.GetObjectData().GetDuplicatedRows(
				ctx,
				tbl.txnTable.store.txn,
				pks,
				nil,
				types.TS{}, types.MaxTs(),
				objectRowIDs,
				common.WorkspaceAllocator,
			)
		}
		if err == nil || moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			for i := 0; i < pks.Length(); i++ {
				if !objectRowIDs.IsNull(i) {
					rowIDs.add(
						obj.IsAppendable(),
						i,
						vector.GetFixedAtNoTypeCheck[types.Rowid](objectRowIDs.GetDownstreamVector(), i),
					)
				}
			}
		}
		objectRowIDs.Close()
		if err != nil {
			return
		}
	}
	return
}

// foreachVisibleObject visits exactly the catalog object versions used by
// getRowsByPK.  Callers that need snapshot visibility for another index (for
// example tombstone lookup) should use this helper instead of constructing an
// incremental ObjectList range with a different visibility rule.
func (tbl *baseTable) foreachVisibleObject(fn func(*catalog.ObjectEntry) error) error {
	var it *catalog.VisibleCommittedObjectIt
	if tbl.isTombstone {
		it = tbl.txnTable.entry.MakeTombstoneVisibleObjectIt(tbl.txnTable.store.txn)
	} else {
		it = tbl.txnTable.entry.MakeDataVisibleObjectIt(tbl.txnTable.store.txn)
	}
	defer it.Release()
	for it.Next() {
		if err := fn(it.Item()); err != nil {
			return err
		}
	}
	return nil
}

/*
similar to findDeletes
*/
func foreachIncrementalObject(
	snapshot catalog.ObjectListSnapshot,
	from, to types.TS,
	fn func(*catalog.ObjectEntry) error,
) error {
	_, err := foreachIncrementalObjectWithStats(snapshot, false, from, to, 0, "other", nil, dedupVisibilityFreeze, func(obj *catalog.ObjectEntry, _ *incrementalObjectScanStats) error {
		return fn(obj)
	})
	return err
}

// incrementalObjectScanStats separates time spent by the object-list iterator
// from time spent in the callback. filter is the per-entry catalog checks
// excluding maxCommit; maxCommit is tracked separately, and operation is the
// object-level GetDuplicatedRows/Contains work.
type incrementalObjectScanStats struct {
	scan                        time.Duration
	filter                      time.Duration
	createdAt                   time.Duration
	deletedAt                   time.Duration
	visibleByTS                 time.Duration
	groupState                  time.Duration
	maxCommit                   time.Duration
	operation                   time.Duration
	metrics                     objectScanMetricCounts
	aobjectCandidate            uint64
	aobjectScanned              uint64
	aobjectMaxCommitSkipped     uint64
	aobjectMaxCommitUnavailable uint64
}

const (
	objectScanVisited = iota
	objectScanCreatedAfterTo
	objectScanDeletedAtOrBeforeFrom
	objectScanNotVisibleAtTo
	objectScanAObjectCandidate
	objectScanAObjectMaxCommitUnavailable
	objectScanAObjectMaxCommitSkipped
	objectScanAObjectScanned
	objectScanEmptyDroppedSkipped
	objectScanGetDuplicatedRows
	objectScanResultCount
)

var objectScanResultNames = [...]string{
	"object_list_visited",
	"created_after_to",
	"deleted_at_or_before_from",
	"not_visible_at_to",
	"aobject_candidate",
	"aobject_max_commit_unavailable",
	"aobject_max_commit_skipped",
	"aobject_scanned",
	"empty_dropped_skipped",
	"object_get_duplicated_rows",
}

const objectStepCount = 9

var objectStepNames = [...]string{
	"object_list_scan",
	"object_list_iterator",
	"object_entry_filter",
	"object_filter_created_at",
	"object_filter_deleted_at",
	"object_filter_visible_by_ts",
	"object_filter_group_state",
	"object_max_commit_lookup",
	"object_operation",
}

type metricCounter interface {
	Add(float64)
}

type metricObserver interface {
	Observe(float64)
}

var prePrepareObjectCounters = func() [2][6][objectScanResultCount]metricCounter {
	var counters [2][6][objectScanResultCount]metricCounter
	types := [...]string{"data", "tombstone"}
	for typ := range types {
		for group := 0; group < 6; group++ {
			groupName := incrementalObjectGroupName(catalog.ObjectListGroup(group))
			for result := range objectScanResultNames {
				counters[typ][group][result] = v2.TxnTNPrePrepareObjectCounter.WithLabelValues(
					types[typ], groupName, objectScanResultNames[result],
				)
			}
		}
	}
	return counters
}()

var prePrepareObjectStepObservers = func() [2][6][objectStepCount]metricObserver {
	var observers [2][6][objectStepCount]metricObserver
	types := [...]string{"data", "tombstone"}
	for typ := range types {
		for group := 0; group < 6; group++ {
			groupName := incrementalObjectGroupName(catalog.ObjectListGroup(group))
			for step := range objectStepNames {
				observers[typ][group][step] = v2.TxnTNPrePrepareObjectStepDurationHistogram.WithLabelValues(
					types[typ], groupName, objectStepNames[step],
				)
			}
		}
	}
	return observers
}()

var prePrepareDedupStepObservers = func() map[string][2]metricObserver {
	var observers = make(map[string][2]metricObserver)
	for _, step := range []string{
		"source_persisted", "build_pk_vector", "build_pk_zonemap", "pk_dedup",
		"incremental_get_rows", "find_deletes", "check_duplicate_rows",
		"find_deletes_build_zonemap", "find_deletes_workspace_contains",
		"find_deletes_tombstone_scan", "find_deletes_object_contains",
		"wait_object_committed", "object_list_scan", "object_entry_filter",
		"object_filter_created_at", "object_filter_deleted_at",
		"object_filter_visible_by_ts", "object_filter_group_state",
		"object_max_commit_lookup", "object_operation",
	} {
		observers[step] = [2]metricObserver{
			v2.TxnTNPrePrepareDeduplicateStepDurationHistogram.WithLabelValues("data", step),
			v2.TxnTNPrePrepareDeduplicateStepDurationHistogram.WithLabelValues("tombstone", step),
		}
	}
	return observers
}()

var prePrepareBookmarkCounters = func() [2][4]metricCounter {
	var counters [2][4]metricCounter
	types := [...]string{"data", "tombstone"}
	groups := [...]string{
		"valid", "invalid", "prefix_skipped", "range_visited",
	}
	for typ := range types {
		for result := range groups {
			counters[typ][result] = v2.TxnTNPrePrepareObjectBookmarkCounter.WithLabelValues(
				types[typ], "appendable_create", groups[result],
			)
		}
	}
	return counters
}()

var tableScanMetricCache sync.Map

type tableScanMetricSet struct {
	tableLabel string
	once       [2][2][6]sync.Once
	counters   [2][2][6][objectScanResultCount]metricCounter
}

func (set *tableScanMetricSet) ensure(typ, kind, group int) {
	set.once[typ][kind][group].Do(func() {
		types := [...]string{"data", "tombstone"}
		kinds := [...]string{"aobject", "naobject"}
		groupName := incrementalObjectGroupName(catalog.ObjectListGroup(group))
		drop := prePrepareObjectHasDrop(catalog.ObjectListGroup(group))
		for result := range objectScanResultNames {
			set.counters[typ][kind][group][result] = v2.TxnTNPrePrepareObjectScanCounter.WithLabelValues(
				set.tableLabel, types[typ], kinds[kind], drop, groupName,
				objectScanResultNames[result],
			)
		}
	})
}

func getTableScanMetricSet(tableID uint64) *tableScanMetricSet {
	if tableID == 0 {
		return nil
	}
	if value, ok := tableScanMetricCache.Load(tableID); ok {
		return value.(*tableScanMetricSet)
	}
	set := &tableScanMetricSet{tableLabel: strconv.FormatUint(tableID, 10)}
	actual, _ := tableScanMetricCache.LoadOrStore(tableID, set)
	return actual.(*tableScanMetricSet)
}

type objectScanMetricCounts struct {
	// [aobject/naobject][result]. Keeping these counts local avoids touching
	// Prometheus label maps for every object in the iterator hot path.
	counts [2][objectScanResultCount]uint64
}

func (c *objectScanMetricCounts) add(obj *catalog.ObjectEntry, result int) {
	kind := 1 // naobject
	if obj.IsAppendable() {
		kind = 0
	}
	c.counts[kind][result]++
}

func incrementalObjectGroupName(group catalog.ObjectListGroup) string {
	switch group {
	case catalog.ObjectListGroupAppendableCreate:
		return "appendable_create"
	case catalog.ObjectListGroupAppendableCreateWithDrop:
		return "appendable_create_with_drop"
	case catalog.ObjectListGroupAppendableDrop:
		return "appendable_drop"
	case catalog.ObjectListGroupNonAppendableCreate:
		return "non_appendable_create"
	case catalog.ObjectListGroupNonAppendableCreateWithDrop:
		return "non_appendable_create_with_drop"
	case catalog.ObjectListGroupNonAppendableDrop:
		return "non_appendable_drop"
	default:
		return "unknown"
	}
}

func prePrepareObjectHasDrop(group catalog.ObjectListGroup) string {
	switch group {
	case catalog.ObjectListGroupAppendableCreateWithDrop,
		catalog.ObjectListGroupAppendableDrop,
		catalog.ObjectListGroupNonAppendableCreateWithDrop,
		catalog.ObjectListGroupNonAppendableDrop:
		return "true"
	default:
		return "false"
	}
}

func observePrePrepareObjectScan(
	tableID uint64,
	isTombstone bool,
	group catalog.ObjectListGroup,
	counts objectScanMetricCounts,
) {
	typIndex := 0
	if isTombstone {
		typIndex = 1
	}
	groupIndex := int(group)
	tableCounters := getTableScanMetricSet(tableID)
	for kindIndex := range [...]string{"aobject", "naobject"} {
		for resultIndex, count := range counts.counts[kindIndex] {
			if count == 0 {
				continue
			}
			prePrepareObjectCounters[typIndex][groupIndex][resultIndex].Add(float64(count))
			if tableCounters != nil {
				tableCounters.ensure(typIndex, kindIndex, groupIndex)
				tableCounters.counters[typIndex][kindIndex][groupIndex][resultIndex].Add(float64(count))
			}
		}
	}
	if group == catalog.ObjectListGroupAppendableCreate {
		visited := counts.counts[0][objectScanVisited] + counts.counts[1][objectScanVisited]
		if visited != 0 {
			prePrepareBookmarkCounters[typIndex][3].Add(float64(visited))
		}
	}
}

func observeIncrementalObjectScanStats(isTombstone bool, stats incrementalObjectScanStats) {
	typ := "data"
	if isTombstone {
		typ = "tombstone"
	}
	v2.TxnTNPrePrepareAObjectMaxCommitUnavailablePerScan.WithLabelValues(typ).
		Observe(float64(stats.aobjectMaxCommitUnavailable))

	observePrePrepareDedupStepDuration(isTombstone, "object_list_scan", stats.scan)
	observePrePrepareDedupStepDuration(isTombstone, "object_entry_filter", stats.filter)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_created_at", stats.createdAt)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_deleted_at", stats.deletedAt)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_visible_by_ts", stats.visibleByTS)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_group_state", stats.groupState)
	observePrePrepareDedupStepDuration(isTombstone, "object_max_commit_lookup", stats.maxCommit)
	observePrePrepareDedupStepDuration(isTombstone, "object_operation", stats.operation)

	// The iterator duration includes callback execution. Subtracting the
	// measured entry work leaves the time spent in the ObjectList traversal
	// itself (tree navigation, callback dispatch, and group boundaries).
	iterator := stats.scan - stats.filter - stats.maxCommit - stats.operation
	if iterator > 0 {
		observePrePrepareDedupStepDuration(isTombstone, "object_list_iterator", iterator)
	}
}

func observeIncrementalObjectGroupScanStats(
	isTombstone bool,
	group catalog.ObjectListGroup,
	stats incrementalObjectScanStats,
) {
	typIndex := boolToInt(isTombstone)
	groupIndex := int(group)
	durations := [...]time.Duration{
		stats.scan,
		maxDuration(0, stats.scan-stats.filter-stats.maxCommit-stats.operation),
		stats.filter,
		stats.createdAt,
		stats.deletedAt,
		stats.visibleByTS,
		stats.groupState,
		stats.maxCommit,
		stats.operation,
	}
	for step, duration := range durations {
		prePrepareObjectStepObservers[typIndex][groupIndex][step].Observe(duration.Seconds())
	}
	if stats.aobjectCandidate != 0 {
		v2.TxnAObjectDedupCandidateCounter.Add(float64(stats.aobjectCandidate))
	}
	if stats.aobjectScanned != 0 {
		v2.TxnAObjectDedupScannedCounter.Add(float64(stats.aobjectScanned))
	}
	if stats.aobjectMaxCommitSkipped != 0 {
		v2.TxnAObjectDedupMaxCommitSkippedCounter.Add(float64(stats.aobjectMaxCommitSkipped))
	}
	if stats.aobjectMaxCommitUnavailable != 0 {
		v2.TxnAObjectDedupMaxCommitUnavailableCounter.Add(float64(stats.aobjectMaxCommitUnavailable))
	}
}

// prePrepareObjectLogStats is an in-memory aggregation bucket for one table,
// object type, and ObjectList group.  It is deliberately independent from the
// Prometheus counters: the latter are permanent process counters, while this
// state is periodically swapped to produce a bounded diagnostic window.
type prePrepareObjectLogStats struct {
	calls  atomic.Uint64
	counts [2][objectScanResultCount]atomic.Uint64

	// Keep the per-scan selected NAObject distribution in a fixed atomic
	// histogram.  This is updated on the PrePrepare hot path without a map,
	// allocation, or lock.  The reporter snapshots it every 10 seconds.
	naobjectSelected         [prePrepareNAObjectHistogramSize]atomic.Uint64
	naobjectSelectedOverflow atomic.Uint64
}

type prePrepareObjectLogSnapshot struct {
	calls                    uint64
	counts                   [2][objectScanResultCount]uint64
	naobjectSelected         [prePrepareNAObjectHistogramSize]uint64
	naobjectSelectedOverflow uint64
}

const prePrepareNAObjectHistogramSize = 256

func (s *prePrepareObjectLogStats) add(counts objectScanMetricCounts) {
	s.calls.Add(1)
	for kind := range counts.counts {
		for result, count := range counts.counts[kind] {
			if count != 0 {
				s.counts[kind][result].Add(count)
			}
		}
	}
	selected := counts.counts[1][objectScanGetDuplicatedRows]
	if selected < prePrepareNAObjectHistogramSize {
		s.naobjectSelected[selected].Add(1)
	} else {
		s.naobjectSelectedOverflow.Add(1)
	}
}

func (s *prePrepareObjectLogStats) snapshotAndReset() prePrepareObjectLogSnapshot {
	var snapshot prePrepareObjectLogSnapshot
	snapshot.calls = s.calls.Swap(0)
	for kind := range s.counts {
		for result := range s.counts[kind] {
			snapshot.counts[kind][result] = s.counts[kind][result].Swap(0)
		}
	}
	for count := range s.naobjectSelected {
		snapshot.naobjectSelected[count] = s.naobjectSelected[count].Swap(0)
	}
	snapshot.naobjectSelectedOverflow = s.naobjectSelectedOverflow.Swap(0)
	return snapshot
}

type prePrepareObjectLogKey struct {
	tableID     uint64
	isTombstone bool
	group       catalog.ObjectListGroup
}

type prePrepareNAObjectDistributionEntry struct {
	tableID       uint64
	isTombstone   bool
	group         catalog.ObjectListGroup
	naobjectCount int
	occurrences   uint64
}

func isHigherNAObjectDistributionEntry(
	left, right prePrepareNAObjectDistributionEntry,
) bool {
	if left.naobjectCount != right.naobjectCount {
		return left.naobjectCount > right.naobjectCount
	}
	if left.occurrences != right.occurrences {
		return left.occurrences > right.occurrences
	}
	if left.tableID != right.tableID {
		return left.tableID < right.tableID
	}
	if left.isTombstone != right.isTombstone {
		return !left.isTombstone
	}
	return left.group < right.group
}

func addNAObjectDistributionTop(
	top []prePrepareNAObjectDistributionEntry,
	entry prePrepareNAObjectDistributionEntry,
) []prePrepareNAObjectDistributionEntry {
	insertAt := len(top)
	for i := range top {
		if isHigherNAObjectDistributionEntry(entry, top[i]) {
			insertAt = i
			break
		}
	}
	if insertAt == len(top) && len(top) == 10 {
		return top
	}
	if len(top) < 10 {
		top = append(top, prePrepareNAObjectDistributionEntry{})
	}
	last := len(top) - 1
	if last > 9 {
		last = 9
	}
	for i := last; i > insertAt; i-- {
		top[i] = top[i-1]
	}
	top[insertAt] = entry
	return top
}

var (
	prePrepareObjectLogBuckets sync.Map
	prePrepareObjectLogOnce    sync.Once
)

func startPrePrepareObjectLogReporter() {
	prePrepareObjectLogOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				flushPrePrepareObjectLogStats()
			}
		}()
	})
}

func flushPrePrepareObjectLogStats() {
	var top []prePrepareNAObjectDistributionEntry
	prePrepareObjectLogBuckets.Range(func(key, value any) bool {
		bucket := value.(*prePrepareObjectLogStats)
		snapshot := bucket.snapshotAndReset()
		if snapshot.calls == 0 {
			return true
		}
		k := key.(prePrepareObjectLogKey)
		typeName := "data"
		if k.isTombstone {
			typeName = "tombstone"
		}
		naobject := snapshot.counts[1]
		aobject := snapshot.counts[0]
		logutil.Infof(
			"[PrePrepareDedupObjectScan] window=10s table=%d type=%s group=%s calls=%d "+
				"naobject_visited=%d naobject_selected=%d naobject_created_after_to=%d "+
				"naobject_deleted_at_or_before_from=%d naobject_not_visible_at_to=%d "+
				"aobject_visited=%d aobject_selected=%d",
			k.tableID,
			typeName,
			incrementalObjectGroupName(k.group),
			snapshot.calls,
			naobject[objectScanVisited],
			naobject[objectScanGetDuplicatedRows],
			naobject[objectScanCreatedAfterTo],
			naobject[objectScanDeletedAtOrBeforeFrom],
			naobject[objectScanNotVisibleAtTo],
			aobject[objectScanVisited],
			aobject[objectScanGetDuplicatedRows],
		)
		for naobjectCount := 1; naobjectCount < len(snapshot.naobjectSelected); naobjectCount++ {
			occurrences := snapshot.naobjectSelected[naobjectCount]
			if occurrences == 0 {
				continue
			}
			top = addNAObjectDistributionTop(top, prePrepareNAObjectDistributionEntry{
				tableID:       k.tableID,
				isTombstone:   k.isTombstone,
				group:         k.group,
				naobjectCount: naobjectCount,
				occurrences:   occurrences,
			})
		}
		if snapshot.naobjectSelectedOverflow != 0 {
			top = addNAObjectDistributionTop(top, prePrepareNAObjectDistributionEntry{
				tableID:       k.tableID,
				isTombstone:   k.isTombstone,
				group:         k.group,
				naobjectCount: prePrepareNAObjectHistogramSize,
				occurrences:   snapshot.naobjectSelectedOverflow,
			})
		}
		return true
	})
	for rank, entry := range top {
		typeName := "data"
		if entry.isTombstone {
			typeName = "tombstone"
		}
		logutil.Infof(
			"[PrePrepareDedupNAObjectTop] window=10s rank=%d table=%d type=%s group=%s "+
				"naobject_selected_count=%d occurrences=%d",
			rank+1,
			entry.tableID,
			typeName,
			incrementalObjectGroupName(entry.group),
			entry.naobjectCount,
			entry.occurrences,
		)
	}
}

// recordPrePrepareObjectGroupStats records one group in memory.  Logging is
// intentionally deferred to the 10-second reporter so the PrePrepare queue
// does not perform an Info log for every transaction/group.
func recordPrePrepareObjectGroupStats(
	tableID uint64,
	isTombstone bool,
	group catalog.ObjectListGroup,
	stats incrementalObjectScanStats,
) {
	naobject := stats.metrics.counts[1]
	aobject := stats.metrics.counts[0]
	if naobject[objectScanVisited] == 0 && aobject[objectScanVisited] == 0 {
		return
	}
	startPrePrepareObjectLogReporter()
	key := prePrepareObjectLogKey{tableID: tableID, isTombstone: isTombstone, group: group}
	bucketValue, _ := prePrepareObjectLogBuckets.LoadOrStore(key, &prePrepareObjectLogStats{})
	bucketValue.(*prePrepareObjectLogStats).add(stats.metrics)
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func foreachIncrementalObjectWithStats(
	snapshot catalog.ObjectListSnapshot,
	isTombstone bool,
	from, to types.TS,
	tableID uint64,
	scope string,
	reader txnif.TxnReader,
	mode dedupVisibilityMode,
	fn func(*catalog.ObjectEntry, *incrementalObjectScanStats) error,
) (stats incrementalObjectScanStats, err error) {
	usePrepareTS := mode == dedupVisibilityPrePrepare
	// ObjectList counters and per-object timings are queue diagnostics. Freeze
	// uses the same traversal helper, but must not contribute to PrePrepare
	// metrics or make queue scans appear larger than they are.
	collectPrePrepareMetrics := scope == "preprepare" || scope == "preprepare_find_deletes"
	// Normal readers wait until catalog MVCC has applied its commit timestamp
	// and then use CreatedAt/DeletedAt directly. PrePrepareDedup is different:
	// it runs in the serialized pre-WAL worker, where a predecessor can be in
	// Preparing but not yet ApplyCommitted. In that path the entry's
	// CreateNode/DeleteNode prepare timestamp is the visibility fence. Keep
	// this opt-in so Freeze, GetByFilter, and other readers retain their
	// existing wait-and-read semantics.
	objectTimes := func(obj *catalog.ObjectEntry) (createdAt, deletedAt types.TS) {
		createdAt = obj.CreatedAt
		deletedAt = obj.DeletedAt
		if !usePrepareTS {
			return
		}
		// Do not make a just-created object visible to its own object-level
		// dedup: the source-persisted path is querying the object it is about to
		// publish and would otherwise match every row against itself.
		if createdAt == txnif.UncommitTS {
			createdAt = obj.CreateNode.GetPrepare()
		}
		if deletedAt == txnif.UncommitTS {
			deletedAt = obj.DeleteNode.GetPrepare()
		}
		return
	}
	// Freeze uses the normal catalog MVCC visibility path.  It is important
	// that this is not reduced to the PrepareTS check used by PrePrepare:
	// Freeze can see an active object version belonging to its own txn, while
	// an active version belonging to another txn is not a committed object
	// that should participate in the range scan.  Committed objects newer than
	// the txn start TS are deliberately kept here; their CreatedAt/DeletedAt
	// range is what makes them conflict candidates for dedup.
	freezeObjectVisible := func(obj *catalog.ObjectEntry) bool {
		if reader == nil {
			return true
		}
		visible := obj.IsVisible(reader)
		if !visible && !obj.IsCommitted() {
			return false
		}
		// A create entry and its delete entry can both be present while this
		// transaction is freezing.  Do not dedup the old version against the
		// replacement written by the same transaction.
		if obj.DeleteNode.IsSameTxn(reader) {
			return false
		}
		next := obj.GetNextVersion()
		return next == nil || !next.DeleteNode.IsSameTxn(reader)
	}
	// PrePrepare runs in the serialized pre-WAL worker.  It cannot wait for
	// the catalog commit path in the same way as Freeze, so it uses the
	// predecessor's PrepareTS and only hides a same-transaction drop.
	prePrepareSameTxnDrop := func(obj *catalog.ObjectEntry) bool {
		if !usePrepareTS || reader == nil {
			return false
		}
		if obj.DeleteNode.IsSameTxn(reader) {
			return true
		}
		// The create entry keeps the delete entry as its next version. Both
		// entries can be present in the snapshot while the same transaction is
		// still preparing; hide the old version from its replacement dedup.
		next := obj.GetNextVersion()
		return next != nil && next.DeleteNode.IsSameTxn(reader)
	}
	shouldSkipForPhase := func(obj *catalog.ObjectEntry) bool {
		if usePrepareTS {
			return prePrepareSameTxnDrop(obj)
		}
		return !freezeObjectVisible(obj)
	}
	objectVisibleByTS := func(createdAt, deletedAt, ts types.TS) bool {
		if createdAt.GT(&ts) {
			return false
		}
		return deletedAt.IsEmpty() || !deletedAt.LT(&ts)
	}
	groupStats := make(map[catalog.ObjectListGroup]*incrementalObjectScanStats)
	var activeStats *incrementalObjectScanStats
	getGroupStats := func(group catalog.ObjectListGroup) *incrementalObjectScanStats {
		if groupStats[group] == nil {
			groupStats[group] = &incrementalObjectScanStats{}
		}
		return groupStats[group]
	}
	mergeStats := func(dst *incrementalObjectScanStats, src *incrementalObjectScanStats) {
		dst.scan += src.scan
		dst.filter += src.filter
		dst.createdAt += src.createdAt
		dst.deletedAt += src.deletedAt
		dst.visibleByTS += src.visibleByTS
		dst.groupState += src.groupState
		dst.maxCommit += src.maxCommit
		dst.operation += src.operation
		dst.aobjectCandidate += src.aobjectCandidate
		dst.aobjectScanned += src.aobjectScanned
		dst.aobjectMaxCommitSkipped += src.aobjectMaxCommitSkipped
		dst.aobjectMaxCommitUnavailable += src.aobjectMaxCommitUnavailable
	}
	observeObject := func(obj *catalog.ObjectEntry, result int) {
		activeStats.metrics.add(obj, result)
	}
	shouldSkipAObject := func(obj *catalog.ObjectEntry) bool {
		objData := obj.GetObjectData()
		maxCommitter, ok := objData.(interface {
			GetAppendMaxCommitTS() (types.TS, bool)
		})
		if !ok {
			// Only an in-memory append object has an append-history max. Do not
			// use the catalog appendable bit as the gate: it describes the
			// object's storage kind, while a sealed AObject is no longer
			// appendable at the data layer but must still be filtered here.
			return false
		}
		observeObject(obj, objectScanAObjectCandidate)
		activeStats.aobjectCandidate++
		maxCommitStart := time.Now()
		maxCommit, finalized := maxCommitter.GetAppendMaxCommitTS()
		activeStats.maxCommit += time.Since(maxCommitStart)
		if !finalized {
			// Unsealed append history has no safe upper bound. It must not be
			// skipped, even when the currently visible rows look old.
			activeStats.aobjectMaxCommitUnavailable++
			activeStats.aobjectScanned++
			observeObject(obj, objectScanAObjectMaxCommitUnavailable)
			observeObject(obj, objectScanAObjectScanned)
			frozen := false
			if inspectable, ok := objData.(interface{ IsAppendFrozen() bool }); ok {
				frozen = inspectable.IsAppendFrozen()
			}
			logutil.Debugf(
				"[PrePrepareDedup] aobject max commit unavailable scope=%s table=%d tombstone=%v object=%s created=%s drop=%v catalog_appendable=%v frozen=%v from=%s to=%s",
				scope,
				tableID,
				isTombstone,
				obj.ID().ShortStringEx(),
				obj.CreatedAt.ToString(),
				obj.HasDropCommitted(),
				obj.IsAppendable(),
				frozen,
				from.ToString(),
				to.ToString())
			return false
		}
		if maxCommit.LT(&from) {
			activeStats.aobjectMaxCommitSkipped++
			observeObject(obj, objectScanAObjectMaxCommitSkipped)
			return true
		}
		activeStats.aobjectScanned++
		observeObject(obj, objectScanAObjectScanned)
		return false
	}
	recordFilter := func(entryStart time.Time, maxCommitBefore, operationBefore time.Duration) {
		filterElapsed := time.Since(entryStart) -
			(activeStats.maxCommit - maxCommitBefore) -
			(activeStats.operation - operationBefore)
		if filterElapsed > 0 {
			activeStats.filter += filterElapsed
		}
	}
	visitCreateGroup := func(group catalog.ObjectListGroup, appendable bool) error {
		// An appendable object created before from can still contain an append
		// prepared in [from, to]. Do not use the lower CreatedAt bound to
		// truncate an appendable create group. The upper bound is safe because
		// the group's tree is ordered by CreatedAt.
		var err error
		localStats := getGroupStats(group)
		activeStats = localStats
		visitEntry := func(obj *catalog.ObjectEntry) bool {
			entryStart := time.Now()
			maxCommitBefore := activeStats.maxCommit
			operationBefore := activeStats.operation
			observeObject(obj, objectScanVisited)
			if shouldSkipForPhase(obj) {
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			createdAt, deletedAt := objectTimes(obj)
			createdAfterTo := createdAt.GT(&to)
			if createdAfterTo {
				observeObject(obj, objectScanCreatedAfterTo)
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				// An uncommitted entry is ordered by UncommitTS in the catalog
				// tree, while its prepared timestamp may be inside [from, to].
				// Do not terminate the group on that raw sentinel.
				return !(usePrepareTS && obj.CreatedAt == txnif.UncommitTS)
			}
			// Keep create-group filtering consistent with the data scan: an
			// object whose delete timestamp is before the snapshot must not be
			// consulted by findDeletes, even if its catalog entry is still in
			// the create group while the delete is being prepared.
			if !objectVisibleByTS(createdAt, deletedAt, to) {
				observeObject(obj, objectScanNotVisibleAtTo)
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			if shouldSkipAObject(obj) {
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			operationStart := time.Now()
			err = fn(obj, activeStats)
			activeStats.operation += time.Since(operationStart)
			recordFilter(entryStart, maxCommitBefore, operationBefore)
			return err == nil
		}
		scanStart := time.Now()
		bookmarkStart, bookmarkValid, bookmarkSkipped := (*catalog.ObjectEntry)(nil), false, 0
		if appendable && !usePrepareTS {
			bookmarkStart, bookmarkValid, bookmarkSkipped = snapshot.CommitBookmarkStartWithCount(group, from)
		}
		if from == txnif.UncommitTS {
			bookmarkValid = false
			bookmarkSkipped = 0
		}
		if collectPrePrepareMetrics && group == catalog.ObjectListGroupAppendableCreate {
			kind := "data"
			if isTombstone {
				kind = "tombstone"
			}
			groupName := incrementalObjectGroupName(group)
			bookmarkResultIndex := 1
			if bookmarkValid {
				bookmarkResultIndex = 0
			}
			prePrepareBookmarkCounters[boolToInt(isTombstone)][bookmarkResultIndex].Add(1)
			if bookmarkSkipped > 0 {
				prePrepareBookmarkCounters[boolToInt(isTombstone)][2].Add(float64(bookmarkSkipped))
			}
			startID := "<none>"
			if bookmarkStart != nil {
				startID = bookmarkStart.ID().ShortStringEx()
			}
			logutil.Debugf(
				"[PrePrepareDedup] object-list bookmark group=%s type=%s valid=%v prefix-skipped=%d start=%s from=%s to=%s",
				groupName, kind, bookmarkValid, bookmarkSkipped, startID, from.ToString(), to.ToString())
		}
		if bookmarkValid {
			// The prefix bookmark proves that every entry before bookmarkStart
			// has maxCommitTS < from. This is the only path that can avoid
			// rechecking those objects one by one.
			if bookmarkStart != nil {
				snapshot.ScanGroupFrom(group, bookmarkStart, visitEntry)
			}
		} else if appendable {
			snapshot.ScanGroup(group, visitEntry)
		} else {
			snapshot.AscendGroup(group, from, visitEntry)
		}
		localStats.scan += time.Since(scanStart)
		if collectPrePrepareMetrics {
			observePrePrepareObjectScan(tableID, isTombstone, group, localStats.metrics)
			observeIncrementalObjectGroupScanStats(isTombstone, group, *localStats)
			recordPrePrepareObjectGroupStats(tableID, isTombstone, group, *localStats)
		}
		mergeStats(&stats, localStats)
		return err
	}
	if err := visitCreateGroup(catalog.ObjectListGroupAppendableCreate, true); err != nil {
		return stats, err
	}
	visitDropGroup := func(group catalog.ObjectListGroup) error {
		var err error
		localStats := getGroupStats(group)
		activeStats = localStats
		scanStart := time.Now()
		visitEntry := func(obj *catalog.ObjectEntry) bool {
			entryStart := time.Now()
			maxCommitBefore := activeStats.maxCommit
			operationBefore := activeStats.operation
			observeObject(obj, objectScanVisited)
			if shouldSkipForPhase(obj) {
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			createdAt, deletedAt := objectTimes(obj)
			deletedAfterFrom := deletedAt.GT(&from)
			// Ascend is inclusive. Drop processing starts strictly after from.
			if !deletedAfterFrom {
				observeObject(obj, objectScanDeletedAtOrBeforeFrom)
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			createdAfterTo := createdAt.GT(&to)
			if createdAfterTo {
				observeObject(obj, objectScanCreatedAfterTo)
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			visible := objectVisibleByTS(createdAt, deletedAt, to)
			if !visible {
				observeObject(obj, objectScanNotVisibleAtTo)
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			if shouldSkipAObject(obj) {
				recordFilter(entryStart, maxCommitBefore, operationBefore)
				return true
			}
			operationStart := time.Now()
			err = fn(obj, activeStats)
			activeStats.operation += time.Since(operationStart)
			recordFilter(entryStart, maxCommitBefore, operationBefore)
			return err == nil
		}
		bookmarkStart, bookmarkValid := (*catalog.ObjectEntry)(nil), false
		if group == catalog.ObjectListGroupAppendableDrop && !usePrepareTS {
			bookmarkStart, bookmarkValid = snapshot.CommitBookmarkStart(group, from)
		}
		if from == txnif.UncommitTS {
			bookmarkValid = false
		}
		if bookmarkValid {
			if bookmarkStart != nil {
				snapshot.ScanGroupFrom(group, bookmarkStart, visitEntry)
			}
		} else {
			snapshot.AscendGroup(group, from, visitEntry)
		}
		localStats.scan += time.Since(scanStart)
		observePrePrepareObjectScan(tableID, isTombstone, group, localStats.metrics)
		observeIncrementalObjectGroupScanStats(isTombstone, group, *localStats)
		if collectPrePrepareMetrics {
			recordPrePrepareObjectGroupStats(tableID, isTombstone, group, *localStats)
		}
		mergeStats(&stats, localStats)
		return err
	}
	if err := visitDropGroup(catalog.ObjectListGroupAppendableDrop); err != nil {
		return stats, err
	}

	if err := visitCreateGroup(catalog.ObjectListGroupNonAppendableCreate, false); err != nil {
		return stats, err
	}
	err = visitDropGroup(catalog.ObjectListGroupNonAppendableDrop)
	return stats, err
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// incrementalGetRowsByPK checks the inclusive logical interval [from, to].
// Callers that hold an exclusive dedup watermark must pass watermark.Next().
func (tbl *baseTable) incrementalGetRowsByPK(ctx context.Context, pks containers.Vector, from, to types.TS, mode dedupVisibilityMode) (rowIDs containers.Vector, err error) {
	stepStart := time.Now()
	var snapshot catalog.ObjectListSnapshot
	if tbl.isTombstone {
		if mode == dedupVisibilityFreeze {
			tbl.txnTable.entry.WaitTombstoneObjectCommitted(to)
		}
		snapshot = tbl.txnTable.entry.MakeTombstoneObjectSnapshot()
	} else {
		if mode == dedupVisibilityFreeze {
			tbl.txnTable.entry.WaitDataObjectCommitted(to)
		}
		snapshot = tbl.txnTable.entry.MakeDataObjectSnapshot()
	}
	if mode == dedupVisibilityPrePrepare {
		observePrePrepareDedupStep(tbl.isTombstone, "wait_object_committed", stepStart)
	}
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer func() {
		// Ownership transfers to the caller only on success. In particular,
		// lazy commit-TS reads add cancellable I/O errors after allocation.
		if err != nil {
			rowIDs.Close()
			rowIDs = nil
		}
	}()
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)

	var scanStats incrementalObjectScanStats
	scope := "freeze"
	if mode == dedupVisibilityPrePrepare {
		scope = "preprepare"
	}
	scanStats, err = foreachIncrementalObjectWithStats(snapshot, tbl.isTombstone, from, to, tbl.txnTable.entry.ID, scope, tbl.txnTable.store.txn, mode, func(obj *catalog.ObjectEntry, stats *incrementalObjectScanStats) error {
		if isEmptyDroppedAppendableObject(obj) {
			// The group counter is emitted once after the group traversal. Keep
			// this callback allocation-free and only record the local result.
			// The object is still passed through the normal operation callback,
			// so use the same per-group metric accumulator as other results.
			stats.metrics.add(obj, objectScanEmptyDroppedSkipped)
			return nil
		}
		objData := obj.GetObjectData()
		err := objData.GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			from, to,
			rowIDs,
			common.WorkspaceAllocator,
		)
		stats.metrics.add(obj, objectScanGetDuplicatedRows)
		return err
	})
	if mode == dedupVisibilityPrePrepare {
		observeIncrementalObjectScanStats(tbl.isTombstone, scanStats)
	}
	if err != nil {
		return
	}
	// s := ""
	// for _, v := range candidates {
	// 	s += v.StringWithLevel(2) + ","
	// }
	// logutil.Info("incr",
	// 	zap.Bool("inQueue", inQueue),
	// 	zap.String("table", tbl.txnTable.store.txn.Repr()),
	// 	zap.String("from", from.ToString()),
	// 	zap.String("to", to.ToString()),
	// 	zap.String("rowIDs", rowIDs.String()),
	// 	zap.String("pks", pks.String()),
	// 	zap.String("candidates", s),
	// )
	return
}

func isEmptyDroppedAppendableObject(obj *catalog.ObjectEntry) bool {
	stats := obj.GetObjectStats()
	if !obj.IsAppendable() || stats.Rows() != 0 || stats.BlkCnt() != 0 {
		return false
	}
	dropCommitted := obj.HasDropCommitted()
	if !dropCommitted && obj.IsCEntry() && obj.HasDCounterpart() {
		dropCommitted = obj.GetNextVersion().HasDropCommitted()
	}
	if !dropCommitted {
		return false
	}
	objData := obj.GetObjectData()
	if objData == nil {
		return false
	}
	rows, err := objData.Rows()
	if err != nil || rows != 0 {
		return false
	}
	return true
}

func (tbl *baseTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
}

func (tbl *baseTable) PrePrepare() error {
	if tbl.tableSpace != nil {
		return tbl.tableSpace.PrepareApply()
	}
	return nil
}

func quickSkipThisObject(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.ObjectEntry,
) (ok bool, err error) {
	ok = !meta.SortKeyZoneMap().FastIntersect(keysZM)
	return
}
