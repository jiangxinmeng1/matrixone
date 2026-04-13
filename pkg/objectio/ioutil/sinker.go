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

package ioutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const DefaultInMemoryStagedSize = mpool.MB * 16

type SinkerOption func(*Sinker)

func WithAllMergeSorted() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.allMergeSorted = true
	}
}

func WithDedupAll() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.dedupAll = true
	}
}

func WithBufferSizeCap(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.bufferSizeCap = size
	}
}

func WithTailSizeCap(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.tailSizeCap = size
	}
}

func WithMemorySizeThreshold(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.staged.memorySizeThreshold = size
	}
}

func WithBuffer(buffer *containers.OneSchemaBatchBuffer, isOwner bool) SinkerOption {
	return func(sinker *Sinker) {
		sinker.buf.isOwner = isOwner
		sinker.buf.buffers = buffer
	}
}

func WithPipelineFlush(sinkWorkers, maxPendingSync int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.pipe.enabled = true
		if sinkWorkers < 1 {
			sinkWorkers = 1
		}
		sinker.pipe.sinkWorkers = sinkWorkers
		if maxPendingSync < 1 {
			maxPendingSync = 1
		}
		sinker.pipe.maxPending = maxPendingSync
	}
}

type FileSinker interface {
	Sink(context.Context, *batch.Batch) error
	Sync(context.Context) (*objectio.ObjectStats, error)
	Reset()
	Close() error
}

var _ FileSinker = new(FSinkerImpl)

type FSinkerImpl struct {
	writer *BlockWriter
	mp     *mpool.MPool
	fs     fileservice.FileService

	sortKeyPos      int
	isPrimaryKey    bool
	isTombstone     bool
	seqnums         []uint16
	schemaVersion   uint32
	hiddenSelection objectio.HiddenColumnSelection
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		if s.isTombstone {
			s.writer = ConstructTombstoneWriter(
				s.hiddenSelection,
				s.fs,
			)
		} else {
			s.writer = ConstructWriter(
				s.schemaVersion,
				s.seqnums,
				s.sortKeyPos,
				s.isPrimaryKey,
				s.isTombstone,
				s.fs,
			)
		}
	}

	_, err := s.writer.WriteBatch(b)
	return err
}

func (s *FSinkerImpl) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	if _, _, err := s.writer.Sync(ctx); err != nil {
		return nil, err
	}

	var ss objectio.ObjectStats
	if s.sortKeyPos > -1 {
		ss = s.writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	} else {
		ss = s.writer.GetObjectStats(objectio.WithCNCreated())
	}

	// s.writer.Reset
	s.writer = nil

	return &ss, nil
}

func (s *FSinkerImpl) Reset() {
	if s.writer != nil {
		// s.writer.Reset
		s.writer = nil
	}
}

func (s *FSinkerImpl) Close() error {
	// s.writer.Reset
	s.writer = nil
	return nil
}

// FileSinkerFactory is a factory function to create a FileSinker
type FileSinkerFactory func(*mpool.MPool, fileservice.FileService) FileSinker

// This factory is used to create a FileSinker for tombstone
// hiddenSelection: whether to write hidden tombstone
//
//	true: TN created tombstone objects
//	false: CN created tombstone objects
func NewTombstoneFSinkerImplFactory(hidden objectio.HiddenColumnSelection) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewTombstoneFSinkerImpl(hidden, mp, fs)
	}
}

// This factory is used to create a FileSinker for all kinds of objects
// the user should provide the seqnums, sortKeyPos, isPrimaryKey, isTombstone, schemaVersion
func NewFSinkerImplFactory(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewFSinkerImpl(
			seqnums,
			sortKeyPos,
			isPrimaryKey,
			isTombstone,
			schemaVersion,
			mp,
			fs,
		)
	}
}

func NewTombstoneFSinkerImpl(
	hidden objectio.HiddenColumnSelection,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:              fs,
		mp:              mp,
		isTombstone:     true,
		hiddenSelection: hidden,
	}
}

func NewFSinkerImpl(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:            fs,
		mp:            mp,
		seqnums:       seqnums,
		sortKeyPos:    sortKeyPos,
		isPrimaryKey:  isPrimaryKey,
		isTombstone:   isTombstone,
		schemaVersion: schemaVersion,
	}
}

func NewTombstoneSinker(
	hidden objectio.HiddenColumnSelection,
	pkType types.Type,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	factory := NewTombstoneFSinkerImplFactory(hidden)
	attrs, attrTypes := objectio.GetTombstoneSchema(pkType, hidden)
	return NewSinker(
		objectio.TombstonePrimaryKeyIdx,
		attrs,
		attrTypes,
		factory,
		mp,
		fs,
		opts...,
	)
}

func NewSinker(
	sortKeyIdx int,
	attrs []string,
	attrTypes []types.Type,
	factory FileSinkerFactory,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	sinker := &Sinker{
		schema: struct {
			attrs      []string
			attrTypes  []types.Type
			sortKeyIdx int
		}{
			attrs:      attrs,
			attrTypes:  attrTypes,
			sortKeyIdx: sortKeyIdx,
		},
		fSinker: struct {
			executor FileSinker
			factory  FileSinkerFactory
		}{
			factory: factory,
		},

		mp: mp,
		fs: fs,
	}

	for _, opt := range opts {
		opt(sinker)
	}

	sinker.fillDefaults()
	return sinker
}

type sinkerStats struct {
	Name               string
	HighWatermarkCnt   uint64
	HighWatermarkBytes uint64
	CurrentCnt         uint64
	CurrentBytes       uint64
}

func (s *sinkerStats) String() string {
	return fmt.Sprintf("%s, high cnt: %d, current cnt: %d, hight bytes: %d, current bytes: %d",
		s.Name, s.HighWatermarkCnt, s.CurrentCnt, s.HighWatermarkBytes, s.CurrentBytes)
}

func (s *sinkerStats) updateCount(n int) {
	if n > 0 {
		s.CurrentCnt += uint64(n)
	} else if n < 0 {
		s.CurrentCnt -= uint64(-n)
	}

	if s.CurrentCnt > s.HighWatermarkCnt {
		s.HighWatermarkCnt = s.CurrentCnt
	}
}

func (s *sinkerStats) updateBytes(n int) {
	if n > 0 {
		s.CurrentBytes += uint64(n)
	} else if n < 0 {
		s.CurrentBytes -= uint64(-n)
	}
	if s.CurrentBytes > s.HighWatermarkBytes {
		s.HighWatermarkBytes = s.CurrentBytes
	}
}

type Sinker struct {
	schema struct {
		attrs      []string
		attrTypes  []types.Type
		sortKeyIdx int
	}
	config struct {
		allMergeSorted bool
		dedupAll       bool
		bufferSizeCap  int
		tailSizeCap    int
	}
	fSinker struct {
		executor FileSinker
		factory  FileSinkerFactory
	}
	staged struct {
		inMemStats          sinkerStats
		inMemory            []*batch.Batch
		persisted           []objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
	}
	result struct {
		persisted []objectio.ObjectStats
		tail      []*batch.Batch
	}

	buf struct {
		isOwner  bool
		bufStats sinkerStats
		buffers  *containers.OneSchemaBatchBuffer
	}

	pipe struct {
		enabled     bool
		sinkWorkers int
		maxPending  int

		started  bool
		ctx      context.Context
		cancel   context.CancelFunc
		sinkChan chan *pipelineSinkJob
		syncChan chan *pipelineSyncJob
		wg       sync.WaitGroup

		mu        sync.Mutex
		persisted []objectio.ObjectStats
		err       error
	}

	timing struct {
		spillCount int64 // atomic
		sortNs     int64 // atomic, nanoseconds
		sinkNs     int64 // atomic, nanoseconds
		syncNs     int64 // atomic, nanoseconds
		waitNs     int64 // atomic, nanoseconds (main goroutine blocked on submit)
		spillNs    int64 // atomic, nanoseconds (total wall time in trySpill)
	}

	mp *mpool.MPool
	fs fileservice.FileService
}

func (sinker *Sinker) fillDefaults() {
	if sinker.staged.memorySizeThreshold == 0 {
		sinker.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}

	sinker.staged.inMemStats.Name = "staged inmem stats"
	sinker.buf.bufStats.Name = "buffer stats"

	if sinker.buf.buffers == nil {
		sinker.buf.isOwner = true
		sinker.buf.buffers = containers.NewOneSchemaBatchBuffer(
			sinker.config.bufferSizeCap,
			sinker.schema.attrs,
			sinker.schema.attrTypes,
		)
	}
}

func (sinker *Sinker) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return sinker.result.persisted, sinker.result.tail
}

func (sinker *Sinker) fetchBuffer() *batch.Batch {
	x := sinker.buf.buffers.Len()
	bat := sinker.buf.buffers.Fetch()
	y := sinker.buf.buffers.Len()

	if x < y {
		sinker.buf.bufStats.updateCount(-1)
		sinker.buf.bufStats.updateBytes(-bat.Size())
	}

	return bat
}

func (sinker *Sinker) putBackBuffer(bat *batch.Batch) {
	x := sinker.buf.buffers.Len()
	sinker.buf.buffers.Putback(bat, sinker.mp)
	y := sinker.buf.buffers.Len()

	if x > y {
		sinker.buf.bufStats.updateCount(1)
		sinker.buf.bufStats.updateBytes(bat.Size())
	}
}

func (sinker *Sinker) popStaged() *batch.Batch {
	if len(sinker.staged.inMemory) == 0 {
		return nil
	}

	ret := sinker.staged.inMemory[len(sinker.staged.inMemory)-1]
	sinker.staged.inMemory = sinker.staged.inMemory[:len(sinker.staged.inMemory)-1]
	sinker.staged.inMemorySize -= ret.Size()

	sinker.staged.inMemStats.updateCount(-1)
	sinker.staged.inMemStats.updateBytes(-ret.Size())

	return ret
}

// pushStaged take the ownership of the bat
func (sinker *Sinker) pushStaged(
	ctx context.Context, bat *batch.Batch,
) error {

	sinker.staged.inMemStats.updateCount(1)
	sinker.staged.inMemStats.updateBytes(bat.Size())

	sinker.staged.inMemory = append(sinker.staged.inMemory, bat)
	sinker.staged.inMemorySize += bat.Size()
	if sinker.staged.inMemorySize >= sinker.staged.memorySizeThreshold {
		return sinker.trySpill(ctx)
	}
	return nil
}

func (sinker *Sinker) clearInMemoryStaged() {
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) cleanupInMemoryStaged() {
	for i, bat := range sinker.staged.inMemory {
		sinker.putBackBuffer(bat)
		sinker.staged.inMemory[i] = nil
	}
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) trySortInMemoryStaged(ctx context.Context) error {
	if sinker.schema.sortKeyIdx == -1 {
		return nil
	}
	for _, bat := range sinker.staged.inMemory {
		if err := mergeutil.SortColumnsByIndex(
			bat.Vecs,
			sinker.schema.sortKeyIdx,
			sinker.mp,
		); err != nil {
			return err
		}
	}
	return nil
}

// pipeline job types

type pipelineSinkJob struct {
	data []*batch.Batch
}

type pipelineSyncJob struct {
	fSinker FileSinker
}

func (sinker *Sinker) startPipeline(ctx context.Context) {
	sinker.pipe.ctx, sinker.pipe.cancel = context.WithCancel(ctx)
	sinker.pipe.sinkChan = make(chan *pipelineSinkJob, sinker.pipe.sinkWorkers)
	sinker.pipe.syncChan = make(chan *pipelineSyncJob, sinker.pipe.maxPending)
	sinker.pipe.started = true

	for i := 0; i < sinker.pipe.sinkWorkers; i++ {
		sinker.pipe.wg.Add(1)
		go sinker.pipelineSinkWorker()
	}
	sinker.pipe.wg.Add(1)
	go sinker.pipelineSyncWorker()
}

func (sinker *Sinker) pipelineSinkWorker() {
	defer sinker.pipe.wg.Done()
	for job := range sinker.pipe.sinkChan {
		if sinker.pipelineHasError() {
			sinker.freePipelineBatches(job.data)
			continue
		}

		sinkStart := time.Now()
		fSinker := sinker.fSinker.factory(sinker.mp, sinker.fs)
		var sinkErr error
		for _, bat := range job.data {
			if err := fSinker.Sink(sinker.pipe.ctx, bat); err != nil {
				sinkErr = err
				break
			}
		}
		atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
		sinker.freePipelineBatches(job.data)

		if sinkErr != nil {
			fSinker.Close()
			sinker.setPipelineError(sinkErr)
			continue
		}

		select {
		case sinker.pipe.syncChan <- &pipelineSyncJob{fSinker: fSinker}:
		case <-sinker.pipe.ctx.Done():
			fSinker.Close()
		}
	}
}

func (sinker *Sinker) pipelineSyncWorker() {
	defer sinker.pipe.wg.Done()
	for job := range sinker.pipe.syncChan {
		if sinker.pipelineHasError() {
			job.fSinker.Close()
			continue
		}

		syncStart := time.Now()
		stats, err := job.fSinker.Sync(sinker.pipe.ctx)
		atomic.AddInt64(&sinker.timing.syncNs, int64(time.Since(syncStart)))
		job.fSinker.Close()

		if err != nil {
			sinker.setPipelineError(err)
			continue
		}

		sinker.pipe.mu.Lock()
		sinker.pipe.persisted = append(sinker.pipe.persisted, *stats)
		sinker.pipe.mu.Unlock()
	}
}

func (sinker *Sinker) pipelineHasError() bool {
	sinker.pipe.mu.Lock()
	defer sinker.pipe.mu.Unlock()
	return sinker.pipe.err != nil
}

func (sinker *Sinker) pipelineError() error {
	sinker.pipe.mu.Lock()
	defer sinker.pipe.mu.Unlock()
	return sinker.pipe.err
}

func (sinker *Sinker) setPipelineError(err error) {
	sinker.pipe.mu.Lock()
	if sinker.pipe.err == nil {
		sinker.pipe.err = err
	}
	sinker.pipe.mu.Unlock()
	sinker.pipe.cancel()
}

func (sinker *Sinker) drainPipeline() error {
	if !sinker.pipe.started {
		return nil
	}
	close(sinker.pipe.sinkChan)
	sinker.pipe.wg.Wait()
	close(sinker.pipe.syncChan)
	return sinker.pipe.err
}

func (sinker *Sinker) freePipelineBatches(batches []*batch.Batch) {
	for _, bat := range batches {
		if bat != nil {
			bat.Clean(sinker.mp)
		}
	}
}

func (sinker *Sinker) pipelineSubmit(ctx context.Context, data []*batch.Batch) error {
	if err := sinker.pipelineError(); err != nil {
		sinker.freePipelineBatches(data)
		return err
	}
	waitStart := time.Now()
	select {
	case sinker.pipe.sinkChan <- &pipelineSinkJob{data: data}:
		atomic.AddInt64(&sinker.timing.waitNs, int64(time.Since(waitStart)))
		return nil
	case <-sinker.pipe.ctx.Done():
		sinker.freePipelineBatches(data)
		return sinker.pipelineError()
	case <-ctx.Done():
		sinker.freePipelineBatches(data)
		return context.Cause(ctx)
	}
}

func (sinker *Sinker) trySpill(ctx context.Context) error {
	spillStart := time.Now()
	defer func() {
		atomic.AddInt64(&sinker.timing.spillCount, 1)
		atomic.AddInt64(&sinker.timing.spillNs, int64(time.Since(spillStart)))
	}()

	// sort all in memory data
	sortStart := time.Now()
	if err := sinker.trySortInMemoryStaged(ctx); err != nil {
		return err
	}

	defer sinker.cleanupInMemoryStaged()
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) error {
		oneSorted := sinker.fetchBuffer()
		_, err := oneSorted.AppendWithCopy(ctx, sinker.mp, data)
		if err != nil {
			sinker.putBackBuffer(oneSorted)
			return err
		}
		sorted = append(sorted, oneSorted)
		return nil
	}

	defer func() {
		for i, bat := range sorted {
			sinker.putBackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	data := sinker.staged.inMemory

	// 1. merge sort
	if sinker.schema.sortKeyIdx != -1 {
		buffer := sinker.fetchBuffer() // note the lifecycle of buffer
		defer sinker.putBackBuffer(buffer)
		if err := mergeutil.MergeSortBatches(
			sinker.staged.inMemory,
			sinker.schema.sortKeyIdx,
			buffer,
			innersinker,
			sinker.mp,
			true,
		); err != nil {
			return err
		}
		data = sorted
	}

	// 3. dedup
	if sinker.config.dedupAll {
		if err := containers.DedupSortedBatches(
			sinker.schema.sortKeyIdx,
			data,
		); err != nil {
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))

	// pipeline path: hand off serialization + IO to workers
	if sinker.pipe.enabled {
		if !sinker.pipe.started {
			sinker.startPipeline(ctx)
		}

		// transfer batch ownership to pipeline workers
		var jobData []*batch.Batch
		if sinker.schema.sortKeyIdx != -1 {
			// sorted[] contains copies from merge sort — steal them
			jobData = make([]*batch.Batch, len(sorted))
			copy(jobData, sorted)
			// prevent the defer from freeing these batches
			for i := range sorted {
				sorted[i] = nil
			}
			sorted = sorted[:0]
		} else {
			// no sort key — steal in-memory staged batches directly
			jobData = make([]*batch.Batch, len(sinker.staged.inMemory))
			copy(jobData, sinker.staged.inMemory)
			// prevent cleanupInMemoryStaged from freeing them
			for i := range sinker.staged.inMemory {
				sinker.staged.inMemory[i] = nil
			}
			sinker.staged.inMemory = sinker.staged.inMemory[:0]
			sinker.staged.inMemorySize = 0
		}

		return sinker.pipelineSubmit(ctx, jobData)
	}

	// synchronous path: serialize + write in the current goroutine
	sinkStart := time.Now()
	fSinker := sinker.getStageFileSinker()
	defer sinker.resetFileSinker()
	for _, bat := range data {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))

	syncStart := time.Now()
	stats, err := fSinker.Sync(ctx)
	atomic.AddInt64(&sinker.timing.syncNs, int64(time.Since(syncStart)))
	if err != nil {
		return err
	}
	sinker.staged.persisted = append(sinker.staged.persisted, *stats)

	return nil
}

func (sinker *Sinker) resetFileSinker() {
	sinker.fSinker.executor.Reset()
}
func (sinker *Sinker) getStageFileSinker() FileSinker {
	if sinker.fSinker.executor == nil {
		sinker.fSinker.executor = sinker.fSinker.factory(sinker.mp, sinker.fs)
	}
	return sinker.fSinker.executor
}

// Write always copy the data
func (sinker *Sinker) Write(
	ctx context.Context,
	data *batch.Batch,
) (err error) {
	var curr *batch.Batch
	defer func() {
		if err != nil && curr != nil {
			sinker.putBackBuffer(curr)
		}
	}()

	offset := 0
	left := data.RowCount()
	for left > 0 {
		if curr == nil {
			curr = sinker.popStaged()
			if curr == nil {
				curr = sinker.fetchBuffer()
			} else if curr.RowCount() == objectio.BlockMaxRows {
				if err = sinker.pushStaged(ctx, curr); err != nil {
					return
				}
				curr = sinker.fetchBuffer()
			}
		}

		toAdd := left
		currPos := curr.RowCount()
		if currPos+toAdd > objectio.BlockMaxRows {
			toAdd = objectio.BlockMaxRows - currPos
		}
		if err = curr.UnionWindow(data, offset, toAdd, sinker.mp); err != nil {
			return
		}
		if curr.RowCount() == objectio.BlockMaxRows {
			if err = sinker.pushStaged(ctx, curr); err != nil {
				return
			}
			curr = nil
		}
		left -= toAdd
		offset += toAdd
	}
	if curr != nil && curr.RowCount() > 0 {
		if err = sinker.pushStaged(ctx, curr); err != nil {
			return
		}
	}
	return
}

func (sinker *Sinker) Sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if sinker.pipe.enabled && sinker.pipe.started {
		// check pipeline error before proceeding
		if err := sinker.pipelineError(); err != nil {
			return err
		}
	}
	if len(sinker.staged.persisted) == 0 && len(sinker.staged.inMemory) == 0 &&
		(!sinker.pipe.enabled || !sinker.pipe.started) {
		return nil
	}
	// spill the remaining data
	if sinker.staged.inMemorySize > 0 &&
		sinker.staged.inMemorySize >= sinker.config.tailSizeCap {
		if err := sinker.trySpill(ctx); err != nil {
			return err
		}
	} else {
		if err := sinker.trySortInMemoryStaged(ctx); err != nil {
			return err
		}
		if sinker.config.dedupAll {
			if err := containers.DedupSortedBatches(
				sinker.schema.sortKeyIdx,
				sinker.staged.inMemory,
			); err != nil {
				return err
			}
		}
		sinker.result.tail = sinker.staged.inMemory
		sinker.clearInMemoryStaged()
	}

	// drain pipeline and collect results
	if sinker.pipe.enabled && sinker.pipe.started {
		if err := sinker.drainPipeline(); err != nil {
			return err
		}
		sinker.staged.persisted = append(sinker.staged.persisted, sinker.pipe.persisted...)
	}

	defer func() {
		sinker.staged.persisted = sinker.staged.persisted[:0]
	}()

	spillCount := atomic.LoadInt64(&sinker.timing.spillCount)
	if spillCount > 0 {
		logutil.Info("Sinker flush stats",
			zap.Bool("pipeline", sinker.pipe.enabled),
			zap.Int("sinkWorkers", sinker.pipe.sinkWorkers),
			zap.Int64("spills", spillCount),
			zap.Duration("sortTime", time.Duration(atomic.LoadInt64(&sinker.timing.sortNs))),
			zap.Duration("serializeTime", time.Duration(atomic.LoadInt64(&sinker.timing.sinkNs))),
			zap.Duration("ioTime", time.Duration(atomic.LoadInt64(&sinker.timing.syncNs))),
			zap.Duration("submitWaitTime", time.Duration(atomic.LoadInt64(&sinker.timing.waitNs))),
			zap.Duration("totalSpillTime", time.Duration(atomic.LoadInt64(&sinker.timing.spillNs))),
			zap.Int("objects", len(sinker.staged.persisted)),
		)
	}

	// if there is only one file, it is sorted an deduped
	if len(sinker.staged.persisted) == 1 {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted[0])
		return nil
	}

	if !sinker.config.allMergeSorted && !sinker.config.dedupAll {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted...)
		return nil
	}
	panic("not implemented")
	// TODO: merge the files and dedup
	// newPersied, err := MergeSortedFilesAndDedup(sinker.staged.persisted)
	// if err != nil {
	// 	return err
	// }
	// sinker.results = append(sinker.results, newPersied...)
	//return nil
}

func (sinker *Sinker) Close() error {
	if sinker.pipe.enabled && sinker.pipe.started {
		sinker.drainPipeline()
	}
	sinker.cleanupInMemoryStaged()
	if sinker.buf.buffers != nil {
		if sinker.buf.isOwner {
			// it's not safe to free a shared buffer
			sinker.buf.buffers.Close(sinker.mp)
		}

		sinker.buf.buffers = nil
	}
	for i := range sinker.result.tail {
		if sinker.result.tail[i] != nil {
			sinker.result.tail[i].Clean(sinker.mp)
			sinker.result.tail[i] = nil
		}
	}
	sinker.result.tail = nil
	sinker.staged.persisted = nil
	if sinker.fSinker.executor != nil {
		sinker.fSinker.executor.Close()
		sinker.fSinker.executor = nil
	}
	sinker.fSinker.factory = nil
	sinker.mp = nil
	sinker.fs = nil
	return nil
}
