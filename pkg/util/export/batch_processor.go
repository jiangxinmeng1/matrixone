// Copyright 2022 Matrix Origin
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

package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	morun "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/ring"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

// defaultRingBufferSize need 2^N value
// OLD Case: defaultQueueSize default length for collect Channel (value = 1310720)
// NEW Case:
// 1. [consume] one goroutine collect items ~= 1s / 50us = 20_000
// 2. [generate] statement_info << query qps
// 3. [generate] rawlog qps ~= [1000, 3000]
// 4. [generate] metric 1ps = 60s / 15s = 4
const defaultRingBufferSize = 2 << 13

const LoggerNameMOCollector = "MOCollector"

const discardCollectTimeout = time.Millisecond
const discardCollectRetry = time.Millisecond / 10

// bufferHolder hold ItemBuffer content, handle buffer's new/flush/reset/reminder(base on timer) operations.
// work like:
// ---> Add ---> ShouldFlush or trigger.signal -----> StopAndGetBatch ---> FlushAndReset ---> Add ---> ...
// #     ^                   |No                |Yes, go next call
// #     |<------------------/Accept next Add
type bufferHolder struct {
	c   *MOCollector
	ctx context.Context
	// background loop
	bgCtx    context.Context
	bgCancel context.CancelFunc
	// name like a type
	name string
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer motrace.Buffer
	// bufferPool
	bufferPool *sync.Pool
	bufferCnt  atomic.Int32
	discardCnt atomic.Int32
	// reminder
	reminder batchpipe.Reminder
	// signal send signal to Collector
	signal bufferSignalFunc // see awakeBufferFactory
	// impl NewItemBatchHandler
	impl motrace.PipeImpl
	// trigger handle Reminder strategy
	trigger *time.Timer

	aggr table.Aggregator

	mux sync.Mutex
	// stopped mark
	stopped bool
}

type bufferSignalFunc func(*bufferHolder)

func newBufferHolder(ctx context.Context, name batchpipe.HasName, impl motrace.PipeImpl, signal bufferSignalFunc, c *MOCollector) *bufferHolder {
	b := &bufferHolder{
		c:      c,
		ctx:    ctx,
		name:   name.GetName(),
		signal: signal,
		impl:   impl,
	}
	b.bgCtx, b.bgCancel = context.WithCancel(b.ctx)
	b.bufferPool = &sync.Pool{}
	b.bufferCnt.Swap(0)
	b.bufferPool.New = func() interface{} {
		return b.impl.NewItemBuffer(b.name)
	}
	b.buffer = b.getBuffer()
	b.reminder = b.buffer.(batchpipe.Reminder)
	b.mux.Lock()
	defer b.mux.Unlock()
	b.trigger = time.AfterFunc(time.Hour, func() {})
	b.aggr = impl.NewAggregator(ctx, b.name)
	return b
}

// Start separated from newBufferHolder, should call only once, fix trigger started before first Add
func (b *bufferHolder) Start() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.stopped = false
	b.trigger.Stop()
	b.trigger = time.AfterFunc(b.reminder.RemindNextAfter(), func() {
		if b.mux.TryLock() {
			b.mux.Unlock()
		}
		v2.GetTraceCollectorSignalTotal(b.name, "trigger").Inc()
		b.signal(b)
	})
	if b.aggr != nil {
		go b.loopAggr()
	}
}

func (b *bufferHolder) getBuffer() motrace.Buffer {
	b.c.allocBuffer()
	b.bufferCnt.Add(1)
	buffer := b.bufferPool.Get().(motrace.Buffer)
	b.logStatus("new buffer")
	return buffer
}

func (b *bufferHolder) putBuffer(buffer motrace.Buffer) {
	buffer.Reset()
	b.bufferPool.Put(buffer)
	b.bufferCnt.Add(-1)
	b.c.releaseBuffer()
	b.logStatus("release buffer")
}

func (b *bufferHolder) logStatus(msg string) {
	if b.c.logger.Enabled(zap.DebugLevel) {
		b.c.logger.Debug(msg,
			zap.String("item", b.name),
			zap.Int32("cnt", b.bufferCnt.Load()),
			zap.Int32("using", b.c.bufferTotal.Load()),
		)
	}
}

func (b *bufferHolder) discardBuffer(buffer motrace.Buffer) {
	b.discardCnt.Add(1)
	b.putBuffer(buffer)
}

// Add call buffer.Add(), while bufferHolder is NOT readonly
func (b *bufferHolder) Add(item batchpipe.HasName, needAggr bool) {
	locked := true
	b.mux.Lock()
	unlock := func() {
		if locked {
			locked = false
			b.mux.Unlock()
		}
	}
	defer unlock()
	defer func() {
		if err := recover(); err != nil {
			_ = moerr.ConvertPanicError(b.ctx, err)
			logutil.Error("catch panic #19755")
			if b.buffer != nil {
				b.putBuffer(b.buffer)
			}
			b.buffer = nil
		}
	}()
	if b.stopped {
		return
	}

	if b.buffer == nil {
		b.buffer = b.getBuffer()
	}

	if b.aggr != nil && needAggr {
		if i, ok := item.(table.Item); ok {
			_, err := b.aggr.AddItem(i)
			if err == nil {
				// aggred, then return
				i.Free()
				return
			}
		}
	}

	buf := b.buffer
	buf.Add(item)
	unlock()
	if buf.ShouldFlush() {
		v2.GetTraceCollectorSignalTotal(b.name, "flush").Inc()
		b.signal(b)
	} else if checker, is := item.(table.NeedSyncWrite); is && checker.NeedSyncWrite() {
		v2.GetTraceCollectorSignalTotal(b.name, "sync").Inc()
		b.signal(b)
	}
}

func (b *bufferHolder) loopAggr() {
	var (
		lastPrintTime time.Time
		logger        = b.c.logger.With(zap.String("name", b.name)).Named("bufferHolder")
		loopInterval  = time.Second
		printInterval = time.Minute * 10
		recordCnt     = 0
	)
	if b.aggr == nil {
		logger.Warn("no aggregator available, just quit this loop")
		return
	}
	counter := v2.GetTraceMOLoggerAggrCounter(b.name)
	logutil.Info("handle-aggr-start")
mainL:
	for {
		select {
		case <-time.After(loopInterval):
			// handle aggr
			end := time.Now().Truncate(b.aggr.GetWindow())
			results := b.aggr.PopResultsBeforeWindow(end)
			for _, item := range results {
				// tips: Add() will free the {item} obj.
				b.Add(item, false)
				counter.Inc()
			}
			recordCnt += len(results)
			if time.Since(lastPrintTime) > printInterval {
				logutil.Info(
					"handle-aggr",
					zap.Int("records", recordCnt),
					zap.Time("end", end),
				)
				recordCnt = 0
				lastPrintTime = time.Now()
			}
			// END> handle aggr

		case <-b.bgCtx.Done():
			// trigger by b.bgCancel
			logger.Info("handle-aggr-bgctx-done")
			break mainL
		case <-b.ctx.Done():
			logutil.Info("handle-aggr-ctx-done")
			break mainL
		}
	}
	logutil.Info("handle-aggr-exit")
}

var _ generateReq = (*bufferGenerateReq)(nil)

type bufferGenerateReq struct {
	// itemName name of buffer's item
	itemName string
	// buffer keep content
	buffer motrace.Buffer
	// impl NewItemBatchHandler
	b *bufferHolder
}

func (r *bufferGenerateReq) handle(buf *bytes.Buffer) (exportReq, error) {
	batch := r.buffer.GetBatch(r.b.ctx, buf)
	r.b.putBuffer(r.buffer)
	return &bufferExportReq{
		batch: batch,
		b:     r.b,
	}, nil
}

func (r *bufferGenerateReq) callback(err error) {}

func (r *bufferGenerateReq) typ() string { return r.itemName }

var _ exportReq = (*bufferExportReq)(nil)

type bufferExportReq struct {
	batch any
	b     *bufferHolder
}

func (r *bufferExportReq) handle() error {
	if r.batch != nil {
		var flush = r.b.impl.NewItemBatchHandler(context.Background())
		flush(r.batch)
	} else {
		r.b.c.logger.Debug("batch is nil", zap.String("item", r.b.name))
	}
	return nil
}

func (r *bufferExportReq) callback(err error) {}

// getGenerateReq get req to do generate logic
// return nil, if b.buffer is nil
func (b *bufferHolder) getGenerateReq() (req generateReq) {
	b.mux.Lock()
	defer b.mux.Unlock()
	defer b.resetTrigger()
	defer func() {
		if err := recover(); err != nil {
			_ = moerr.ConvertPanicError(b.ctx, err)
			logutil.Error("catch panic #19755")
			if b.buffer != nil {
				b.putBuffer(b.buffer)
			}
			b.buffer = nil
			req = nil
		}
	}()

	if b.buffer == nil || b.buffer.IsEmpty() {
		return nil
	}

	req = &bufferGenerateReq{
		buffer: b.buffer,
		b:      b,
		// impl generateReq.typ()
		itemName: b.name,
	}
	b.buffer = nil
	return req
}

func (b *bufferHolder) resetTrigger() {
	if b.stopped {
		return
	}
	b.trigger.Reset(b.reminder.RemindNextAfter())
}

// Stop all internal job, the loopAggr goroutine
// consume all aggr-ed result.
func (b *bufferHolder) Stop() {
	b.bgCancel() // stop loopAggr goroutine
	if b.aggr != nil {
		aggrResults := b.aggr.GetResults()
		b.c.logger.Info("handle last aggr stmt",
			zap.Int("records", len(aggrResults)),
			zap.String("name", b.name))
		for _, result := range aggrResults {
			b.Add(result, false)
		}
	}
	b.mux.Lock() // lock against b.Add(...)
	defer b.mux.Unlock()
	// mark stop
	b.stopped = true
	b.trigger.Stop()
}

var _ motrace.BatchProcessor = (*MOCollector)(nil)

// MOCollector handle all bufferPipe
type MOCollector struct {
	motrace.BatchProcessor
	ctx    context.Context
	logger *log.MOLogger

	// mux control all changes on buffers
	mux sync.RWMutex
	// buffers maintain working buffer for each type
	buffers map[string]*bufferHolder
	// awakeCollect handle collect signal
	awakeQueue *ring.RingBuffer[batchpipe.HasName]
	// awakeGenerate handle generate signal
	awakeGenerate chan generateReq
	// awakeBatch handle export signal
	awakeBatch chan exportReq

	collectorCnt int // WithCollectorCnt
	generatorCnt int // WithGeneratorCnt
	exporterCnt  int // WithExporterCnt
	// num % of (#cpu * 0.1)
	collectorCntP int // default: 10
	generatorCntP int // default: 20
	exporterCntP  int // default: 80
	// pipeImplHolder hold implement
	pipeImplHolder *PipeImplHolder

	statsInterval time.Duration // WithStatsInterval

	maxBufferCnt int32 // cooperate with bufferCond
	bufferTotal  atomic.Int32
	bufferMux    sync.Mutex
	bufferCond   *sync.Cond

	// flow control
	started  uint32
	stopOnce sync.Once
	stopWait sync.WaitGroup
	stopCh   chan struct{}
	stopDrop atomic.Uint64
}

type MOCollectorOption func(*MOCollector)

func NewMOCollector(
	ctx context.Context,
	service string,
	opts ...MOCollectorOption,
) *MOCollector {
	c := &MOCollector{
		ctx:            ctx,
		logger:         morun.ServiceRuntime(service).Logger().Named(LoggerNameMOCollector).With(logutil.Discardable()),
		buffers:        make(map[string]*bufferHolder),
		awakeQueue:     ring.NewRingBuffer[batchpipe.HasName](defaultRingBufferSize, ring.WithGoScheduleThreshold(1e5 /*~=1ms/10ns*/)),
		awakeGenerate:  make(chan generateReq, 16),
		awakeBatch:     make(chan exportReq),
		stopCh:         make(chan struct{}),
		collectorCntP:  10,
		generatorCntP:  20,
		exporterCntP:   80,
		pipeImplHolder: newPipeImplHolder(),
		statsInterval:  5 * time.Minute,
		maxBufferCnt:   int32(runtime.NumCPU()),
	}
	c.bufferCond = sync.NewCond(&c.bufferMux)
	for _, opt := range opts {
		opt(c)
	}
	// calculate collectorCnt, generatorCnt, exporterCnt
	c.calculateDefaultWorker(runtime.NumCPU())
	return c
}

const maxPercentValue = 1000

// calculateDefaultWorker
// totalNum = int( #cpu * 0.1 + 0.5 )
// default collectorCntP : generatorCntP : exporterCntP = 10 : 20 : 80.
// It means collectorCnt = int( $totalNum * $collectorCntP / 100 + 0.5 )
//
// For example
// | #cpu | #totalNum | collectorCnt | generatorCnt | exporterCnt |
// | --   | --        | --           | --           | --          |
// | 6    | 0.6 =~ 1  | 1            | 1            | 3           |
// | 30   | 3.0       | 1            | 1            | 2           |
// | 50   | 5.0       | 1            | 1            | 4           |
// | 60   | 6.0       | 1            | 2            | 5           |
func (c *MOCollector) calculateDefaultWorker(numCpu int) {
	var totalNum = math.Ceil(float64(numCpu) * 0.1)
	unit := float64(totalNum) / (100.0)
	// set default value if non-set
	c.collectorCnt = int(math.Round(unit * float64(c.collectorCntP)))
	c.generatorCnt = int(math.Round(unit * float64(c.generatorCntP)))
	c.exporterCnt = int(math.Round(unit * float64(c.exporterCntP)))
	if c.collectorCnt <= 0 {
		c.collectorCnt = 1
	}
	if c.generatorCnt <= 0 {
		c.generatorCnt = 1
	}
	if c.exporterCnt <= 0 {
		c.exporterCnt = 1
	}

	// check max value < numCpu
	if c.collectorCnt > numCpu {
		c.collectorCnt = numCpu
	}
	if c.generatorCnt > numCpu {
		c.generatorCnt = numCpu
	}
	if c.exporterCnt > numCpu {
		c.exporterCnt = numCpu
	}

	// last check: disable calculation
	if c.collectorCnt >= maxPercentValue {
		c.collectorCnt = numCpu
	}
	if c.generatorCntP >= maxPercentValue {
		c.generatorCnt = numCpu
	}
	if c.exporterCnt >= maxPercentValue {
		c.generatorCnt = numCpu
	}
}

func WithCollectorCntP(p int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.collectorCntP = p })
}
func WithGeneratorCntP(p int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.generatorCntP = p })
}
func WithExporterCntP(p int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.exporterCntP = p })
}

func WithOBCollectorConfig(cfg *config.OBCollectorConfig) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) {
		c.statsInterval = cfg.ShowStatsInterval.Duration
		c.maxBufferCnt = cfg.BufferCnt
		if c.maxBufferCnt == -1 {
			c.maxBufferCnt = math.MaxInt32
		} else if c.maxBufferCnt == 0 {
			c.maxBufferCnt = int32(runtime.NumCPU())
		}
		c.collectorCntP = cfg.CollectorCntPercent
		c.generatorCntP = cfg.GeneratorCntPercent
		c.exporterCntP = cfg.ExporterCntPercent
	})
}

func (c *MOCollector) initCnt() {
	if c.collectorCnt <= 0 {
		c.collectorCnt = c.pipeImplHolder.Size() * 2
	}
	if c.generatorCnt <= 0 {
		c.generatorCnt = c.pipeImplHolder.Size()
	}
	if c.exporterCnt <= 0 {
		c.exporterCnt = c.pipeImplHolder.Size()
	}
}

func (c *MOCollector) Register(name batchpipe.HasName, impl motrace.PipeImpl) {
	_ = c.pipeImplHolder.Put(name.GetName(), impl)
}

// Collect item in chan, if collector is stopped then return error
func (c *MOCollector) Collect(ctx context.Context, item batchpipe.HasName) error {
	start := time.Now()
	for {
		select {
		case <-c.stopCh:
			c.stopDrop.Add(1)
			ctx = errutil.ContextWithNoReport(ctx, true)
			return moerr.NewInternalError(ctx, "MOCollector stopped")
		default:
			ok, err := c.awakeQueue.Offer(item)
			if ok {
				v2.TraceCollectorCollectDurationHistogram.Observe(time.Since(start).Seconds())
				return nil
			}
			if err != nil {
				v2.GetTraceCollectorCollectHungCounter(item.GetName(), err.Error()).Inc()
			} else {
				v2.GetTraceCollectorCollectHungCounter(item.GetName(), "retry").Inc()
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// DiscardableCollect implements motrace.DiscardableCollector
// cooperate with logutil.Discardable() field
func (c *MOCollector) DiscardableCollect(ctx context.Context, item batchpipe.HasName) error {
	now := time.Now()
	for {
		select {
		case <-c.stopCh:
			c.stopDrop.Add(1)
			ctx = errutil.ContextWithNoReport(ctx, true)
			return moerr.NewInternalError(ctx, "MOCollector stopped")
		default:
			ok, _ := c.awakeQueue.Offer(item)
			if ok {
				return nil
			} else if time.Since(now) > discardCollectTimeout {
				v2.GetTraceCollectorDiscardItemCounter(item.GetName()).Inc()
				return nil
			}
			time.Sleep(discardCollectRetry)
		}
	}
}

// Start all goroutine worker, including collector, generator, and exporter
func (c *MOCollector) Start() bool {
	if atomic.LoadUint32(&c.started) != 0 {
		return false
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.started != 0 {
		return false
	}
	defer atomic.StoreUint32(&c.started, 1)

	c.initCnt()

	c.logger.Info("MOCollector Start")
	for i := 0; i < c.collectorCnt; i++ {
		c.stopWait.Add(1)
		go c.doCollect(i)
	}
	for i := 0; i < c.generatorCnt; i++ {
		c.stopWait.Add(1)
		go c.doGenerate(i)
	}
	for i := 0; i < c.exporterCnt; i++ {
		c.stopWait.Add(1)
		go c.doExport(i)
	}
	c.stopWait.Add(1)
	go c.showStats()
	return true
}

func (c *MOCollector) allocBuffer() {
	c.bufferCond.L.Lock()
	for c.bufferTotal.Load() == c.maxBufferCnt {
		c.bufferCond.Wait()
	}
	c.bufferTotal.Add(1)
	c.bufferCond.L.Unlock()
}

func (c *MOCollector) releaseBuffer() {
	c.bufferCond.L.Lock()
	c.bufferTotal.Add(-1)
	c.bufferCond.Signal()
	c.bufferCond.L.Unlock()
}

// doCollect handle all item accept work, send it to the corresponding buffer
// goroutine worker
func (c *MOCollector) doCollect(idx int) {
	var startWait = time.Now()
	defer c.stopWait.Done()
	ctx, span := trace.Start(c.ctx, "MOCollector.doCollect")
	defer span.End()
	c.logger.Debug("doCollect %dth: start", zap.Int("idx", idx))

loop:
	for {
		select {
		default:
			i, got, err := c.awakeQueue.Pop()
			if !got {
				if errors.Is(err, ring.ErrDisposed) {
					v2.TraceCollectorDisposedCounter.Inc()
				}
				if errors.Is(err, ring.ErrTimeout) {
					v2.TraceCollectorTimeoutCounter.Inc()
				}
				if errors.Is(err, ring.ErrEmpty) {
					v2.TraceCollectorEmptyCounter.Inc()
				}
				time.Sleep(time.Millisecond)
				continue
			}
			v2.GetTraceCollectorMOLoggerQueueLength().Set(float64(c.awakeQueue.Len()))
			start := time.Now()
			v2.TraceCollectorConsumeDelayDurationHistogram.Observe(start.Sub(startWait).Seconds())
			c.mux.RLock()
			if buf, has := c.buffers[i.GetName()]; !has {
				c.logger.Debug("doCollect: init buffer", zap.Int("idx", idx), zap.String("item", i.GetName()))
				c.mux.RUnlock()
				c.mux.Lock()
				if _, has := c.buffers[i.GetName()]; !has {
					c.logger.Debug("doCollect: init buffer done.", zap.Int("idx", idx))
					if impl, has := c.pipeImplHolder.Get(i.GetName()); !has {
						c.logger.Panic("unknown item type", zap.String("item", i.GetName()))
					} else {
						buf = newBufferHolder(ctx, i, impl, awakeBufferFactory(c), c)
						c.buffers[i.GetName()] = buf
						buf.Add(i, true)
						buf.Start()
					}
				}
				c.mux.Unlock()
			} else {
				buf.Add(i, true)
				c.mux.RUnlock()
			}
			v2.TraceCollectorConsumeDurationHistogram.Observe(time.Since(start).Seconds())
			startWait = time.Now() // next Round
		case <-c.stopCh:
			break loop
		}
	}
	c.logger.Debug("doCollect: Done.", zap.Int("idx", idx))
}

type generateReq interface {
	handle(*bytes.Buffer) (exportReq, error)
	callback(error)
	typ() string
}

type exportReq interface {
	handle() error
	callback(error)
}

// awakeBufferFactory frozen buffer, send GenRequest to awake
var awakeBufferFactory = func(c *MOCollector) func(holder *bufferHolder) {
	return func(holder *bufferHolder) {
		start := time.Now()
		defer func() {
			v2.TraceCollectorGenerateAwareDurationHistogram.Observe(time.Since(start).Seconds())
		}()
		v2.GetTraceCollectorSignalTotal(holder.name, "main").Inc()
		req := holder.getGenerateReq()
		if req == nil {
			return
		}
		if holder.name != motrace.RawLogTbl {
			select {
			case c.awakeGenerate <- req:
			case <-time.After(time.Second * 3):
				c.logger.Warn("awakeBufferFactory: timeout after 3 seconds")
				goto discardL
			}
		} else {
			select {
			case c.awakeGenerate <- req:
			default:
				c.logger.Warn("awakeBufferFactory: awakeGenerate chan is full")
				goto discardL
			}
		}
		return
	discardL:
		if r, ok := req.(*bufferGenerateReq); ok {
			r.b.discardBuffer(r.buffer)
		}
		v2.TraceCollectorGenerateAwareDiscardDurationHistogram.Observe(time.Since(start).Seconds())
	}
}

// doGenerate handle buffer gen BatchRequest, which could be anything
// goroutine worker
func (c *MOCollector) doGenerate(idx int) {
	defer c.stopWait.Done()
	var buf = new(bytes.Buffer)
	c.logger.Debug("doGenerate start", zap.Int("idx", idx))
loop:
	for {
		select {
		case req := <-c.awakeGenerate:
			v2.TraceCollectorExportQueueLength.Inc()
			start := time.Now()
			if req == nil {
				c.logger.Warn("generate req is nil")
				v2.TraceCollectorExportQueueLength.Dec()
			} else if exportReq, err := req.handle(buf); err != nil {
				req.callback(err)
				v2.TraceCollectorGenerateDurationHistogram.Observe(time.Since(start).Seconds())
				v2.TraceCollectorExportQueueLength.Dec()
			} else {
				startDelay := time.Now()
				select {
				case c.awakeBatch <- exportReq:
				case <-c.stopCh:
				case <-time.After(time.Second * 10):
					c.logger.Info("awakeBatch: timeout after 10 seconds")
					v2.TraceCollectorGenerateDiscardDurationHistogram.Observe(time.Since(start).Seconds())
					// fixme: do csv output, should NO discard case
					v2.GetTraceCollectorDiscardCounter(req.typ()).Inc()
					v2.TraceCollectorExportQueueLength.Dec()
				}
				end := time.Now()
				v2.TraceCollectorGenerateDelayDurationHistogram.Observe(end.Sub(startDelay).Seconds())
				v2.TraceCollectorGenerateDurationHistogram.Observe(end.Sub(start).Seconds())
			}
		case <-c.stopCh:
			break loop
		}
	}
	c.logger.Debug("doGenerate: Done.", zap.Int("idx", idx))
}

// doExport handle BatchRequest
func (c *MOCollector) doExport(idx int) {
	defer c.stopWait.Done()
	c.logger.Debug("doExport %dth: start", zap.Int("idx", idx))
loop:
	for {
		select {
		case req := <-c.awakeBatch:
			v2.TraceCollectorExportQueueLength.Dec()
			start := time.Now()
			if req == nil {
				c.logger.Warn("export req is nil")
			} else if err := req.handle(); err != nil {
				req.callback(err)
			}
			v2.TraceCollectorExportDurationHistogram.Observe(time.Since(start).Seconds())
		case <-c.stopCh:
			c.mux.Lock()
			for len(c.awakeBatch) > 0 {
				<-c.awakeBatch
			}
			c.mux.Unlock()
			break loop
		}
	}
	c.logger.Debug("doExport Done.", zap.Int("idx", idx))
}

func (c *MOCollector) showStats() {
	defer c.stopWait.Done()
	c.logger.Debug("start showStats")

loop:
	for {
		select {
		case <-time.After(c.statsInterval):
			fields := make([]zap.Field, 0, 16)
			fields = append(fields, zap.Int32("MaxBufferCnt", c.maxBufferCnt))
			fields = append(fields, zap.Int32("TotalBufferCnt", c.bufferTotal.Load()))
			fields = append(fields, zap.Int("QueueLength", int(c.awakeQueue.Len())))
			for _, b := range c.buffers {
				fields = append(fields, zap.Int32(fmt.Sprintf("%sBufferCnt", b.name), b.bufferCnt.Load()))
				fields = append(fields, zap.Int32(fmt.Sprintf("%sDiscardCnt", b.name), b.discardCnt.Load()))
			}
			c.logger.Info("stats", fields...)
		case <-c.stopCh:
			break loop
		}
	}
	c.logger.Debug("showStats Done.")
}

func (c *MOCollector) Stop(graceful bool) error {
	var err error
	var buf = new(bytes.Buffer)
	c.stopOnce.Do(func() {
		for c.awakeQueue.Len() > 0 && graceful {
			c.logger.Debug(fmt.Sprintf("doCollect left %d job", c.awakeQueue.Len()))
			time.Sleep(250 * time.Millisecond)
		}
		c.mux.Lock()
		for _, buffer := range c.buffers {
			buffer.Stop()
		}
		c.mux.Unlock()
		close(c.stopCh)
		c.stopWait.Wait()
		close(c.awakeGenerate)
		close(c.awakeBatch)
		if !graceful {
			// shutdown directly
			return
		}
		// handle remain data
		handleExport := func(req exportReq) {
			if err = req.handle(); err != nil {
				req.callback(err)
			}
		}
		handleGen := func(req generateReq) {
			if export, err := req.handle(buf); err != nil {
				req.callback(err)
			} else {
				handleExport(export)
			}
		}
		for req := range c.awakeBatch {
			handleExport(req)
		}
		for req := range c.awakeGenerate {
			handleGen(req)
		}
		for _, buffer := range c.buffers {
			if generate := buffer.getGenerateReq(); generate != nil {
				handleGen(generate)
			}
		}
	})
	return err
}

type PipeImplHolder struct {
	mux   sync.RWMutex
	impls map[string]motrace.PipeImpl
}

func newPipeImplHolder() *PipeImplHolder {
	return &PipeImplHolder{
		impls: make(map[string]motrace.PipeImpl),
	}
}

func (h *PipeImplHolder) Get(name string) (motrace.PipeImpl, bool) {
	h.mux.RLock()
	defer h.mux.RUnlock()
	impl, has := h.impls[name]
	return impl, has
}

func (h *PipeImplHolder) Put(name string, impl motrace.PipeImpl) bool {
	h.mux.Lock()
	defer h.mux.Unlock()
	_, has := h.impls[name]
	h.impls[name] = impl
	return has
}

func (h *PipeImplHolder) Size() int {
	h.mux.Lock()
	defer h.mux.Unlock()
	return len(h.impls)
}
