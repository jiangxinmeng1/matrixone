// Copyright 2023 Matrix Origin
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

package merge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

type activeTaskStats map[uint64]struct {
	blk      int
	estBytes int
}

// executor consider resources to decide to merge or not.
type executor struct {
	tableName           string
	rt                  *dbutils.Runtime
	cnSched             CNMergeScheduler
	memLimit            int
	memUsing            int
	transPageLimit      uint64
	cpuPercent          float64
	activeMergeBlkCount atomic.Int32
	activeEstimateBytes atomic.Int64
	roundMergeRows      uint64
	taskConsume         struct {
		sync.Mutex
		o map[objectio.ObjectId]struct{}
		m activeTaskStats
	}
}

func newMergeExecutor(rt *dbutils.Runtime, sched CNMergeScheduler) *executor {
	return &executor{
		rt:      rt,
		cnSched: sched,
	}
}

func (e *executor) setMemLimit(total uint64) {
	containerMLimit, err := memlimit.FromCgroup()
	if containerMLimit > 0 && containerMLimit < total {
		e.memLimit = int(float64(containerMLimit) * 0.9)
	} else {
		e.memLimit = int(float64(total) * 0.9)
	}

	if e.memLimit > 200*common.Const1GBytes {
		e.transPageLimit = uint64(e.memLimit / 25 * 2) // 8%
	} else if e.memLimit > 100*common.Const1GBytes {
		e.transPageLimit = uint64(e.memLimit / 25 * 3) // 12%
	} else if e.memLimit > 40*common.Const1GBytes {
		e.transPageLimit = uint64(e.memLimit / 25 * 4) // 16%
	} else {
		e.transPageLimit = math.MaxUint64 // no limit
	}

	logutil.Info(
		"MergeExecutorMemoryInfo",
		common.AnyField("container-limit", common.HumanReadableBytes(int(containerMLimit))),
		common.AnyField("host-memory", common.HumanReadableBytes(int(total))),
		common.AnyField("process-limit", common.HumanReadableBytes(e.memLimit)),
		common.AnyField("transfer-page-limit", common.HumanReadableBytes(int(e.transPageLimit))),
		common.AnyField("error", err),
	)
}

var proc *process.Process

func (e *executor) refreshMemInfo() {
	if proc == nil {
		proc, _ = process.NewProcess(int32(os.Getpid()))
	} else if mem, err := proc.MemoryInfo(); err == nil {
		e.memUsing = int(mem.RSS)
	}

	if e.memLimit == 0 {
		if stats, err := mem.VirtualMemory(); err == nil {
			e.setMemLimit(stats.Total)
		}
	}

	if percents, err := cpu.Percent(0, false); err == nil {
		e.cpuPercent = percents[0]
	}
	e.roundMergeRows = 0
}

func (e *executor) printStats() {
	cnt := e.activeMergeBlkCount.Load()
	if cnt == 0 && e.memAvailBytes() > 512*common.Const1MBytes {
		return
	}

	logutil.Info(
		"MergeExecutorMemoryStats",
		common.AnyField("process-limit", common.HumanReadableBytes(e.memLimit)),
		common.AnyField("process-mem", common.HumanReadableBytes(e.memUsing)),
		common.AnyField("inuse-mem", common.HumanReadableBytes(int(e.activeEstimateBytes.Load()))),
		common.AnyField("inuse-cnt", cnt),
	)
}

func (e *executor) addActiveTask(taskId uint64, blkn, esize int) {
	e.activeEstimateBytes.Add(int64(esize))
	e.activeMergeBlkCount.Add(int32(blkn))
	e.taskConsume.Lock()
	if e.taskConsume.m == nil {
		e.taskConsume.m = make(activeTaskStats)
	}
	e.taskConsume.m[taskId] = struct {
		blk      int
		estBytes int
	}{blkn, esize}
	e.taskConsume.Unlock()
}

func (e *executor) OnExecDone(v any) {
	task := v.(tasks.MScopedTask)

	e.taskConsume.Lock()
	stat := e.taskConsume.m[task.ID()]
	delete(e.taskConsume.m, task.ID())
	e.taskConsume.Unlock()

	e.activeMergeBlkCount.Add(-int32(stat.blk))
	e.activeEstimateBytes.Add(-int64(stat.estBytes))
}

func (e *executor) executeFor(entry *catalog.TableEntry, mobjs []*catalog.ObjectEntry, kind TaskHostKind) {
	if e.roundMergeRows*36 /*28 * 1.3 */ > e.transPageLimit/8 {
		return
	}
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema(false).Name)

	if ActiveCNObj.CheckOverlapOnCNActive(mobjs) {
		return
	}

	if kind == TaskHostCN {
		osize, esize := estimateMergeConsume(mobjs)
		blkCnt := 0
		for _, obj := range mobjs {
			blkCnt += obj.BlockCnt()
		}
		stats := make([][]byte, 0, len(mobjs))
		cids := make([]common.ID, 0, len(mobjs))
		for _, obj := range mobjs {
			stat := *obj.GetObjectStats()
			stats = append(stats, stat[:])
			cids = append(cids, *obj.AsCommonID())
		}
		if e.rt.Scheduler.CheckAsyncScopes(cids) != nil {
			return
		}
		schema := entry.GetLastestSchema(false)
		cntask := &api.MergeTaskEntry{
			AccountId:         schema.AcInfo.TenantID,
			UserId:            schema.AcInfo.UserID,
			RoleId:            schema.AcInfo.RoleID,
			TblId:             entry.ID,
			DbId:              entry.GetDB().GetID(),
			TableName:         entry.GetLastestSchema(false).Name,
			DbName:            entry.GetDB().GetName(),
			ToMergeObjs:       stats,
			EstimatedMemUsage: uint64(esize),
		}
		if err := e.cnSched.SendMergeTask(context.TODO(), cntask); err == nil {
			ActiveCNObj.AddActiveCNObj(mobjs)
			logMergeTask(e.tableName, math.MaxUint64, mobjs, blkCnt, osize, esize)
		} else {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("send-cn-task"),
				common.AnyField("task", fmt.Sprintf("table-%d-%s", cntask.TblId, cntask.TableName)),
				common.AnyField("error", err),
			)
			return
		}
		entry.Stats.SetLastMergeTime()
	} else {
		objScopes := make([]common.ID, 0)
		tombstoneScopes := make([]common.ID, 0)
		objs := make([]*catalog.ObjectEntry, 0)
		tombstones := make([]*catalog.ObjectEntry, 0)
		objectBlkCnt := 0
		tombstoneBlkCnt := 0
		for _, obj := range mobjs {
			if obj.IsTombstone {
				tombstoneBlkCnt += obj.BlockCnt()
				tombstones = append(tombstones, obj)
				tombstoneScopes = append(tombstoneScopes, *obj.AsCommonID())
			} else {
				objectBlkCnt += obj.BlockCnt()
				objs = append(objs, obj)
				objScopes = append(objScopes, *obj.AsCommonID())
			}
		}

		if len(objs) > 0 {
			e.scheduleMergeObjects(objScopes, objs, objectBlkCnt, entry, false)
		}
		if len(tombstones) > 1 {
			e.scheduleMergeObjects(tombstoneScopes, tombstones, tombstoneBlkCnt, entry, true)
		}
	}
}
func (e *executor) scheduleMergeObjects(scopes []common.ID, mobjs []*catalog.ObjectEntry, blkCnt int, entry *catalog.TableEntry, isTombstone bool) {
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt, common.DefaultMaxOsizeObjMB*common.Const1MBytes, isTombstone)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTaskWithObserver(nil, tasks.DataCompactionTask, scopes, factory, e)
	if err != nil {
		if !errors.Is(err, tasks.ErrScheduleScopeConflict) {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("schedule-merge-task"),
				common.AnyField("error", err),
				common.AnyField("task", task.Name()),
			)
		}
		return
	}
	osize, esize := estimateMergeConsume(mobjs)
	e.addActiveTask(task.ID(), blkCnt, esize)
	for _, obj := range mobjs {
		e.roundMergeRows += uint64(obj.Rows())
	}
	logMergeTask(e.tableName, task.ID(), mobjs, blkCnt, osize, esize)
	entry.Stats.SetLastMergeTime()
}

func (e *executor) memAvailBytes() int {
	merging := int(e.activeEstimateBytes.Load())
	avail := e.memLimit - e.memUsing - merging
	if avail < 0 {
		avail = 0
	}
	return avail
}

func (e *executor) transferPageSizeLimit() uint64 {
	return e.transPageLimit
}

func (e *executor) CPUPercent() int64 {
	return int64(e.cpuPercent)
}

func logMergeTask(name string, taskId uint64, merges []*catalog.ObjectEntry, blkn, osize, esize int) {
	rows := 0
	infoBuf := &bytes.Buffer{}
	for _, obj := range merges {
		r := int(obj.Rows())
		rows += r
		infoBuf.WriteString(fmt.Sprintf(" %d(%s)", r, obj.ID().ShortStringEx()))
	}
	platform := fmt.Sprintf("t%d", taskId)
	if taskId == math.MaxUint64 {
		platform = "CN"
		v2.TaskCNMergeScheduledByCounter.Inc()
		v2.TaskCNMergedSizeCounter.Add(float64(osize))
	} else {
		v2.TaskDNMergeScheduledByCounter.Inc()
		v2.TaskDNMergedSizeCounter.Add(float64(osize))
	}
	logutil.Info(
		"MergeExecutor",
		common.OperationField("schedule-merge-task"),
		common.AnyField("name", name),
		common.AnyField("platform", platform),
		common.AnyField("num-obj", len(merges)),
		common.AnyField("num-blk", blkn),
		common.AnyField("orig-size", common.HumanReadableBytes(osize)),
		common.AnyField("est-size", common.HumanReadableBytes(esize)),
		common.AnyField("rows", rows),
		common.AnyField("info", infoBuf.String()),
	)
}
