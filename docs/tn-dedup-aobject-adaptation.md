# TN 去重适配新的 aobject + less2 排序方案

## 1. 背景

当前分支 `object-list-refactor-new` 引入了两个关键重构：

**less2 排序机制**：用 `objectListRankAndTS()` 替代旧的 `max(CreatedAt, DeletedAt)` 比较，将 ObjectEntry 按「对象类型 + 时间戳」分为 6 个 Rank 层级。

**ObjectList 内部结构**：从 `maxTs_objectID map[ObjectId]TS` 改为 `latest_objectID map[ObjectId]*ObjectEntry`，通过 `prevVersion`/`nextVersion` 链维护同一 object 的版本关系。

去重代码需要适配这些变化，下面按改动点逐一说明现状和修改方案。

---

## 2. 背景：less2 排序机制

### 2.1 ObjectEntry.Less 比较器

```go
func (entry *ObjectEntry) Less(b *ObjectEntry) bool {
    r1, t1 := entry.objectListRankAndTS()
    r2, t2 := b.objectListRankAndTS()
    if r1 != r2 { return r1 < r2 }
    if !t1.EQ(&t2) { return t1.LT(&t2) }
    return bytes.Compare(entry.ObjectShortName()[:], b.ObjectShortName()[:]) < 0
}
```

### 2.2 Rank 含义

| Rank | 对象类型 | 排序键 |
|------|----------|--------|
| 0 | Appendable + CEntry + 无 DCounterpart（活跃 appender） | GetMinCommitTS 或 CreatedAt |
| 1 | Appendable + CEntry + 有 DCounterpart（已 flush） | GetMinCommitTS 或 CreatedAt |
| 2 | Appendable + DEntry（已删除） | DeletedAt |
| 3 | Non-Appendable + CEntry + 无 DCounterpart | CreatedAt |
| 4 | Non-Appendable + CEntry + 有 DCounterpart | CreatedAt |
| 5 | Non-Appendable + DEntry | DeletedAt |
| 6 | 未提交 | UncommitTS |

---

## 3. 改动一：incrementalGetRowsByPK 适配 less2 遍历顺序

### 涉及文件

`pkg/vm/engine/tae/txn/txnimpl/base_table.go`

> 注：`getRowsByPK`（snapshot dedup）全量遍历所有可见对象，没有 early break，遍历顺序不影响正确性，**不需要改动**。`incrementalGetRowsByPK`（precommit dedup）有 early break 逻辑，需要适配 less2。

### 现状

`base_table.go:214-288`，使用原始 btree 迭代器（`MakeDataObjectIt`），逆序遍历（Last → Prev）。按 `CreatedAt` 范围 + `IsAppendable` + `HasDropIntent` + `VisibleByTS` 过滤。对 appendable + 未删除 + `CreatedAt < from` 有 `earlybreak`：

```go
var earlybreak bool
for ok := objIt.Last(); ok; ok = objIt.Prev() {
    if earlybreak { break }
    obj := objIt.Item()

    if obj.CreatedAt.GT(&to) { continue }

    if obj.IsAppendable() {
        if !obj.HasDropIntent() && obj.CreatedAt.LT(&from) {
            earlybreak = true
        }
    } else if obj.CreatedAt.LT(&from) { continue }

    // skip category-b entries (replaced intermediate versions)
    if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil { continue }

    if !obj.VisibleByTS(to) { continue }
    // ... 去重 ...
}
```

### 问题

旧 btree 按 `max(CreatedAt, DeletedAt)` 排序，逆序遍历时 entry 大致按时间从新到旧排列，`earlybreak` 基于「appendable + CreatedAt < from」能正确停止。但现在 btree 按 less2 的 `(rank, entryTS)` 排序，逆序遍历顺序变为：

```
rank 5 → rank 4 → rank 3 → rank 2 → rank 1 → rank 0
```

旧代码用 `CreatedAt` 比较不再与 btree 遍历顺序对齐，且 CEntry/DEntry 不在 btree 中相邻，`GetPrevVersion()/GetNextVersion()` 的 category-b 过滤也需要替换。

### 改动方案

利用 less2 的 rank 语义重新组织过滤逻辑。

#### 遍历方向验证

btree 按 Less 升序排列（0 → 6）。逆序遍历 `Last() → Prev()` 的到达顺序为 `rank 6 → 5 → 4 → 3 → 2 → 1 → 0`。

同一个 rank 内部，entry 按 `entryTS` 升序排列，所以 `Last() → Prev()` 遍历时该 rank 内是从大到小（**entryTS 降序**）访问。

以 rank 2（aobj DEntry，排序键 DeletedAt）为例：

```
btree 升序:  [DeletedAt=100] → [DeletedAt=200] → [DeletedAt=300]  → rank 1 entries...
                                      ↑ btree last
Last→Prev:   DeletedAt=300 → 200 → 100 → rank 1 entries...
             从大到小（降序）
```

当 `from=150` 时：遇到 `DeletedAt=300 >= 150`（处理），`200 >= 150`（处理），`100 < 150` → 此 entry 及 rank 内后续所有 entry 的 DeletedAt 都 `< from`，全部 `continue` 跳过，自然进入 rank 1。

✅ **方向正确**：降序遍历时，第一个满足 `< from` 的 entry 就是该 rank 内的分界点。

#### 处理逻辑表

```
遍历顺序              排序键                   处理逻辑
───────────────────────────────────────────────────────────
rank 5 naobj DEntry    DeletedAt               entryTS < from → continue（本 rank 内后续全跳过）
rank 4 naobj CEntry(w/ DC)                    全部 continue
rank 3 naobj CEntry(w/o DC)  CreatedAt         entryTS > to → continue
rank 2 aobj  DEntry    DeletedAt               entryTS < from → continue（本 rank 内后续全跳过）
rank 1 aobj  CEntry(w/ DC)                    全部 continue
rank 0 aobj  CEntry(w/o DC)  GetMinCommitTS/CreatedAt  entryTS > to → continue

未跳过的 entry → VisibleByTS(to) → 去重
```

**核心规则**：
- **DEntry（rank 2/5）**：DeletedAt 降序，遇到 `DeletedAt < from` 即可 `continue` 跳过本 rank 剩余所有 entry；其余的按 `VisibleByTS(to)` 判断
- **CEntry 有 DCounterpart（rank 1/4）**：整个 rank 全部跳过。用内层 while 循环快进，**不逐 entry `continue`**
- **CEntry 无 DCounterpart（rank 0/3）**：entryTS 降序，遇到 `entryTS > to` 的跳过（太新），一旦 `entryTS <= to` 后续都满足条件

```go
for ok := objIt.Last(); ok; {
    obj := objIt.Item()
    rank, entryTS := obj.objectListRankAndTS()

    // DEntry (rank 5, 2): DeletedAt < from → 本 rank 后续全跳过
    if rank == 5 || rank == 2 {
        if entryTS.LT(&from) {
            ok = objIt.Prev()
            continue
        }
    }

    // CEntry with DCounterpart (rank 4, 1): 跳过整个 rank，不逐 entry continue
    if rank == 4 || rank == 1 {
        for ok = objIt.Prev(); ok; ok = objIt.Prev() {
            if r, _ := objIt.Item().objectListRankAndTS(); r != rank {
                break  // 离开本 rank，进入下一 rank
            }
        }
        continue  // 重新处理下一 rank 的 entry
    }

    // CEntry without DCounterpart (rank 3, 0): entryTS > to → 跳过
    if entryTS.GT(&to) {
        ok = objIt.Prev()
        continue
    }

    if !obj.VisibleByTS(to) {
        ok = objIt.Prev()
        continue
    }

    // ... 去重 ...
    ok = objIt.Prev()
}
```

**说明**：外层 `for` 不用自动 `Prev()`（`for ok := ...; ok;` 没有 post statement），改为每处手动 `ok = objIt.Prev()`。这样 rank 4/1 的内层 while 循环结束后 `continue` 能正确回到外层循环顶部处理下一 rank 的第一个 entry。

### DEntry 和 rank 4/1 的 skip 比较

| rank | 策略 | 原因 |
|------|------|------|
| DEntry (5, 2) | 逐 entry `continue` | DeletedAt 降序，遇到 `< from` 后本 rank 剩余全跳过；DEntry 数量通常不多，逐 entry 开销可接受 |
| CEntry w/ DC (4, 1) | while 循环快进 | 整个 rank 无条件跳过，无需逐 entry 判断；flush 频繁时可能有较多 entry |

两种策略都是「rank 内跳过」，不会 `return` 跳出整个函数——跳过 DEntry rank 后会进入 CEntry rank，跳过 rank 4 后进入 rank 3。

---

## 4. 改动二：CollectAppendLocked 返回 bitmap

### 涉及文件

`pkg/vm/engine/tae/tables/updates/mvcc.go`

### 现状

```go
func (n *AppendMVCCHandle) CollectAppendLocked(
    start, end types.TS, mp *mpool.MPool,
) (
    minRow, maxRow uint32,            // ← 连续行范围
    commitTSVec, abortVec containers.Vector,
    aborts *nulls.Bitmap,
) {
    // 按 PrepareTS 范围定位 AppendNode
    startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
    // ...
    endOffset, node := n.appends.GetNodeToReadByPrepareTS(end)
    // ...
    minRow = n.appends.GetNodeByOffset(startOffset).startRow
    maxRow = node.maxRow

    // 遍历区间内所有 AppendNode（含 abort 的）
    n.appends.LoopOffsetRange(startOffset, endOffset,
        func(node *AppendNode) bool {
            if node.IsAborted() {
                aborts.AddRange(uint64(node.startRow), uint64(node.maxRow))
            }
            for i := 0; i < int(node.maxRow-node.startRow); i++ {
                commitTSVec.Append(node.GetCommitTS(), false)
                abortVec.Append(node.IsAborted(), false)
            }
            return true
        })
    return
}
```

### 问题

返回 `[minRow, maxRow)` 是连续范围，包含了 abort 的 AppendNode 的行数据。调用方无法跳过这些行，只能拿到整个窗口后自行通过 `aborts` bitmap 过滤（而当前调用方 `getDataWindowOnWriteSchema` 直接丢弃了 abort bitmap）。

### 改动方案

改为返回 `rowMask *nulls.Bitmap`（替代 `minRow, maxRow`），保留 `abortVec` 给调用方使用。`rowMask` 包含所有行（含 abort），`abortVec` 标记哪些行是 abort 的：

```go
func (n *AppendMVCCHandle) CollectAppendLocked(
    start, end types.TS, mp *mpool.MPool,
) (
    rowMask *nulls.Bitmap,              // 标记所有需要包含的行（含 abort）
    commitTSVec containers.Vector,      // 每行的 commitTS（与 rowMask 一一对应）
    abortVec containers.Vector,         // 每行是否 abort（与 rowMask 一一对应）
) {
    startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
    if node != nil && node.GetPrepare().LT(&start) { startOffset++ }
    endOffset, node := n.appends.GetNodeToReadByPrepareTS(end)
    if node == nil || startOffset > endOffset { return nil, nil, nil }

    // 遍历区间内所有节点找到真正的 maxRow
    // （ReorderAppendsByPrepareTSLocked 按 PrepareTS 排序后，endOffset 节点的 maxRow 不一定是最大值）
    maxRow := uint32(0)
    n.appends.LoopOffsetRange(startOffset, endOffset,
        func(node *AppendNode) bool {
            if node.maxRow > maxRow {
                maxRow = node.maxRow
            }
            return true
        })

    rowMask = nulls.NewWithSize(int(maxRow))
    commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
    abortVec = containers.MakeVector(types.T_bool.ToType(), mp)

    n.appends.LoopOffsetRange(startOffset, endOffset,
        func(node *AppendNode) bool {
            txn := node.GetTxn()
            if txn != nil {
                n.RUnlock()
                txn.GetTxnState(true)
                n.RLock()
            }
            // rowMask 包含所有行（含 abort），abortVec 标记 abort
            rowMask.AddRange(uint64(node.startRow), uint64(node.maxRow))
            for i := 0; i < int(node.maxRow-node.startRow); i++ {
                commitTSVec.Append(node.GetCommitTS(), false)
                abortVec.Append(node.IsAborted(), false)
            }
            return true
        })
    return rowMask, commitTSVec, abortVec
}
```

**关键变化**：
- `rowMask` 替代 `minRow, maxRow`：不再是连续范围，而是精确的 bitmap，**包含 abort 行**
- 保留 `abortVec`：调用方自行决定是否过滤 abort
- 去掉 `aborts` 返回值：`abortVec` 已经提供了逐行标记
- `commitTSVec` 长度 = `rowMask` 中 set bit 的数量（一一对应）

### 对调用方的影响

有两个调用方需要适配：

1. **`mnode.go:getDataWindowOnWriteSchema`**（单个 aobject flush）→ 见 [改动三](#5-改动三flush-写路径使用-bitmap-过滤行)
   - 使用 `rowMask` 拷贝数据，保留 `abortVec` 写入 abort 列
2. **`mnode.go:CollectObjectTombstoneInRange`**（tombstone 收集）
   - 遍历 `rowMask` 替代 `[minRow, maxRow)` 范围，通过 `abortVec` 跳过 abort 行

`CollectObjectTombstoneInRange` 的适配：

```go
// 旧代码 — 用 minRow/maxRow 遍历
for i := minRow; i < maxRow; i++ {
    if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
        // ... 收集 tombstone ...
    }
}

// 新代码 — 遍历 rowMask bitmap，跳过 abort 行
abortIdx := 0
it := rowMask.Iter()
for it.Next() {
    i := uint32(it.Row())
    if abortVec.Get(abortIdx).(bool) { abortIdx++; continue }
    abortIdx++
    if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
        // ... 收集 tombstone ...
    }
}
```

---

## 5. 改动三：flush 写路径 — 单个 aobject 保留 abort 列

### 涉及文件

`pkg/vm/engine/tae/tables/mnode.go`、`pkg/vm/engine/tae/containers/types.go`、`pkg/objectio/const.go`

### 现状

`mnode.go:149-184` 的 `getDataWindowOnWriteSchema`：

```go
from, to, commitTSVec, abort, _ :=
    node.object.appendMVCC.CollectAppendLocked(start, end, mp)
if abort != nil {
    abort.Close()          // ← abort bitmap 被丢弃！
}
if commitTSVec == nil {
    return nil
}
// 拷贝 [from, to) 范围的所有行（包括 abort 行）
inner := node.data.CloneWindowWithPool(int(from), int(to-from), ...)
inner.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
```

### 问题

- `[from, to)` 是连续范围，包含了 abort 行
- `abort.Close()` 丢弃了 abort 标记，abort 行被当做正常数据写入磁盘，去重读盘时误判
- 没有 abort 列，无法区分正常行和 abort 行

### 改动方案

单个 aobject flush 保留 abort 行，用 abort 列标记。**merge 路径不会保留 abort 行**（见 [改动六](#8-改动六flushmerge-按-committs-排序)），所以 abort 列只需在单个 flush 时写入。

```go
func (node *memoryNode) getDataWindowOnWriteSchema(...) {
    // ...
    rowMask, commitTSVec, abortVec :=
        node.object.appendMVCC.CollectAppendLocked(start, end, mp)
    if commitTSVec == nil {
        return nil
    }
    // 用 rowMask 拷贝数据（包含 abort 行）
    inner := node.data.CloneWindowWithBitmap(rowMask)
    inner.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
    inner.AddVector(objectio.TombstoneAttr_Abort_Attr, abortVec)     // 新增 abort 列
    // ...
    batWithVer.Seqnums = append(batWithVer.Seqnums,
        objectio.SEQNUM_COMMITTS,
        objectio.SEQNUM_ABORT,                                       // 新增 seqnum
    )
}
```

需要在 `objectio/const.go` 中定义新常量：

```go
SEQNUM_ABORT              = math.MaxUint16 - 3
TombstoneAttr_Abort_Attr  = "abort"
```

需要新增 `containers.Batch.CloneWindowWithBitmap` 方法：

```go
// 在 containers/types.go 中
func (b *Batch) CloneWindowWithBitmap(mask *nulls.Bitmap) *Batch {
    // 遍历 mask 中的 set bits，从 b.Vecs[*] 中拷贝对应行到新 Batch
    // 新 Batch 的行数 = mask.GetCardinality()
}
```

### flush 的两个产物

一次 flush 产生**两个 object**：

| 产物 | 路径 | 内容 |
|------|------|------|
| **单个 aobject** | `flushAObjsForSnapshot` → `flushObjTask` | 保留 abort 行 + `SEQNUM_COMMITTS` + `SEQNUM_ABORT` 列 |
| **merge object** | `mergeAObjs` | 不保留 abort 行，只有 `SEQNUM_COMMITTS` 列 |

---

## 6. 改动四：内存 aobject 去重使用 bitmap

### 涉及文件

`pkg/vm/engine/tae/tables/aobj.go`、`pkg/vm/engine/tae/tables/mnode.go`、`pkg/vm/engine/tae/tables/updates/mvcc.go`

### 6.1 GetDuplicatedRows 内存路径

**现状** — `aobj.go:183-239`，使用 `getRowOffset` 函数返回 `[minRow, maxRow)` 连续范围：

```go
fn := func() (minv, maxv int32, err error) {
    if to == txn.startTS {
        maxv = obj.appendMVCC.GetMaxVisibleRowLocked(txn)
    } else {
        maxv = obj.GetMaxRowByTS(to)
    }
    minv = obj.GetMaxRowByTS(from)
    return
}
return node.GetDuplicatedRows(ctx, txn, fn, keys, keysZM, rowIDs, mp)
```

**问题**：
- `[minRow, maxRow)` 包含 abort 行，只能通过 `checkConflictLocked` 逐行检查 WW 冲突，效率低
- 没有利用 commitTS 做范围过滤

**改动方案** — 构造 `neededRows` bitmap，标记真正需要去重检查的行：

```go
func (obj *aobject) GetDuplicatedRows(...) {
    node := obj.PinNode()
    defer node.Unref()
    if !node.IsPersisted() {
        // 构造 neededRows bitmap（排除 abort + commitTS 超范围的行）
        obj.RLock()
        neededRows, commitTSVec, err := obj.appendMVCC.BuildDedupRowBitmapLocked(from, to, txn.GetStartTS())
        obj.RUnlock()
        if err != nil { return err }

        return node.GetDuplicatedRowsWithBitmap(
            ctx, txn, keys, keysZM, rowIDs, neededRows, commitTSVec, mp,
        )
    } else {
        return obj.persistedGetDuplicatedRows(ctx, txn, from, to, keys, keysZM, rowIDs, true, mp)
    }
}
```

### 6.2 BuildDedupRowBitmapLocked

**新增** 在 `updates/mvcc.go`。

参照 `GetVisibleRowLocked` 的模式：先收集需要等待的 AppendNode，再统一等待，最后构建 bitmap。

```go
func (n *AppendMVCCHandle) BuildDedupRowBitmapLocked(
    from, to, startTS types.TS,
) (neededRows *nulls.Bitmap, commitTSVec containers.Vector, err error) {
    if n.appends == nil || n.appends.IsEmpty() {
        return &nulls.Bitmap{}, nil, nil
    }

    // ========================================
    // 第一阶段：收集需要等待的 AppendNode
    // 参照 GetVisibleRowLocked 的 NeedWaitCommitting 模式
    // ========================================
    anToWait := make([]*AppendNode, 0)
    txnToWait := make([]txnif.TxnReader, 0)
    n.appends.ForEach(func(an *AppendNode) bool {
        needWait, waitTxn := an.NeedWaitCommitting(startTS)
        if needWait {
            anToWait = append(anToWait, an)
            txnToWait = append(txnToWait, waitTxn)
        }
        return true  // 不基于 startTS 提前停止，需要遍历全部
    }, true)

    // 等待这些 txn 完成提交/abort
    if len(anToWait) != 0 {
        n.RUnlock()
        for _, txn := range txnToWait {
            txn.GetTxnState(true)
        }
        n.RLock()
    }

    // ========================================
    // 第二阶段：遍历所有 AppendNode，构建 neededRows bitmap
    // 此时所有 AppendNode 的 IsCommitted/IsAborted 已是最终状态
    // ========================================

    // 找到真正的 maxRow（ReorderAppendsByPrepareTSLocked 排序后，getUpdateNode 的
    // maxRow 不一定是最大值，因为不同 txn 的 row 分配顺序与 PrepareTS 顺序无关）
    maxRow := uint32(0)
    n.appends.ForEach(func(an *AppendNode) bool {
        if an.maxRow > maxRow {
            maxRow = an.maxRow
        }
        return true
    }, true)

    neededRows = nulls.NewWithSize(int(maxRow))
    commitTSVec = containers.MakeVector(types.T_TS.ToType(), common.WorkspaceAllocator)

    n.appends.ForEach(func(an *AppendNode) bool {
        if an.IsAborted() { return true }          // abort 行不加入
        if !an.IsCommitted() { return true }       // 未提交不加入

        commitTS := an.GetCommitTS()
        if !from.IsEmpty() && commitTS.LE(&from) { return true }  // 太旧
        if commitTS.GT(&to)   { return true }                       // 太新
        if commitTS.GT(&startTS) {
            err = txnif.ErrTxnWWConflict
            return false
        }

        neededRows.AddRange(uint64(an.startRow), uint64(an.maxRow))
        for i := an.startRow; i < an.maxRow; i++ {
            for commitTSVec.Length() <= int(i) {
                commitTSVec.Append(types.TS{}, true)
            }
            commitTSVec.Update(int(i), commitTS, false)
        }
        return true
    }, true)
    return
}
```

### 6.3 GetDuplicatedRowsWithBitmap

**新增** 在 `mnode.go`（替代原有的 `GetDuplicatedRows` 实现）：

```go
func (node *memoryNode) GetDuplicatedRowsWithBitmap(
    ctx context.Context, txn txnif.TxnReader,
    keys containers.Vector, keysZM index.ZM,
    rowIDs containers.Vector,
    neededRows *nulls.Bitmap, commitTSVec containers.Vector,
    mp *mpool.MPool,
) (err error) {
    node.object.RLock()
    defer node.object.RUnlock()

    blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
    return node.pkIndex.GetDuplicatedRowsWithBitmap(
        ctx,
        keys.GetDownstreamVector(),
        keysZM,
        &blkID,
        rowIDs.GetDownstreamVector(),
        neededRows,
        commitTSVec,
        node.checkConflictLocked(txn),   // WW 冲突检查（skipFn）
        mp,
    )
}
```

---

## 7. 改动五：磁盘 aobject 去重使用 bitmap

### 涉及文件

`pkg/vm/engine/tae/tables/base.go`

### 现状

`persistedGetDuplicatedRows`（`base.go:377-409`）：对每个 block 调用 `pkIndex.BatchDedup` 获取候选选择，然后逐个进入 `getDuplicateRowsWithLoad` 逐行检查。在 `getDuplicateRowsWithLoad` 内部，ABLK 路径加载 commitTS 列后线性扫描每一行，逐个过滤 commitTS 范围。

### 问题

- 每行都要加载 commitTS 并逐行检查，效率低
- 没有利用 `rowMask` 预过滤

### 改动方案

在进入逐行检查前，先加载 commitTS 构建 `neededRows` bitmap，与 pkIndex 的选择结果求交集，只检查真正需要的行：

```go
func (obj *baseObject) persistedGetDuplicatedRows(
    ctx context.Context, txn txnif.TxnReader,
    from, to types.TS,
    keys containers.Vector, keysZM index.ZM,
    rowIDs containers.Vector, isAblk bool, mp *mpool.MPool,
) (err error) {
    pkIndex, err := MakeImmuIndex(ctx, obj.meta.Load(), nil, obj.rt)
    if err != nil { return }

    var neededRows *nulls.Bitmap
    if isAblk {
        commitTSVec, err := obj.LoadPersistedCommitTS(0)
        if err != nil { return err }
        defer commitTSVec.Close()

        startTS := txn.GetStartTS()
        commits := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec.GetDownstreamVector())
        neededRows = nulls.NewWithSize(len(commits))
        for i, ts := range commits {
            if ts.IsEmpty() { continue }
            if !from.IsEmpty() && ts.LE(&from) { continue }
            if ts.GT(&to) { continue }
            if ts.GT(&startTS) { return txnif.ErrTxnWWConflict }
            neededRows.Add(uint64(i))
        }
    }

    for i := 0; i < obj.meta.Load().BlockCnt(); i++ {
        sels, err := pkIndex.BatchDedup(ctx, keys, keysZM, obj.rt, obj.meta.Load().IsTombstone, uint32(i))
        if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) { continue }

        if isAblk && neededRows != nil {
            sels = sels.Intersect(neededRows)   // ← 与 neededRows 求交集
            if sels.IsEmpty() { continue }
        }

        err = obj.getDuplicateRowsWithLoad(ctx, txn, keys, sels, rowIDs, uint16(i), isAblk, from, to, mp)
        if err != nil { return err }
    }
    return nil
}
```

注意：
- **单个 flush 的 aobject** 磁盘上保留了 abort 行（有 `SEQNUM_ABORT` 列），去重读盘时需要先加载 abort 列，排除 abort 行后再构建 `neededRows`
- **merge object** 不保留 abort 行，无需加载 abort 列

```go
// 对于单个 flush 的 aobject，需要先排除 abort 行
if isAblk && obj.meta.Load().IsAppendable() {
    abortVec, err := obj.LoadPersistedAbortVec(0)
    if err != nil { return err }
    defer abortVec.Close()
    aborts := vector.MustFixedColNoTypeCheck[bool](abortVec.GetDownstreamVector())
    for i := range commits {
        if aborts[i] { continue }    // ← 排除 abort 行
        // ... commitTS 范围过滤 ...
    }
}
```

**新增 `LoadPersistedAbortVec`**（在 `base.go`）：

```go
func (obj *baseObject) LoadPersistedAbortVec(blkOffset uint16) (containers.Vector, error) {
    return obj.LoadPersistedColumnBySeqnum(blkOffset, objectio.SEQNUM_ABORT)
}
```

---

## 8. 改动六：flush/merge 的行顺序和 abort 过滤

### 涉及文件

`pkg/vm/engine/tae/tables/jobs/flushTableTail.go`

### 现状

`getDataWindowOnWriteSchema` 只有一处调用方：`ScanInMemory` → `RangeScanInMemoryByObject` → `flushAObjsForSnapshot`。调用时 `start=TS{}`，`end=flushTxn.startTS`，即拷贝 aobject 中所有已 prepare 的行（整个 aobject 的数据），**不需要按 commitTS 做范围过滤**。

**单 aobject flush**：
- 拷贝 aobject 全部数据行，保留原始 row offset 顺序
- 加上 `SEQNUM_COMMITTS` 列（和改动后的 `SEQNUM_ABORT` 列）
- 不需要排序

**多 aobject merge**（`mergeAObjs`）：
- 通过 `mergesort.SortBlockColumns` 按 **PK（sort key）** 排序
- 通过 `GetVisibleRowLocked` → `holes` → `bat.Deletes` 排除 abort 行
- merge 产物是一个 PK 有序、不含 abort 行的 object

### 结论

不需要额外改动：

| 行为 | 现状 | 需要改吗 |
|------|------|----------|
| 单个 flush 行顺序 | 保留原始 row offset 顺序 | ❌ 不需要，commitTS/abort 只是元数据列 |
| merge PK 排序 | 按 sort key 排序 | ❌ 不需要 |
| merge 过滤 abort | `bat.Deletes` 排除 | ✅ 已实现，无需改动 |

---

## 9. 改动汇总

### 9.1 文件变更总览

| 文件 | 改动 |
|------|------|
| `txn/txnimpl/base_table.go` | `incrementalGetRowsByPK` 用 `objectListRankAndTS()` 的 rank+entryTS 替代 `CreatedAt` 过滤，DEntry rank 内 `continue` 跳过 |
| `tables/updates/mvcc.go` | `CollectAppendLocked` 返回 `(rowMask, commitTSVec, abortVec)` 替代 `(minRow, maxRow, commitTSVec, abortVec, aborts)`；新增 `BuildDedupRowBitmapLocked`（含 wait txn 逻辑） |
| `tables/mnode.go` | `getDataWindowOnWriteSchema` 用 `CloneWindowWithBitmap(rowMask)` 替代 `CloneWindowWithPool`，写入 `SEQNUM_ABORT` 列；适配 `CollectObjectTombstoneInRange`；新增 `GetDuplicatedRowsWithBitmap` |
| `tables/aobj.go` | `GetDuplicatedRows` 内存路径用 `BuildDedupRowBitmapLocked` + `GetDuplicatedRowsWithBitmap` |
| `tables/base.go` | `persistedGetDuplicatedRows` 加载 commitTS + abort 列构建 bitmap，与 pkIndex 结果求交集；新增 `LoadPersistedAbortVec` |
| `containers/types.go` | 新增 `Batch.CloneWindowWithBitmap(rowMask)` 方法 |
| `objectio/const.go` | 新增 `SEQNUM_ABORT`、`TombstoneAttr_Abort_Attr` 常量 |

### 9.2 abort 行处理链路

```
写入（单个 aobject flush）:
  内存 AppendNode (含 abort 标记)
    → CollectAppendLocked 返回 rowMask（含 abort）+ abortVec
    → getDataWindowOnWriteSchema 写入 SEQNUM_COMMITTS + SEQNUM_ABORT
    → flushObjTask 写入磁盘
    → 磁盘 aobject: [col1, col2, ..., pk, commitTS, abort]

写入（merge）:
  内存 AppendNode (含 abort 标记)
    → memoryNode.Scan → GetVisibleRowLocked 返回 holes（含 abort）
    → holes → bat.Deletes
    → merge 路径通过 bat.Deletes 过滤 abort 行
    → merge object: [col1, col2, ..., pk, commitTS]
    → 不含 abort 行 ✅

读取（内存去重）:
  AppendNode
    → BuildDedupRowBitmapLocked 返回 neededRows（排除 abort + 不在 commitTS 范围的行）
    → GetDuplicatedRowsWithBitmap 只检查 neededRows 中的行

读取（单个 flush aobject 去重）:
  磁盘 aobject [col1, ..., pk, commitTS, abort]
    → LoadPersistedCommitTS + LoadPersistedAbortVec
    → 排除 abort 行
    → 构建 neededRows bitmap（commitTS 在 [from, to] 内）
    → pkIndex.BatchDedup 结果 ∩ neededRows

读取（merge object 去重）:
  磁盘 merge object [col1, ..., pk, commitTS]（不含 abort）
    → LoadPersistedCommitTS
    → 构建 neededRows bitmap（commitTS 在 [from, to] 内）
    → pkIndex.BatchDedup 结果 ∩ neededRows
```

### 9.3 兼容性考量

- `CollectAppendLocked` 签名变更需要修改两个调用方
- bitmap 去重依赖 pkIndex API 扩展（`GetDuplicatedRowsWithBitmap`）
- `CloneWindowWithBitmap` 需要新增，性能可接受（O(n) 扫描 mask）
- 新增 `SEQNUM_ABORT` 列改变磁盘格式，旧的 aobject 数据没有这个列，读取时需要兼容（列不存在 = 全部非 abort）
