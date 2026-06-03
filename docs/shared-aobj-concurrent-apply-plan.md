# Plan A: 预扩展 + 按偏移写入 — 解决 Shared Aobj 并发 ApplyAppend 物理-逻辑错位问题

## 1. 背景

### 1.1 什么是 Shared Aobj

旧模式下，每个 txn commit 时创建**自己独占的** aobj（appendable object），在里面写数据
→ 提交。多个 txn 的数据天然物理隔离，不存在并发写入同一个 aobj 的问题。

新模式下（这个分支做的重构），table 级别有一个 `sharedAppender` 单例，维护一个
in-memory aobj（称为 shared aobj）。**多个 txn 共享同一个 aobj**，通过在 aobj 的
`appendMVCC` 链表上创建 `AppendNode`（MVCC 节点）来隔离不同 txn 的可见性。

这样做的动机：
- 减少 aobj 数量，降低 flush/merge 负担
- 提高空间利用率（多个小 txn 填充同一个 BlockMaxRows）

### 1.2 当前的并发模型

```
                    shared.mu (table 级锁)
                 ┌──────────────────────┐
  Txn A          │  allocateSpace()     │
  PrepareApply ──┤   分配 rows [0, 100) │
                 │   shared.nextRow=100 │
  Txn B          │                      │
  PrepareApply ──┤   分配 rows [100,200)│
                 │   shared.nextRow=200 │
                 └──────────────────────┘

                      ⚡ 并发 ⚡
                 ┌──────────────────────┐
  Txn A          │  ApplyAppend()       │
                 │   writeDataToAobj()  │   谁先到谁先写
  Txn B          │                      │
                 │  ApplyAppend()       │   aobj.Lock() 保护但不保序
                 │   writeDataToAobj()  │
                 └──────────────────────┘
```

`shared.mu` 只保护了 **Prepare 阶段的空间分配**（保证不同 txn 分配不重叠的行范围），
但 **Apply 阶段**没有顺序约束。`aobj.Lock()` 可以防止并发写冲突，但不能保证写入顺序等于分配顺序。

## 2. 问题

### 2.1 物理位置 ≠ 逻辑位置

当前 `ApplyAppendLocked` 的实现：

```go
// mnode.go:243
func (node *memoryNode) ApplyAppendLocked(bat *containers.Batch) (from int, err error) {
    from = int(node.mustData().Length())          // ← 当前物理长度
    for _, attr := range bat.Attrs {
        destVec := node.data.Vecs[def.Idx]
        destVec.Extend(bat.Vecs[srcPos])          // ← 追加到末尾
    }
    return
}
```

而 `writeDataToAobj` 使用这个返回值：

```go
// shared_appender.go:242
func (txnApp *txnAppender) writeDataToAobj(data *containers.Batch, ctx *appendContext) error {
    // ...
    from, err := mnode.ApplyAppendLocked(bat)
    // PK index uses physical position ← 问题！
    mnode.pkIndex.BatchUpsert(..., from)
}
```

`allocateSpace` 分配的逻辑位置 `startRow` 记录在了 `ctx.destRow` 里，但 `writeDataToAobj`
没有使用它。

### 2.2 错位场景

```
allocateSpace 分配序列:
  Txn A: startRow=0,   allocated=100
  Txn B: startRow=100, allocated=100

ApplyAppend 顺序（B 先到达）:
  Txn B: node.data.Length()=0   → from=0   → Extend → 物理[0,   99] = B 的数据(逻辑行[100, 200))
  Txn A: node.data.Length()=100 → from=100 → Extend → 物理[100, 199] = A 的数据(逻辑行[0,   100))

结果: 物理布局 [B的数据][A的数据] ≠ 逻辑分配 [A][B]
```

### 2.3 影响范围

**① PK Index** — `BatchUpsert` 用 `from`（物理偏移）做 PK entries 的 row offset。
如果 `from` 不等于 `destRow`，PK 查找返回的 row 位置对不上 `AppendNode` 的逻辑行范围。
`GetAppendNodeByRowLocked(row)` 会查到错误的 AppendNode。

**② Scan** — `memoryNode.Scan` 通过 `getDataWindowLocked(0, maxRow)` 按物理偏移读数据。
如果物理布局和逻辑顺序不一致，scan 返回的数据顺序就错了。

**③ Holes bitmap** — `GetVisibleRowLocked` 返回的 holes bitmap 按逻辑行号设置。
如果物理位置不等于逻辑位置，holes bitmap 过滤的就是错误的数据。

**④ Dedup** — 依赖 PK Index + AppendNode 查找来判断冲突，如果 PK index 的 offset 不对，
w-w conflict 检查就会出错。

## 3. 方案

### 3.1 核心思路

**Prepare 阶段**：分配空间的同时，预扩展 `memoryNode.data` 到 `startRow + allocated`，
用 NULL 值填充。「占好坑位」。

**Apply 阶段**：按 `ctx.destRow` 偏移写入数据到 `memoryNode.data`，覆盖预扩展时填入的
NULL 值。「往自己的坑里填数据」。

**PK Index**：始终用 `ctx.destRow`（逻辑位置）做 offset。

```
时间线:

allocateSpace (Prepare)              writeDataToAobj (Apply)
─────────────────────                ───────────────────────
shared.mu Lock                       aobj.Lock()
分配 rows [destRow, destRow+N)       mnode.OverwriteAtLocked(bat, destRow)
  ↓                                    ↓
mnode.PreExtendTo(destRow + N)      copy batch columns → mnode.data[dRow:dRow+N]
  填 NULL 到 destRow+N                pkIndex.BatchUpsert(..., destRow)
shared.mu Unlock                     aobj.Unlock()
```

**关键变化**：无论 Apply 顺序如何，每个 txn 的数据都写入到它分配时预定的位置。
物理位置 = 逻辑位置 = `destRow`。

### 3.2 为什么这样是安全的

- 预扩展发生在 `shared.mu` 保护下，保证不同 txn 的 `[destRow, destRow+N)` 区间不重叠
- 预扩展填入 NULL，Apply 阶段用真实数据覆盖 NULL
- 读者（Scan/Dedup）通过 `GetVisibleRowLocked` 判断可见性 — 未提交的 AppendNode 对应的
  行（即使是真实数据而非 NULL）对读者不可见
- 因此，即使 Apply 阶段读者看到了已写入的真实数据，只要 AppendNode 未提交，就不可见；
  一旦 AppendNode 提交，数据一定已经写入完毕（Apply 在 ApplyCommit 之前）

### 3.3 新旧流程对比

```
旧流程:
  prepareApplyANode
    └─ for loop:
         allocateSpace  → startRow, allocated
         generatePhyAddr(startRow)
         record ctx {..., destRow: startRow}
       (node.data 不修改)
       
  ApplyAppend
    └─ writeDataToAobj
         from = node.Length()  ← 物理位置
         Extend(bat)           ← 追加到末尾
         pkIndex.Upsert(from)  ← 物理位置做 PK offset

新流程:
  prepareApplyANode
    └─ for loop:
         allocateSpace → startRow, allocated
         PreExtendData(node, startRow, allocated)  ← NEW: 占坑
         generatePhyAddr(startRow)
         record ctx {..., destRow: startRow}
  
  ApplyAppend
    └─ writeDataToAobj
         OverwriteAtLocked(bat, ctx.destRow)  ← NEW: 写到预定位置
         pkIndex.Upsert(ctx.destRow)          ← 逻辑位置做 PK offset
```

## 4. 详细实现步骤

### 4.1 `shared_appender.go` — `allocateSpace` 预扩展

在分配空间成功后，立即预扩展 memoryNode.data：

```go
// allocateSpace 中，现有 aobj 分配路径（约 line 330-358）
startRow := shared.nextRow
shared.nextRow += allocated

// === NEW: 预扩展 node.data ===
{
    mnode := shared.currentAobj.PinNode().MustMNode()
    shared.currentAobj.Lock()
    mnode.EnsureLength(startRow + allocated)   // 预扩展到目标长度
    shared.currentAobj.Unlock()
}
// === END NEW ===

// Create AppendNode ...（已有代码）
```

以及新 aobj 创建路径（约 line 396-415）：

```go
// === NEW: 预扩展 node.data ===
{
    mnode := aobj.PinNode().MustMNode()
    aobj.Lock()
    mnode.EnsureLength(allocated)   // 新 aobj，从 0 扩展到 allocated
    aobj.Unlock()
}
// === END NEW ===
```

### 4.2 `mnode.go` — 新增 `EnsureLength`

```go
// EnsureLength ensures node.data has at least `targetLen` rows.
// Missing rows are filled with NULL values for all columns.
// Must be called with aobj Lock held.
func (node *memoryNode) EnsureLength(targetLen uint32) {
    data := node.mustData()
    current := uint32(data.Length())
    if targetLen <= current {
        return
    }
    gap := int(targetLen - current)
    schema := node.writeSchema
    for _, colDef := range schema.ColDefs {
        if colDef.IsPhyAddr() {
            continue
        }
        vec := data.Vecs[colDef.Idx]
        nullVec := containers.NewConstNullVector(*vec.GetType(), gap)
        vec.Extend(nullVec)
        nullVec.Close()
    }
}
```

**注意**：`EnsureLength` 需要持有 aobj Lock（因为涉及修改 node.data），调用方（`allocateSpace`）
在 `shared.mu` 保护下需要额外获取 aobj Lock。

### 4.3 `mnode.go` — 新增 `OverwriteAtLocked`（替代 `ApplyAppendLocked`）

```go
// OverwriteAtLocked writes batch data into node.data at the specified destRow offset,
// overwriting any existing data (typically NULLs from EnsureLength).
// Must be called with aobj Lock held.
func (node *memoryNode) OverwriteAtLocked(bat *containers.Batch, destRow uint32) error {
    schema := node.writeSchema
    data := node.mustData()
    
    for srcPos, attr := range bat.Attrs {
        def := schema.ColDefs[schema.GetColIdx(attr)]
        destVec := data.Vecs[def.Idx]
        srcVec := bat.Vecs[srcPos]
        
        n := srcVec.Length()
        for i := 0; i < n; i++ {
            v := srcVec.Get(i)
            isNull := srcVec.IsNull(i)
            destVec.Update(int(destRow) + i, v, isNull)
        }
    }
    
    // RelLogicalID COMPAT (保持和原 ApplyAppendLocked 兼容逻辑)
    if node.object.meta.Load().GetTable().ID == 2 && len(data.Vecs) > 10 {
        // ... 同原逻辑
    }
    
    return nil
}
```

**性能优化 TODO**：`Get` → `Update` 的 per-element 循环对大数据量较慢。
后续可以给 `containers.Vector` 加一个 `CopyFrom(src Vector, destOff int)` 方法，
利用底层的列式内存拷贝。第一版先用 per-element 保证正确性。

### 4.4 `shared_appender.go` — `writeDataToAobj` 改用 `OverwriteAtLocked`

```go
func (txnApp *txnAppender) writeDataToAobj(data *containers.Batch, ctx *appendContext) error {
    bat := data.Window(int(ctx.srcStart), int(ctx.srcCount))
    defer bat.Close()

    ctx.aobj.Lock()
    defer ctx.aobj.Unlock()

    n := ctx.aobj.PinNode()
    defer n.Unref()

    if !n.IsPersisted() {
        mnode := n.MustMNode()

        // === CHANGED: 使用 OverwriteAtLocked + destRow ===
        if err := mnode.OverwriteAtLocked(bat, ctx.destRow); err != nil {
            return err
        }

        // PK index: 使用 destRow（逻辑位置）做 offset
        schema := mnode.writeSchema
        for _, colDef := range schema.ColDefs {
            if colDef.IsPhyAddr() {
                continue
            }
            if colDef.IsRealPrimary() && !schema.IsSecondaryIndexTable() {
                if err := mnode.pkIndex.BatchUpsert(
                    bat.Vecs[colDef.Idx].GetDownstreamVector(),
                    int(ctx.destRow),           // ← 改为 destRow
                ); err != nil {
                    return err
                }
            }
        }
        return nil
    }

    return moerr.NewInternalErrorNoCtx("cannot append to persisted node")
}
```

### 4.5 `ApplyAppendLocked` 处理

旧的 `ApplyAppendLocked` 在 `objectAppender.ApplyAppend`（`appender.go`）中也被调用，
那是旧 append 路径（非 shared aobj，用于 stats 等场景）。那个路径不变，`ApplyAppendLocked`
保留。

### 4.6 `shared_appender.go` — `testAppend` option 调整（如果有）

检查测试 helper 中如果直接调用了旧 `ApplyAppendLocked`，需要更新。

## 5. 边缘情况

### 5.1 同一个 txn 跨越多个 aobj

单个 txn 的数据可能太大（> BlockMaxRows），在 `allocateSpace` 循环中跨越多个 aobj。
每个 aobj 独立处理自己的预扩展和写入，不受影响。

### 5.2 新 aobj 创建和预扩展的原子性

创建新 aobj 和预扩展发生在 `shared.mu` 保护下，整个过程对 `allocateSpace` 的调用者是原子的。
新 aobj 的 `node.data` 从空开始，`EnsureLength(allocated)` 填充 NULL 到 `allocated` 行。

### 5.3 Flush/Freeze 与预扩展的交互

- `PrepareCompact` / `FreezeAppend` 在 freeze 之后会阻止新的 allocation（`shared.mu` 保护）
- 已分配但未 Apply 的行（NULL 区域）的 AppendNode 已创建，flush 时需要正确处理：
  - 如果 AppendNode 已 commit，flush 需要把数据（包括 NULL）写下去
  - 如果 AppendNode 未 commit（aborted），对应的 NULL 区域需要被跳过
  - 考虑在 `collectAppendLocked` 中跳过 aborted AppendNode 对应的行

### 5.4 Abort/Rollback 场景

Txn 分配了 `[destRow, destRow+N)` 并预扩展了 NULL，但随后 abort：
- `AddAppendNodeLocked` 创建的 AppendNode 会被标记为 Aborted
- NULL 数据保留在 node.data 中，但因为 AppendNode 是 aborted 的，对读者不可见
- TODO: flush 时如何处理这些"空洞"？可能需要 compact 掉 aborted 行

### 5.5 与旧 Append 路径的共存

`tableSpace.ApplyAppend` 中通过 `space.txnAppender != nil` 判断走新路径还是旧路径。
旧路径（`objectAppender.ApplyAppend` → `ApplyAppendLocked`）不受影响。

## 6. 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `tables/mnode.go` | 新增 + 修改 | 新增 `EnsureLength`、`OverwriteAtLocked`；保留 `ApplyAppendLocked` |
| `tables/shared_appender.go` | 修改 | `allocateSpace` 预扩展；`writeDataToAobj` 改用 `OverwriteAtLocked` + `destRow` |
| `tables/shared_appender_test.go` | 新增测试 | 并发 Apply 乱序场景的测试 |
| `tables/aobj.go` | 可能修改 | `GetMinCommitTS`/`GetMaxCommitTS` 处理 NULL 空洞 |

## 7. 测试要点

1. **并发 Apply 乱序**：两个 txn 分配 [0, 100) 和 [100, 200)，但第二个先 Apply。Scan 验证数据顺序正确。
2. **跨 aobj 场景**：大 batch 跨越多个 aobj。
3. **Scan 可见性**：一个 txn Apply 后但未 Commit 时，另一个 txn 的 scan 看不到未提交数据。
4. **PK 去重**：两个 txn 插入相同 PK，验证 w-w conflict 检测。
5. **Abort 空洞**：中间 txn abort 后，scan 应该跳过空洞。
6. **Flush 含空洞的 aobj**：flush 后 replay，验证数据正确。
