# Less2 排序变更：遍历兼容性分析

## 1. 受影响遍历点分类

### 第一类：breaker — 依赖排序做 early break/seek

这些遍历的 break 逻辑直接依赖 Less2 排序，改动 Less2 必须同步改。

| # | 函数 | 当前排序依赖 | 新 Less2 需适配 |
|---|------|-------------|----------------|
| 1 | `incrementalGetRowsByPK` | Tier 顺序 + CreatedAt 做 break | ✅ 已在 dedup redesign 中覆盖 |
| 2 | `findDeletes` | 同上 | ✅ 已在 dedup redesign 中覆盖 |
| 3 | `foreachAobjBefore` | `Seek(DeletedAt=ts.Next())` | ✅ seek key 改为 D entry |
| 4 | `IsTableTailFlushed` | 同上 | ✅ 同上 |
| 5 | `WaitUntilCommitted` | uncommitted 在 btree 尾部 → break | ✅（新 Less2 Tier 4 仍在尾部） |
| 6 | `TombstoneRangeScanByObject` | aobj CreatedAt break | ✅ 复用 dedup 模式 |

### 第二类：iterator — 全量遍历，不 break

这些遍历不依赖排序做 break，只依赖排序决定**返回顺序**。

| # | 函数 | 排序依赖 | 影响 |
|---|------|---------|------|
| 7 | `VisibleCommittedObjectIt` | 返回第一个可见对象 | 低：全量遍历场景无影响 |
| 8 | `TryFindLastAppendableObject` | 找最后一个 appendable | 低：shared appender 下很少用 |
| 9 | `RecurLoop` | 全量遍历无 break | 无 |
| 10 | `GetSoftdeleteObjects` | 全量遍历无 break | 无 |
| 11 | `PPString` | debug 输出 | 无 |
| 12 | GC check | 全量收集 | 无 |

---

## 2. 第一类：需要适配的具体分析

### 2.1 `foreachAobjBefore` 和 `IsTableTailFlushed` — Seek Key 问题

**当前代码**：
```go
key := &ObjectEntry{EntryMVCCNode: EntryMVCCNode{DeletedAt: ts.Next()}}
it.Seek(key)
```

在旧 Less2 下：`max(CreatedAt=0, DeletedAt=ts.Next()) = ts.Next()` → 定位到 naobj 区域中时间 ≈ ts 的位置。

在新 Less2 下：这个 key 的 `prevVersion=nil` → `IsCEntry()=true` → 进入 **Tier 2**，排序键=CreatedAt=0。Seek 会定位到 Tier 2 的开头，完全不对。

**修复**：让 seek key 进入 Tier 3（D entry）。实现中通过
`catalog.NewObjectEntryDEntrySeekKey(ts.Next())` 封装这个细节：
```go
key := catalog.NewObjectEntryDEntrySeekKey(ts.Next())
```

`Less2` 中 `IsCEntry()` 判断的是 `prevVersion == nil`，不比较 prevVersion 的值，所以任意非 nil 指针都行。

### 2.2 `WaitUntilCommitted` — 已经正确

```go
for ok := it.Last(); ok; ok = it.Prev() {
    if obj.IsCommitted() { break }
    // wait for uncommitted...
}
```

新 Less2 下 uncommitted 仍在 Tier 4（最后）。倒序遍历时先遇到 uncommitted → 等完 → 遇到 committed → break ✓。不需要修改。

### 2.3 `TombstoneRangeScanByObject` — 适配新 Tier

当前用 `DeletedAt < start` 做 early break。新 Less2 下 tombstone 的 D entries 在 Tier 3，按 DeletedAt 排序，break 条件可以保序：倒序遍历从最高 DeletedAt 开始，一旦 `DeletedAt < start` 就可以 break。

---

## 3. 第二类：`VisibleCommittedObjectIt` 详解

**没有提前 break**。它一路 `Last() → Prev()` 遍历，逐个过滤，找到第一个匹配就返回。调用者继续调 `Next()` 则继续找下一个。

```go
func (it *VisibleCommittedObjectIt) Next() bool {
    for {
        ok = it.iter.Prev() // 一直往前走
        if !ok { return false }
        entry := it.iter.Item()

        // 过滤逻辑 — 没有 break，只有 continue
        if entry.IsDEntry() || entry.IsCreating() { continue }
        if !entry.HasDCounterpart() { return true }  // 找到！
        if !entry.GetNextVersion().IsVisible(it.txn) { return true }
        // 否则 continue 继续往前找
    }
}
```

**调用者分析**：

| 调用者 | 遍历方式 | 依赖顺序？ |
|--------|---------|-----------|
| `getRowsByPK` (DedupSnapByPK) | `for it.Next()` 全部遍历 | 否，全部检查 |
| `ObjectIt` | 逐次 `Next()` | 否，但首次返回的对象会变 |
| `RangeScanInMemoryByObject` | `for it.Next()` 全部遍历 | 否 |
| `PrefetchScan` | `for it.Next()` 全部遍历 | 否 |
| `GetLastAppendableObject` | `it.Next()` 取第一个 | **是** |

`GetLastAppendableObject` 取第一个非 D、活跃的条目。新 Less2 下，Tier 3(D)→Tier 2(C)→Tier 1(aobj)，取到的「第一个匹配」是 Tier 1 中 minCommitTS 最大的 aobj，恰好就是最新的 aobj。行为正确 ✓。

---

## 4. Flush 顺序：按 minCommitTS freeze + flush

### 4.1 当前不变量

Shared appender 模型下，每个 table 同时只存在**一个活跃 aobj**：

```
sharedAppender 状态:
  currentAobj → 唯一的活跃 aobj（接受 append）
  
  满了之后:
    currentAobj 被 freeze → 变成 frozen aobj
    创建新 NewInMemoryObject → 成为新的 currentAobj
    旧的 frozen aobj 等待 flush
```

ObjectList 中最多有：
- **1 个活跃 aobj**（currentAobj）
- **0~N 个 frozen aobj**（等待 flush）
- **M 个 persisted object**（已 flush 的）

### 4.2 minCommitTS 单调性

Aobj 按创建顺序排列，且创建时间单调：

```
Aobj 1: CreatedAt=T1, 分配 rows [0, maxRows)
  → 满了 → freeze → minCommitTS = 所有 AppendNode 中最小的 commitTS
Aobj 2: CreatedAt=T2, 分配 rows [0, maxRows)
  → T2 > 所有写入 Aobj 1 的 txn 的 PrepareTS
  → minCommitTS(Aobj2) > minCommitTS(Aobj1)  ← 单调！
```

**结论**：minCommitTS 跨 aobj 是单调递增的。

### 4.3 Flush 时 CreatedAt 的选择

目前 `GetUpdateEntry` 用 `minCommitTS` 作为 flushed object 的 CreatedAt：

```go
dropped.CreatedAt = minCommitTS
```

**建议改为 `maxCommitTS`**：

| 用 minCommitTS | 用 maxCommitTS |
|----------------|---------------|
| 排序位置 = 最早数据时间 | 排序位置 = 最晚数据时间 |
| `CreatedAt < from` 只保证最早的 < from | `CreatedAt < from` 保证**全部** < from |
| 后续数据可能 > from → dedup 漏检 | 全部数据 ≤ CreatedAt → 安全 skip ✓ |

**用 maxCommitTS 的好处**：dedup 中 non-aobj 的 `CreatedAt < from → continue` 语义正确——「这个对象所有数据都在 from 之前，可以跳过」。

### 4.4 Flush 保序方案

```
方案：
  1. Aobj 按创建顺序（= minCommitTS 顺序）冻结
     → 新 aobj 的 CreatedAt > 旧 aobj 的 maxCommitTS
     → 已有保证 ✓（sharedAppender 内部逻辑）

  2. Flusher 按 ObjectList 顺序 flush
     → Tier 1 中 aobj 按 minCommitTS 升序排列
     → foreachAobjBefore 倒序遍历 → 先 flush minCommitTS 最大的 aobj
     → 也可以正序遍历 → 先 flush minCommitTS 最小的 aobj（都不影响正确性）

  3. Flush 时设置 CreatedAt = maxCommitTS
     → persisted object 的 CreatedAt 单调
     → Tier 2 (create entries) 中完全按时间排序
     → dedup 的 CreatedAt < from → continue 语义正确
     → 不再需要 CreatedAt > from 的 skip 补丁！
```

### 4.5 影响

| 变更点 | 说明 |
|--------|------|
| `GetUpdateEntry` | `dropped.CreatedAt = maxCommitTS` 代替 `minCommitTS` |
| `incrementalGetRowsByPK` Tier 2 | 去掉 `CreatedAt > from → continue` 补丁，只保留 `CreatedAt < from → continue` 和 `> to → continue` |
| `Less2` Tier 1 | aobj 仍按 minCommitTS 排序（保证创建顺序）；或改为 maxCommitTS（和 flushed 对象一致） |

---

## 5. 变更汇总

### 需要修改的遍历点

| 函数 | 修改内容 |
|------|---------|
| `foreachAobjBefore` | seek key 通过 `NewObjectEntryDEntrySeekKey` 改为 D entry |
| `IsTableTailFlushed` | 同上 |
| `TombstoneRangeScanByObject` | 适配 Tier 3 delete / Tier 2 create / Tier 1 aobj 的遍历逻辑 |
| `incrementalGetRowsByPK` | 按 dedup-less2-redesign-plan.md 重构 |
| `findDeletes` | 同上 |
| `GetUpdateEntry` | `CreatedAt = maxCommitTS`（而非 minCommitTS） |

### 不需要修改的遍历点

| 函数 | 原因 |
|------|------|
| `WaitUntilCommitted` | Tier 4 仍在尾部 |
| `VisibleCommittedObjectIt` | 无 break，全量遍历 |
| `TryFindLastAppendableObject` | 新 Less2 下行为不变 |
| `RecurLoop` | 全量遍历 |
| `GetSoftdeleteObjects` | 全量遍历 |
| `PPString` / GC check | debug/工具，顺序不影响正确性 |
