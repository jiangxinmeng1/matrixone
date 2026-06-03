# Dedup & Less2 排序重构方案

## 1. Less2 排序

### 1.1 新旧对比

```
旧 Less2:                              新 Less2:
─────────────────────                  ─────────────────────
Tier 1: aobj (按 CreatedAt)           Tier 1: aobj (按 minCommitTS)
Tier 2: naobj (按 max(C,Del))         Tier 2: naobj create (按 CreatedAt)
Tier 2: naobj (按 max(C,Del))         Tier 3: naobj delete (按 DeletedAt)  [NEW]
Tier 3: uncommitted                   Tier 4: uncommitted
```

**关键变更**：
- aobj 排序键：`CreatedAt` → **`minCommitTS`**
- naobj 不再混排，拆成 create（`IsCEntry=true`）和 delete（`IsDEntry=true`）两个独立 Tier
- create 按 `CreatedAt` 排，delete 按 `DeletedAt` 排

### 1.2 新 `Less2` 伪代码

```go
func (entry *ObjectEntry) Less2(b *ObjectEntry) bool {
    aUncommitted := (entry.IsLocal || !entry.IsCommitted()) && !entry.IsInMemory()
    bUncommitted := (b.IsLocal || !b.IsCommitted()) && !b.IsInMemory()
    aAppendable := entry.IsAppendable()
    bAppendable := b.IsAppendable()

    // ─── Tier 4: Uncommitted ───
    if aUncommitted != bUncommitted {
        return !aUncommitted   // committed 排前面
    }

    // ─── Tier 1 vs Tier 2/3: Appendable vs Non-Appendable ───
    if aAppendable != bAppendable {
        return aAppendable     // appendable(Tier1) < non-appendable(Tier2+3)
    }

    // ─── Tier 1: Appendable (aobj) — 按 minCommitTS ───
    if aAppendable && bAppendable {
        aMin := entry.GetMinCommitTS()
        bMin := b.GetMinCommitTS()
        if !aMin.EQ(&bMin) {
            return aMin.LT(&bMin)
        }
        return bytes.Compare(entry.ObjectShortName(), b.ObjectShortName()) < 0
    }

    // ─── Tier 2 vs Tier 3: naobj Create vs Delete ───
    aIsCreate := entry.IsCEntry()   // prevVersion == nil
    bIsCreate := b.IsCEntry()
    if aIsCreate != bIsCreate {
        return aIsCreate            // create(Tier2) < delete(Tier3)
    }

    // ─── Tier 2: naobj Create — 按 CreatedAt ───
    if aIsCreate {
        if !entry.CreatedAt.EQ(&b.CreatedAt) {
            return entry.CreatedAt.LT(&b.CreatedAt)
        }
        return bytes.Compare(entry.ObjectShortName(), b.ObjectShortName()) < 0
    }

    // ─── Tier 3: naobj Delete — 按 DeletedAt ───
    // aIsCreate == false, bIsCreate == false → both are D entries
    if !entry.DeletedAt.EQ(&b.DeletedAt) {
        return entry.DeletedAt.LT(&b.DeletedAt)
    }
    return bytes.Compare(entry.ObjectShortName(), b.ObjectShortName()) < 0
}
```

### 1.3 btree 布局

```
btree 升序 (Less2):

┌────────────────┐ ┌───────────────────┐ ┌──────────────────┐ ┌─────────────┐
│ Tier 1: aobj   │ │ Tier 2: create    │ │ Tier 3: delete   │ │ Tier 4:     │
│ 按 minCommitTS │ │ 按 CreatedAt      │ │ 按 DeletedAt     │ │ uncommitted │
│                │ │                   │ │                  │ │             │
│ aobj(min=100)  │ │ create(C=100)     │ │ delete(D=150)    │ │ local       │
│ aobj(min=200)  │ │ create(C=200)     │ │ delete(D=250)    │ │             │
│ aobj(min=300)  │ │ create(C=300)     │ │ delete(D=350)    │ │             │
└────────────────┘ └───────────────────┘ └──────────────────┘ └─────────────┘
                                                               ↑ Last (最新)

                        倒序遍历 ◄───  (Last → Prev)
```

**倒序遍历顺序**：Tier 4 → Tier 3 → Tier 2 → Tier 1

### 1.4 Create/Delete entry 的判定

| Entry 类型 | `IsCEntry()` | `IsDEntry()` | Tier | 举例 |
|---|---|---|---|---|
| 活跃 persisted obj | true | false | 2 | 普通持久化对象，未被删除 |
| 被 flush 的 aobj 的 C entry | true | false | 2 | `HasDCounterpart=true`，dedup 跳过 |
| 被 drop 的对象的 C entry | true | false | 2 | `HasDCounterpart=true`，dedup 跳过 |
| flush 产生的 D entry | false | true | 3 | 标记旧 aobj 被替换 |
| drop 产生的 D entry | false | true | 3 | 标记对象被删除 |

---

## 2. Dedup 遍历

### 2.1 `incrementalGetRowsByPK` — 增量去重

```go
func incrementalGetRowsByPK(pks, from, to) (rowIDs, err) {
    rowIDs = newVector(RowidType, all nulls)
    var earlybreak bool

    for ok := it.Last(); ok; ok = it.Prev() {
        if earlybreak {
            break
        }
        obj := it.Item()

        switch {

        // ═══════════════════════════════════════════════
        // Tier 4: uncommitted
        // ═══════════════════════════════════════════════
        case (obj.IsLocal || !obj.IsCommitted()) && !obj.IsInMemory():
            if !obj.VisibleByTS(to) {
                continue
            }
            // uncommitted 不参与 dedup，continue
            continue

        // ═══════════════════════════════════════════════
        // Tier 3: naobj delete entries (sorted by DeletedAt)
        // ═══════════════════════════════════════════════
        case !obj.IsAppendable() && obj.IsDEntry():
            // Break: 当前 delete 的 DeletedAt < from
            // → 后面所有 delete 的 DeletedAt 更小 → 全在 from 之前 → break
            if obj.DeletedAt.LT(&from) {
                earlybreak = true
                break   // 退出 switch，下一轮迭代 break 整个循环
            }

            // Skip: CreatedAt > to → 数据在 dedup 窗口之后，跳过
            if obj.CreatedAt.GT(&to) {
                continue
            }

            // 可见性
            if !obj.VisibleByTS(to) {
                continue
            }

            // Cat-B 过滤
            if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil {
                continue
            }

            // === 检查这个 delete entry ===
            obj.GetObjectData().GetDuplicatedRows(ctx, txn, pks, nil,
                                                   from, to, rowIDs, mp)

        // ═══════════════════════════════════════════════
        // Tier 2: naobj create entries (sorted by CreatedAt)
        // ═══════════════════════════════════════════════
        case !obj.IsAppendable() && obj.IsCEntry():
            // Skip: CreatedAt > to → 还没提交到 to，跳过
            if obj.CreatedAt.GT(&to) {
                continue
            }
            // Skip: CreatedAt < from → flushed object 的 CreatedAt=maxCommitTS，
            // 说明全部数据都在窗口之前，跳过
            if obj.CreatedAt.LT(&from) {
                continue
            }

            // Skip: 已有 D counterpart → 已 flush/已 drop，数据在 D entry 里
            if obj.HasDCounterpart() {
                continue
            }

            // 可见性
            if !obj.VisibleByTS(to) {
                continue
            }

            // === 检查这个 create entry（活跃的持久化对象）===
            obj.GetObjectData().GetDuplicatedRows(ctx, txn, pks, nil,
                                                   from, to, rowIDs, mp)

        // ═══════════════════════════════════════════════
        // Tier 1: aobj (sorted by minCommitTS)
        // ═══════════════════════════════════════════════
        case obj.IsAppendable():
            // Skip: 不可见
            if !obj.VisibleByTS(to) {
                continue
            }

            // Cat-B 过滤（已 flush 的 aobj 的 C entry）
            if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil {
                continue
            }

            // Break: maxCommitTS < from → 所有数据都在 from 之前
            maxCommitTS := obj.GetObjectData().GetMaxCommitTS()
            if !maxCommitTS.IsEmpty() &&
               maxCommitTS.LT(&from) &&
               !obj.HasDropIntent() {
                earlybreak = true
                // 不 break switch，当前 aobj 仍然检查
            }

            // === 检查这个 aobj ===
            obj.GetObjectData().GetDuplicatedRows(ctx, txn, pks, nil,
                                                   from, to, rowIDs, mp)
        }
    }
    return
}
```

### 2.2 `findDeletes` — Tombstone 删除查重

```go
func findDeletes(rowIDs, from, to) error {
    // 本 txn 自己的工作区先查
    tbl.contains(ctx, rowIDs, keysZM, allocator)

    it := table.MakeTombstoneObjectIt()
    defer it.Release()
    var earlybreak bool

    for ok := it.Last(); ok; ok = it.Prev() {
        if earlybreak {
            break
        }
        obj := it.Item()

        // ═══════════════════════════════════════════════
        // Tier 4: uncommitted
        // ═══════════════════════════════════════════════
        if (obj.IsLocal || !obj.IsCommitted()) && !obj.IsInMemory() {
            if !obj.VisibleByTS(to) { continue }
            continue
        }

        // ═══════════════════════════════════════════════
        // Tier 3: delete entries (sorted by DeletedAt)
        // ═══════════════════════════════════════════════
        if !obj.IsAppendable() && obj.IsDEntry() {
            if obj.DeletedAt.LT(&from) {
                earlybreak = true
                break
            }
            if obj.CreatedAt.GT(&to) {
                continue
            }
            if !obj.VisibleByTS(to) { continue }
            if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil { continue }

            // ZM skip
            if obj.Rows() != 0 {
                if skip, _ := quickSkipThisObject(ctx, keysZM, obj); skip { continue }
            }
            obj.GetObjectData().Contains(ctx, txn, rowIDs, keysZM, allocator)
        }

        // ═══════════════════════════════════════════════
        // Tier 2: create entries (sorted by CreatedAt)
        // ═══════════════════════════════════════════════
        if !obj.IsAppendable() && obj.IsCEntry() {
            if obj.CreatedAt.GT(&to) { continue }
            if obj.CreatedAt.LT(&from) { continue }
            if obj.HasDCounterpart() { continue }
            if !obj.VisibleByTS(to) { continue }

            if obj.Rows() != 0 {
                if skip, _ := quickSkipThisObject(ctx, keysZM, obj); skip { continue }
            }
            obj.GetObjectData().Contains(ctx, txn, rowIDs, keysZM, allocator)
        }

        // ═══════════════════════════════════════════════
        // Tier 1: aobj (sorted by minCommitTS)
        // ═══════════════════════════════════════════════
        if obj.IsAppendable() {
            if !obj.VisibleByTS(to) { continue }
            if obj.GetPrevVersion() == nil && obj.GetNextVersion() != nil { continue }

            maxCommitTS := obj.GetObjectData().GetMaxCommitTS()
            if !maxCommitTS.IsEmpty() &&
               maxCommitTS.LT(&from) &&
               !obj.HasDropIntent() {
                earlybreak = true
            }

            if obj.Rows() != 0 {
                if skip, _ := quickSkipThisObject(ctx, keysZM, obj); skip { continue }
            }
            obj.GetObjectData().Contains(ctx, txn, rowIDs, keysZM, allocator)
        }
    }
    return nil
}
```

### 2.3 `foreachAobjBefore` — Flusher 遍历

Flusher 只关心 aobj（需要 flush 的 appendable 对象），变化最小：

```go
func foreachAobjBefore(table, ts, lastCkp, df, tf) {
    key := NewObjectEntryDEntrySeekKey(ts.Next())

    data := table.MakeDataObjectIt()
    defer data.Release()

    var ok bool
    if ok = data.Seek(key); !ok {
        ok = data.Last()
    }
    for ; ok; ok = data.Prev() {
        item := data.Item()

        // Tier 3: delete entries → 不处理（不是 aobj）
        // 继续遍历到 Tier 2: create entries → 不处理（不是 aobj）
        // 继续遍历到 Tier 1: aobj

        if item.IsAppendable() && item.IsCEntry() &&
           !item.HasDCounterpart() && item.CreatedAt.LE(&ts) {
            // 需要 flush 的 aobj
            df(item)
        }

        // Break: C entry CreatedAt < lastCkp（且不是需要 flush 的 aobj）
        // 说明已经遍历到了上次 checkpoint 之前的老数据
        if item.IsCEntry() && item.CreatedAt.LT(&lastCkp) {
            // 但 aobj 即使老也要 flush（如果没有 D counterpart）
            if item.IsAppendable() && !item.HasDCounterpart() &&
               item.CreatedAt.LE(&ts) {
                df(item)
                continue
            }
            break
        }
    }

    // tombstone 遍历相同逻辑，略
}
```

---

## 3. 各 Tier 的遍历行为总结

```
btree 升序: [aobj(minCTS)] [create(CreatedAt)] [delete(DeletedAt)] [uncommitted]

                        倒序遍历 ◄───

Tier 4: uncommitted
├─ !VisibleByTS(to) → continue
└─ 不参与 dedup，continue

Tier 3: delete entries (按 DeletedAt 升序)
├─ DeletedAt < from     → earlybreak  ← 全 break（后面 delete 更老）
├─ CreatedAt > to       → continue    ← 数据太新
├─ Cat-B / !Visible     → continue
└─ 其余 → Contains / GetDuplicatedRows

Tier 2: create entries (按 CreatedAt 升序)
├─ CreatedAt > to       → continue    ← 数据太新
├─ HasDCounterpart      → continue    ← 已 flush/drop，数据在 D entry 里
├─ !VisibleByTS(to)     → continue
└─ 其余 → Contains / GetDuplicatedRows
    (无 break，遍历完 Tier 2)

Tier 1: aobj (按 minCommitTS 升序)
├─ !VisibleByTS(to)     → continue
├─ Cat-B                → continue
├─ maxCommitTS < from   → earlybreak  ← break（后面 aobj 更老）
└─ 其余 → Contains / GetDuplicatedRows
```

---

## 4. 文件变更清单

| 文件 | 变更 |
|------|------|
| `catalog/object.go` | `Less2` 重写；区分 `IsDEntry`/`IsCEntry` 用于 Tier 判定 |
| `catalog/object_sort_test.go` | 更新测试：验证新排序顺序 |
| `txn/txnimpl/base_table.go` | `incrementalGetRowsByPK` 改为按 Tier 的分支逻辑 |
| `txn/txnimpl/table.go` | `findDeletes` 改为按 Tier 的分支逻辑 |
| `tables/table_scan.go` | `TombstoneRangeScanByObject` 相应调整 |
| `db/checkpoint/flusher.go` | `foreachAobjBefore` 调整 break 条件适配新 Tier 顺序 |
| `catalog/object.go` | flush 后对象 `CreatedAt` 使用 `maxCommitTS` |

## 5. 注意事项

### 5.1 minCommitTS 与 maxCommitTS 的单调性

aobj 按 `minCommitTS` 排序，但 early break 用 `maxCommitTS < from`。
这两个值不一定同序：较早创建的 aobj 可能有尾部延迟提交的 AppendNode，
导致 `maxCommitTS` 大于后续 aobj 的 `maxCommitTS`。

**影响**：当 `maxCommitTS < from` 触发 early break 时，可能漏掉前面
`minCommitTS` 较小但 `maxCommitTS >= from` 的 aobj。

**缓解**：sharedAppender 中，新 aobj 在所有旧 aobj 分配完毕后才创建，
且新 aobj 的 `CreatedAt`（= txn.StartTS）晚于旧 aobj 所有 PrepareTS。
因此 `maxCommitTS` 整体上仍是单调的。极端乱序场景需要额外处理。

### 5.2 `GetMinCommitTS` 对非 shared aobj 的处理

当前 `ObjectEntry.GetMinCommitTS()` 对非 shared aobj 直接返回 `CreatedAt`。
新 `Less2` 在 Tier 1 比较 `GetMinCommitTS()`，旧 aobj（非 shared）行为不变。

### 5.3 调试日志清理

现有的 8 处 `non-appendable CreatedAt > from` 调试日志应在完成重构后移除。
