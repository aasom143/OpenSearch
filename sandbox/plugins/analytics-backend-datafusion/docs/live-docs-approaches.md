# Live Docs Filtering — Approach Comparison

## Problem
When deleted docs exist, we need to filter them at query time. The current approach (inject MatchAll into Substrait → force indexed path) adds ~450ms overhead for pure DF queries because the indexed path is structurally heavier than the ListingTable path.

## Requirements
- Works for all query types: Path A (pure DF), Path B (SingleCollector), Path C (BitmapTree)
- Zero overhead when no deletions exist
- Minimal overhead when deletions exist
- No correctness issues

## Approaches

### 1. Current: Inject delegated_predicate(INT_MAX) at coordinator → indexed path
**How:** Coordinator injects `AND(filter, delegated_predicate(INT_MAX))` into Substrait. Forces indexed path. Rust calls `collectDocs` per RG. Java returns liveDocs bitset.

**Pros:**
- Works for all paths (A, B, C)
- Clean integration with existing infrastructure
- Defuse skips FFM calls when no deletions

**Cons:**
- Forces pure DF queries onto indexed path (+450ms overhead even with defuse)
- Indexed path overhead: per-segment evaluator setup, per-RG prefetch machinery
- Can't avoid the path switch even when no deletions (Substrait already has the injection)

---

### 2. ListingTable + post-batch filter
**How:** Stay on ListingTable path. After DataFusion produces RecordBatches, apply a liveDocs mask to drop deleted rows. The filter runs as a custom `FilterExec` or in `DatafusionResultStream`.

**Pros:**
- No indexed path overhead — pure DF queries stay fast (250ms baseline)
- Works for Path A naturally
- Simple concept: read parquet normally, filter deleted rows after

**Cons:**
- Reads deleted rows from parquet (wasted I/O for high-deletion segments)
- For Path B/C: the indexed path already exists — adding post-filter there is redundant
- Needs row-position tracking to know which parquet row → which Lucene doc ID
- How to get liveDocs to the Rust side? New FFM call or pass bitset at session creation?

---

### 3. RowSelection-based pre-filter at parquet reader level
**How:** Before reading parquet, build a `RowSelection` from the liveDocs bitset. Pass it to parquet reader so it physically skips deleted rows during I/O.

**Pros:**
- Skips I/O for deleted rows (better than post-filter for high-deletion ratio)
- Works at parquet reader level — transparent to all paths
- No indexed path overhead for pure DF queries

**Cons:**
- Needs liveDocs bitset available before parquet read starts (per-segment)
- RowSelection is per-file, liveDocs is per-segment — need mapping
- Passing segment-wide bitset from Java to Rust: one FFM call per segment at session creation
- Integration with IndexedTableProvider (already uses RowSelection for Collector pruning)

---

### 4. Pass liveDocs bitset at session creation (one-time per segment)
**How:** At `createSessionContextForIndexedExecution`, pass the full liveDocs `long[]` per segment. Rust stores it. During scan (any path), Rust applies it locally — no per-RG FFM calls.

**Pros:**
- One FFM call per segment (not per RG) — amortized cost
- Works for all paths
- Rust has the bitset locally — can apply as RowSelection OR post-filter
- No indexed path forced for pure DF queries

**Cons:**
- Memory: full segment bitset stored in Rust (100M docs = ~12MB per shard)
- API change: new parameter on session creation
- Needs segment-to-file mapping on Rust side

---

### 5. Segment-level `createCollector` returns full bitset once (cache in Rust)
**How:** Keep the existing FFM infrastructure but call `collectDocs` once per segment (full range) at evaluator creation time. Cache the bitset in Rust. Apply per-RG by slicing the cached bitset.

**Pros:**
- One `collectDocs` call per segment instead of per-RG
- Works with existing FFM infrastructure (no new API)
- Bitset cached in Rust — per-RG application is just a slice/AND

**Cons:**
- Still forces indexed path for pure DF queries (same overhead as approach 1)
- Memory: stores full segment bitset in Rust
- Doesn't help with the 450ms indexed path overhead

---

### 6. Hybrid: conditional path selection at data node
**How:** Data node checks `hasDeletedDocs`. If false → plain ListingTable path (no injection, no overhead). If true → choose based on query type:
- Path A (pure DF): ListingTable + post-batch filter (approach 2)
- Path B/C (has Collector): existing indexed path with liveDocs in collectDocs

**Pros:**
- Zero overhead for no-deletions case
- Pure DF queries stay fast even with deletions (post-filter is lightweight)
- Indexed queries get correct filtering through existing scorer path
- Best of both worlds

**Cons:**
- Two different mechanisms (post-filter for Path A, scorer for Path B/C)
- Need to handle Path A at data node (can't use Substrait injection)
- Post-filter needs row-position → liveDocs mapping
- More complex implementation

---

### 7. New FFM callback: `getLiveDocs(segment)` → Rust caches per-segment
**How:** Add a new FFM callback `getLiveDocs(contextId, writerGeneration) → long[] | null`. Rust calls it once per segment at setup time. Stores the bitset. Applies it as a row mask during parquet read (any path).

**Pros:**
- One FFM call per segment
- No indexed path overhead for pure DF
- Clean separation: liveDocs is orthogonal to query filter
- Works for all paths uniformly

**Cons:**
- New FFM callback (6th slot)
- Memory for cached bitset
- Integration point: where in the parquet read pipeline to apply the mask?
- Changes to both Rust FFM bridge and Java handle

---

## Comparison Matrix

| Approach | Pure DF overhead | Works all paths | FFM calls | Memory | Complexity |
|---|---|---|---|---|---|
| 1. Current (inject) | +450ms (indexed path) | Yes | Per-RG | Low | Done |
| 2. Post-batch filter | ~0ms | Yes | 0 or 1/seg | Low | Medium |
| 3. RowSelection pre-filter | ~0ms (skips I/O) | Yes | 1/seg | Medium | High |
| 4. Pass at session creation | ~0ms | Yes | 1/session | Medium | Medium |
| 5. Cache full segment bitset | +450ms (indexed path) | Yes | 1/seg | Medium | Medium |
| 6. Hybrid (conditional) | ~0ms | Yes | Varies | Low | High |
| 7. New getLiveDocs callback | ~0ms | Yes | 1/seg | Medium | Medium |

## Recommendation

For correctness with minimal overhead across all query types, **Approach 4 or 7** (pass liveDocs to Rust once per segment) seems best. Rust can then apply it uniformly at the parquet read level without forcing path changes or per-RG FFM calls.

For simplest implementation that solves the immediate problem, **Approach 6** (hybrid) avoids the indexed path overhead for pure DF while keeping the existing working solution for Path B/C.
