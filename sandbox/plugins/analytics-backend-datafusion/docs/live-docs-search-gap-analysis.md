# Live Docs Search-Side Gap Analysis

**Source design doc:** [Mustang Updates and Deletes Support in Multi-Format Engine](https://quip-amazon.com/FSoPA2qaLKHb)

## Summary

This document maps the delete-support design to the current codebase, identifies what exists vs. what's coming from upstream PRs, and clarifies what the search side needs to do.

## Execution Paths

| Path | Name | Description |
|---|---|---|
| **A** | Pure DF | No Collector leaves — plain DataFusion parquet scan |
| **B** | SingleCollector | `AND(Collector, Predicate?, ...)` — single Lucene collector + optional DF predicates |
| **C** | BitmapTree | Multiple Collectors, OR, NOT, nested — full boolean tree evaluation |

All three paths need live docs filtering. Lucene's scorer respects `liveDocs` for Collector leaves, but Predicate leaves (DF-evaluated on parquet) bypass Lucene entirely. Any path with a Predicate-only branch can return deleted rows without the filter.

## Upstream PRs (Dependencies)

### PR #22406 — Integrated Updates and Deletes for DataFormatAwareEngine
**Author:** thorkous | **Status:** Awaiting review

Adds the full **ingestion-side** update/delete path:
- Hard deletes via Lucene `liveDocs` (no tombstones)
- Positional deletes: maps `_id → (generation, rowId)`, applies via `indexWriter.tryDeleteDocument(leaf, position)`
- Rows stay in parquet at original offset — 1:1 correspondence maintained
- `droppedGenerations` in `RefreshResult` — fully-deleted segments removed from catalog snapshot
- `Deleter` / `DeleterImpl` / `DeleteExecutionEngine` abstractions in `server/.../dataformat/`

**Impact on search:** After this PR, `LeafReader.getLiveDocs()` returns the correct deletion bitset for any segment with deletes.

### PR #21597 — Deletion Handling During Merge
**Author:** sachin-27 | **Status:** Awaiting review

Adds live-docs filtering during **parquet merges**:
- New `LiveDocs` abstraction: `Map<Long, long[]>` keyed by segment generation
- `LuceneDeleteExecutionEngine.getLiveDocsForSegments()` — opens NRT reader, extracts `FixedBitSet` per segment
- Rust-side merger (`cursor.rs`, `sorted.rs`, `unsorted.rs`) skips dead rows during k-way merge
- Two-phase merge protocol (`prepareMerge` / `executeMerge`) to freeze live-docs snapshot
- `CompositeMerger` passes live-docs bitmap from Lucene into parquet merge

**Impact on search:** After merge, dead rows are physically removed from parquet files — no filtering needed for merged segments.

### PR #21937 — Add Search Support for Handling Deleted Documents
**Author:** (search-side PR) | **Status:** Awaiting review

Adds live-docs filtering during **search** by **reusing the existing `collectDocs` delegation infrastructure** — no new FFM callback needed.

**Key approach:** Treats live-docs filtering as a **delegated predicate** (like `match(field, 'text')`) that gets ANDed conjunctively:

1. `AnalyticsSearchBackendPlugin.hasDeletedDocs(reader)` — iterates leaves, returns `true` if any `getLiveDocs() != null`
2. `ShardScanInstructionHandler` — when `hasDeletedDocs == true`, switches filter tree shape to `CONJUNCTIVE` with 1 delegated predicate
3. `LuceneFilterDelegationHandle.collectDocs()` — enhanced to check `liveDocs.get(docId)` inside the iteration loop; only sets bits for live docs
4. Forces query into **Path B** (SingleCollector) with the live-docs delegation as the correctness Collector

**How it works in `collectDocs`:**
```java
Bits liveDocs = handle.liveDocs;  // fetched from leaf.reader().getLiveDocs()
while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
    if (liveDocs == null || liveDocs.get(docId)) {
        bits.set(docId - minDoc);
    }
    docId = iterator.nextDoc();
}
```

**Key insight:** No new `getLiveDocs` FFM callback is required. The existing `collectDocs` path produces a bitset that is the intersection of (query matches ∩ live docs). When the "query" is `MatchAllQuery`, the bitset IS the live docs bitmap.

## What This Means for Each Path

| Path | Before PR #21937 | After PR #21937 |
|---|---|---|
| **A** (Pure DF, no filter) | Deleted rows returned | Forced into Path B with live-docs Collector → deleted rows filtered |
| **B** (SingleCollector) | Collector already respects liveDocs | No change needed — scorer already excludes deletes |
| **B** (performance-only) | No Collector, deletes leak | Live-docs delegation added as correctness Collector → fixed |
| **C** (BitmapTree) | Predicate-only OR branches leak deletes | Live-docs delegation ANDed at top level → fixed |

## Revised Understanding: What's Actually Missing

Given the upstream PRs, the original Quip design's `getLiveDocs` JNI callback approach is **NOT being implemented**. Instead, the solution reuses `collectDocs` with a MatchAll scorer + liveDocs check.

### What PR #21937 delivers (search side):
- Detection of deleted docs via `hasDeletedDocs(reader)`
- Automatic injection of a live-docs delegation as a conjunctive Collector
- `collectDocs` enhanced to respect `liveDocs` bitset
- Path A queries promoted to Path B when deletes exist

### What still needs verification/work after these PRs land:
1. **Path C correctness** — When live-docs delegation is injected AND the original query already has a complex tree (OR/NOT), does it become `AND(LiveDocsCollector, OriginalTree)`? Need to verify the tree shape composition.
2. **Performance impact** — Every RG now requires a `collectDocs` call even for queries that had no Collector before (Path A → B promotion). The `MatchAllQuery` scorer is cheap (iterates all docs) but the FFM round-trip per RG is not free.
3. **Parquet-standalone mode** — PR #21937 uses `LeafReader.getLiveDocs()` (Lucene). For parquet-standalone (no Lucene), a different source of live docs is needed (the `.liv` file / `DeleteFormatWriter`). This is not yet addressed.
4. **Segment drop propagation** — When `droppedGenerations` removes a segment from the catalog snapshot, the parquet file list for the DataFusion scan must also exclude it. Need to verify this propagation path.

## Design Decisions (Revised)

### No new FFM callback needed

PR #21937 proves that the existing `collectDocs` infrastructure is sufficient. The live-docs filter piggybacks as a delegated predicate with `MatchAllQuery` scorer + liveDocs check. This avoids:
- New callback registration in `ffm_callbacks.rs`
- New `FilterTreeCallbacks.java` method
- New Rust-side `LiveDocsSource` trait

### Unconditional application via tree shape promotion

When `hasDeletedDocs == true`:
- Path A (no filter) → promoted to Path B (SingleCollector with live-docs delegation)
- Path B/C (existing filter) → live-docs delegation ANDed conjunctively at the top

When `hasDeletedDocs == false`:
- No change — zero overhead, no delegation injected

### Composite vs. Parquet-Standalone

| Mode | Live Docs Source | Status |
|---|---|---|
| Lucene + Parquet (composite) | `LeafReader.getLiveDocs()` via `collectDocs` | **Covered by PR #21937** |
| Parquet standalone | `.liv` file / `DeleteFormatWriter` | **NOT YET ADDRESSED** — needs separate implementation |
| Lucene standalone | N/A | Regular Lucene behavior |

## Open Questions

- For Path C with `OR(Collector, Predicate)` + live-docs delegation: is the final shape `AND(LiveDocsCollector, OR(Collector, Predicate))`? This would be Path C (BitmapTree) not Path B — need to confirm the tree classifier handles this.
- What's the performance overhead of promoting Path A → Path B for large scans with few deletes? (1 FFM call per RG for MatchAll scorer)
- For parquet-standalone: will the same `collectDocs` pattern work with a non-Lucene backend providing live docs, or is a new callback needed?

## References

- [PR #22406](https://github.com/opensearch-project/OpenSearch/pull/22406) — Ingestion-side updates/deletes
- [PR #21597](https://github.com/opensearch-project/OpenSearch/pull/21597) — Merge-time dead row removal
- [PR #21937](https://github.com/opensearch-project/OpenSearch/pull/21937) — Search-side live docs filtering
- [Quip Design Doc](https://quip-amazon.com/FSoPA2qaLKHb) — Original design (describes `getLiveDocs` JNI callback approach — superseded by PR #21937's delegation approach)
