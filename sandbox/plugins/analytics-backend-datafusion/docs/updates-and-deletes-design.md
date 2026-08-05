# Mustang Updates and Deletes Support in Multi-Format Engine

**Source:** [Quip Doc](https://quip-amazon.com/FSoPA2qaLKHb)

## Overview

This document describes how updates and deletes are handled in the `DataFormatAwareEngine` across three data format configurations:
1. **Standalone Lucene** — existing Lucene behavior
2. **Standalone Parquet** — new Mustang-native handling via `.liv` bitsets
3. **Composite (Lucene + Parquet)** — Lucene handles version tracking/deletes; Parquet just appends

---

## Handling Updates — Ingestion Flow

### Phase 0: Version Resolution

`IndexingStrategyPlanner` / `DeletionStrategyPlanner` resolves the current version of a document as the first step. The resolved version drives the planner to decide whether to append a new document or update an existing one.

**Two-tier lookup:**

#### LiveVersionMap lookup (fast path)
The engine checks the in-memory `LiveVersionMap` for the term uid. If recently indexed/updated/deleted, version is cached and returned immediately.

#### Data source query (slow path)
If not in the version map, the engine acquires a `DataFormatAwareReader` via `acquireReader()` and uses format-specific readers to look up `_version`, `_seq_no`, and `_primary_term`. **Lucene is always prioritized over other formats.**

#### Scenario 1: Standalone Lucene

```
DataFormatAwareEngine.resolveDocVersion(op, loadSeqNo)
│
├─ 1. versionMap.getUnderLock(op.uid().bytes())          ← in-memory (fast)
│
└─ 2. acquireReader() → DataFormatAwareReader
       └─ reader.getReader(luceneFormat, DirectoryReader.class)
            └─ DirectoryReader
                 ├─ new IndexSearcher(directoryReader)
                 ├─ VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher, uid, loadSeqNo)
                 │    └─ PerThreadIDVersionAndSeqNoLookup:
                 │         ├─ TermsEnum.seekExact(id)          ← inverted index O(1)
                 │         ├─ postings → last live docId        ← liveDocs bitset
                 │         └─ NumericDocValues for _version, _seq_no, _primary_term
                 └─ return IndexVersionValue(version, seqNo, primaryTerm)
```

#### Scenario 2: Standalone Parquet (no Lucene)

Since Parquet has no inverted index, version resolution requires a filter/scan on the `_id` column. Two options considered:

**Option A: Via DataFusion plugin → Substrait (existing infra)**
```sql
SELECT _seq_no, _version, _primary_term FROM parquet WHERE _id = ?
```
- Pros: Uses existing DataFusion pipeline (predicate pushdown, row group pruning, bloom filters)
- Cons: Heavy for single-row point lookup — full query plan with Substrait serialization

**Option B: Direct Parquet file read via Arrow Java (bypasses DataFusion)**
- Pros: No DataFusion overhead, direct file I/O with metadata pruning
- Cons: Reimplements predicate pushdown manually, more code to maintain

**Full flow (Option A):**
```
docVersionResolver.resolveVersion(datafusionReader, "abc")
│
├─ 1. Build SQL: "SELECT _seq_no, _version, _primary_term, _file_name, _row_id
│                  FROM <index> WHERE _id = 'abc'"
├─ 2. NativeBridge.sqlToSubstrait(...) → Substrait protobuf bytes
├─ 3. NativeBridge.executeQueryAsync(...)
│     → Optimizer: predicate pushdown (_id='abc') + projection pushdown (5 cols)
│     → ParquetExec: column stats pruning → bloom filter → scan matching pages
├─ 4. Iterate DatafusionResultStream → extract fields
└─ 5. Return DocVersionInfo(version, seqNo, primaryTerm, parquetFile, rowId)
       or null if not found
```

#### Scenario 3: Composite (Parquet + Lucene)

Always uses Lucene format reader to resolve version (same as Scenario 1).

---

### Phase 1: Updates in DataFormatAwareEngine

#### Case A: Updates on the same generation writer

All updates land on a single writer; parent/other child writers don't contain other versions.

```
W1 - 1, 2, 3, 4
P
```

| Subcase | Behavior |
|---|---|
| Standalone Lucene | Lucene `IndexWriter` handles deduplication — only version 4 survives refresh |
| Standalone Parquet | Buffer delete targeting the old row + append new version (see flow below) |
| Composite | Updates/deletes go to Lucene Writer; Parquet just appends; liveBitset tracked at Lucene level |

**Parquet standalone update flow:**
```
DataFormatAwareEngine.index(Engine.Index)
│
├─ IndexingStrategyPlanner.planOperationAsPrimary(index)
│   ├─ canOptimizeAddDocument? YES → APPEND (skip version check)
│   └─ NO → resolveDocVersion(op) → version found → processNormally (delete + append)
│
├─ writer = writerPool.getAndLock()
│   ├─ IF UPDATE: writer.bufferDelete(parquetFile, rowId, generation)
│   └─ writer.addDoc(documentInput) → ParquetWriter → Arrow vectors → Rust writer
│
└─ Record in translog, track seq_no in localCheckpointTracker
```

#### Case B: Updates across different writers

For cross-writer version sync, each update operation performs a delete on the writer containing the previous version. The `LiveVersionMap` (or a new map) tracks which writer last held the document.

If version ≥ 1 and document is not in the version map, the last writer defaults to the parent writer.

```
W1 - 2, 3
P  - 1
```

- Version 2 arrives: version map miss → resolve from parent → delete term from parent (version 1) → ingest version 2 in W1
- Version 3 arrives: version map hit → last writer is W1 → same-writer update handled automatically

---

### Phase 2: How Updates Get Applied During Refresh

#### Standalone Lucene
No special handling — Lucene IndexWriter + cross-writer sync (Case B) handles everything.

#### Standalone Parquet

```
DataFormatAwareEngine.refresh("source")

PHASE 1: Acquire current state
  catalogSnapshot = catalogSnapshotManager.acquireSnapshot()
  existingSegments = catalogSnapshot.getSegments()

PHASE 2: Flush child writers (sorted by generation ascending)
  For each writer: flush() → produce parquet file + DeleteEntry list + buffered deletes

PHASE 3: Apply buffered deletes to existing (parent) segments
  These are precise (parquetFile, rowId) deletes resolved at indexing time.
  → Load target segment's DeleteFormatWriter → markDeleted(rowId)

PHASE 4: DeleteEntry cascading across child writers (generation order)
  For each writer (lowest gen first):
    → Apply its DeleteEntry list to ALL older writers + parent
    → Resolves cross-writer conflicts where same _id written to multiple
      in-flight child writers simultaneously

PHASE 5: Write updated .liv files to disk
  For each segment with deletions → write .liv bitset file

PHASE 6: Build segments and delegate to engine refresh
  refreshInput = RefreshInput(existingSegments, newSegments)
  result = indexingExecutionEngine.refresh(refreshInput)

PHASE 7: Commit new CatalogSnapshot
  catalogSnapshotManager.commitNewSnapshot(result.refreshedSegments())
```

**Two types of deletes during refresh:**
- **Buffered deletes** — precise `(parquetFile, rowId)` from `DocVersionResolver` at index time
- **DeleteEntry (term-based)** — cross-writer sync; each writer's term list applied downward to older writers/parent

#### Composite
Same as Lucene standalone — updates/deletes go to Lucene IndexWriter; Parquet just appends.

---

## Delete Flow

### Case A: Deletes on the same generation writer

```
W1 - 1, 2, 3, 4, D
P
```

| Subcase | Behavior |
|---|---|
| Standalone Lucene | IndexWriter handles delete natively |
| Standalone Parquet | Buffer delete + record DeleteEntry (see below) |
| Composite | Delete goes to Lucene Writer only; Parquet no-ops |

**Parquet standalone delete flow:**
```
DataFormatAwareEngine.delete(Engine.Delete)
│
├─ PHASE 0: Version Resolution
│   ├─ LiveVersionMap check (fast) → IndexVersionValue or DeleteVersionValue
│   └─ DocVersionResolver.resolveVersion(reader, "abc") via DataFusion query
│
├─ PLANNING: DeletionStrategyPlanner.planOperationAsPrimary(delete)
│   ├─ null → NOT_FOUND → no-op
│   ├─ DeleteVersionValue → already deleted → no-op
│   ├─ version found → plan = executeOpOnEngine
│   └─ version conflict → skip with error
│
├─ PHASE 1: Execute Delete
│   ├─ writer.bufferDelete(parquetFile, rowId, generation)
│   └─ writer.addDeleteEntry(DeleteEntry(term=_id:"abc", seqNo))
│
├─ PHASE 2: Version Map Update & Translog
│   ├─ versionMap.putDeleteUnderLock(uid, DeleteVersionValue(...))
│   ├─ translogManager.add(Translog.Delete(...))
│   └─ localCheckpointTracker.markSeqNoAsProcessed(seqNo)
│
└─ return DeleteResult(version, primaryTerm, seqNo, found=true)
```

**Post-delete state (not yet searchable):**
- Writer's `DeleteFormatWriter.liveDocs` unchanged (own rows still live)
- `bufferedDeletes` contains the cross-segment delete target
- `DeleteEntry` list records the term for cross-writer cascade at refresh

### Case B: Deletes across different generation writers

Handled same as [Context Aware Segments](https://github.com/opensearch-project/OpenSearch/issues/19530) — delete applied to active writer, writer marked for flush, and parent writer in the sync code flow.

---

## Merge Flow

### Lucene / Composite
No change needed — regular Lucene segment merges handle liveBitset sync and delete expunging.

### Parquet Standalone

Uses **copy-on-write** strategy (same pattern as [Lucene's IndexWriter](https://github.com/apache/lucene/blob/50dac906/lucene/core/src/java/org/apache/lucene/index/IndexWriter.java#L4494)):

```
DeleteFormatWriter internal state:
  liveDocs         : Bits        (read-only, used by merges)
  writeableLiveDocs: FixedBitSet (writeable, null until first write)

getLiveDocs():
  writeableLiveDocs = null       ← disarms copy-on-write
  return liveDocs                ← returns read-only reference

getMutableBits():
  if writeableLiveDocs == null:
    if liveDocs != null:
      writeableLiveDocs = FixedBitSet.copyOf(liveDocs)  ← CLONE
    else:
      writeableLiveDocs = new FixedBitSet(numRows), all bits set
    liveDocs = writeableLiveDocs.asReadOnlyBits()       ← new reference
  return writeableLiveDocs

markDeleted(rowId):
  getMutableBits().getAndClear(rowId)
```

**Merge phases:**

```
PHASE 1: Select parquet files for merge, capture liveDocs snapshot
  For each source segment: prevLiveDocs[seg] = seg.DFW.getLiveDocs()
  (getLiveDocs nulls writeableLiveDocs — copy-on-write armed)

PHASE 2: Merge — copy live rows using prevLiveDocs
  Read source files row by row, skip deleted rows
  Build RowIdMapping: (oldRowId, oldGen) → newRowId
  Output: merged_genN.parquet

MEANWHILE: Concurrent deletes may arrive
  markDeleted() triggers copy-on-write → new writeableLiveDocs
  Merge doesn't see these (uses prevLiveDocs snapshot)

PHASE 3: commitMerge — carryOverHardDeletes (UNDER LOCK)
  For each source segment:
    Compare prevLiveDocs vs current liveDocs (reference identity check)
    If different → copy-on-write happened → new deletes during merge
    Map newly-deleted rowIds to merged segment via RowIdMapping
    Mark them deleted in merged segment's DFW

PHASE 4: Write .liv and commit
  If merged DFW has deletions → write merged_genN.liv
  Swap in CatalogSnapshot: remove source segments, add merged segment
  Release lock, delete old source files
```

---

## Search Flow

### Post-Filter Approach

Deleted documents are filtered using a pluggable `LiveBitsetFilter` applied as a post-filter after the prefetch stage.

```
TreeIndexReader::fetch_row_group(rg_idx)
│
├─ Compute effective doc range [min_doc, max_doc)
│
├─ PHASE 1: evaluate_tree_prefetch(tree, ctx, pp)
│   ├─ Per CollectorLeaf: collector.collect(min, max) → Roaring
│   ├─ Per PredicateLeaf: page_pruner → candidate row ranges → Roaring
│   └─ AND/OR/NOT: Roaring set operations
│   → PrefetchResult { candidates: RoaringBitmap }
│
├─ SKIP CHECK #1: candidates.is_empty()? → skip RG (no JNI, no I/O)
│
├─ FETCH LIVE DOCS (JNI call)
│   live_docs_filter.live_docs_bitmap(min, max)
│   → JNI: getLiveDocs(ctxId, providerId, collectorKey, min, max)
│   → Java: leafReader.getLiveDocs()
│     ├─ liveDocs == null → return null to Rust (all live)
│     └─ liveDocs != null → build long[] bitset for [min, max) → return
│   → Rust: null → Ok(None), long[] → convert to Roaring → Ok(Some(live_bm))
│
├─ APPLY LIVE DOCS: candidates &= live_bm (Roaring AND)
│
├─ SKIP CHECK #2: filtered candidates.is_empty()? → skip RG
│
└─ PHASE 2: Parquet data read + exact tree evaluation
   bitmap_to_offsets(&candidates, rg_first_row) → Vec<u64> offsets
   → PrefetchedRowGroup { rg, offsets, eval_nanos, index_bitmaps }
```

### Key Design Decisions

- **Null = all live** — when `getLiveDocs` returns null, no filtering overhead
- **Per-RG granularity** — only the `[min, max)` slice is transferred, not full segment bitset
- **Applied between prefetch and Phase 2** — after Lucene/page-pruning narrows candidates, before Parquet I/O
- **Pluggable** — `LiveBitsetFilter` abstraction supports both composite (Lucene `Bits`) and standalone (`.liv` file) sources

### Composite Mode

When Lucene is configured, `liveDocs` comes from `LeafReader.getLiveDocs()`. Lucene is the source of truth for delete tracking; Parquet doesn't need its own `.liv` files.

### Parquet Standalone Mode

`liveDocs` comes from the `DeleteFormatWriter`'s in-memory `FixedBitSet`, backed by the `.liv` file on disk.

---

## Clarifications

- For composite format, only Lucene + Parquet is considered currently
- If all documents of a segment file are deleted, that segment is dropped along with the corresponding parquet file
- **Concern noted:** Search/Indexing abstraction changes may become an issue
- Indexing benchmarking results: [Mustang Indexing Benchmarking](https://quip-amazon.com/ZSfqAu3yCHHg)
