# Lucene-side Row Group Skip — Eliminating FFM Round-Trips

## Problem Statement

When a Lucene query has zero matching documents within a row group's doc range,
the current implementation still performs the full FFM round-trip per RG:

```
Rust (prefetch_rg)
  → FFM upcall: collectDocs(collectorKey, minDoc, maxDoc, outPtr, cap)
    → Java: FixedBitSet alloc, scorer iteration (empty), MemorySegment.copy
  ← FFM return: wordCount (0 useful data)
  → Rust: RoaringBitmap::from_lsb0_bytes → is_empty() → Ok(None)
```

Each FFM call costs ~1-5μs even for a no-op, plus FixedBitSet allocation and
memcopy. For selective queries on 20 segments × ~100 RGs, this adds hundreds of
microseconds of wasted work.

**Goal:** Eliminate `collectDocs` FFM calls for RGs with no matching docs, with
zero additional FFM calls and zero regression in the dense case.

## Background: Lucene API Landscape

### What Lucene exposes for range-existence checks

| Mechanism | What it gives | Cost | Destructive? |
|-----------|--------------|------|-------------|
| `Weight.scorer(leaf) == null` | Zero docs in segment | O(1) | No |
| `ScorerSupplier.cost()` | Estimated total matches (segment-wide) | O(1) | No |
| `DocIdSetIterator.advance(target)` | First doc >= target | O(skip-jump) | **Yes** |
| `Scorer.advanceShallow(target)` | Block upper bound containing target | O(skip-entry) | No |
| `ImpactsEnum.getImpacts().getDocIdUpTo(level)` | Skip block boundary | O(1) after advanceShallow | No |
| `DocIdSetIterator.docIDRunEnd()` | End of dense consecutive run | O(1) after positioning | No |
| `BulkScorer.score(collector, bits, min, max)` | Collect in [min,max); returns next-match estimate | O(matches in range) | Yes |
| `IntBlockTermState.singletonDocID` | Exact doc when docFreq==1 | O(0) — in term dict | No |
| `DocValuesSkipper` | Hierarchical doc-ID intervals with value bounds | O(log N) | No |

### What Lucene does NOT expose

- **No `lastDoc` in term metadata** — not stored. Requires walking all skip
  blocks: O(docFreq / 8192).
- **No `firstDoc` without advancing** — `iterator().nextDoc()` is destructive.
- **No range-existence predicate** — no API answers "are there docs in [X, Y)?"
  without either advancing an iterator or having a `DocValuesSkipper`.
- **`advanceShallow` doesn't confirm existence** — positions metadata only; the
  block might be sparse.
- **`Weight.count()` has no range-limited variant** — always counts entire segment.

### Key enabling fact: Two-scorer pattern is explicitly supported

From `Weight.java` javadoc:
> "A scorer for the same LeafReaderContext instance may be requested multiple
> times as part of a single search call."

Creating a probe scorer + a collection scorer from the same Weight is a
standard Lucene pattern (`IndexOrDocValuesQuery` does this).

## Design: Widened `createCollector` with Probe Scorer

### Core Idea

Since `advance()` is the only reliable per-RG existence check, and it's
destructive, we use the **two-scorer pattern**:

1. At `createCollector` time, create a **probe scorer** (lightweight, for
   range-existence scanning).
2. The probe scorer scans ALL RG boundaries in a single forward pass.
3. Return the match bitmap to Rust alongside the collector key.
4. The original (collection) scorer remains untouched for actual `collectDocs`.

This happens inside the existing `createCollector` FFM call — **zero additional
FFM round-trips**.

### FFM Interface Change

```
// Current signature:
createCollector(contextId, providerKey, writerGeneration, minDoc, maxDoc) → collectorKey|(-1)

// New signature:
createCollectorWithProbe(
    contextId: i64,
    providerKey: i32,
    writerGeneration: i64,
    minDoc: i32,
    maxDoc: i32,
    numRanges: i32,            // number of RG ranges to probe
    rgMinsPtr: *const i32,     // array of RG lower bounds (ascending)
    rgMaxsPtr: *const i32,     // array of RG upper bounds (ascending)
    outMatchBitmapPtr: *mut u8 // output: 1 byte per RG (1=may match, 0=skip)
) → i64
    // Return value encoding:
    //   -1: error
    //   -2: empty segment (null scorer — all RGs can be skipped)
    //   >= 0: lower 32 bits = collectorKey; upper 32 bits = firstDoc
```

### Java Implementation

```java
@Override
public long createCollectorWithProbe(
        int providerKey, long writerGeneration,
        int minDoc, int maxDoc,
        int[] rgMins, int[] rgMaxs, byte[] outMatch) {

    Weight weight = weightsByProviderKey.get(providerKey);
    if (weight == null) return -1;

    // ... existing segment lookup (leaf) ...

    // === Phase A: Segment-empty check ===
    Scorer scorer = weight.scorer(leaf);
    if (scorer == null) {
        Arrays.fill(outMatch, (byte) 0);
        return -2;  // Signal: no docs in segment
    }

    int collectorKey = nextCollectorKey.getAndIncrement();
    scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, minDoc, maxDoc));

    // === Phase B: Cost-based gate ===
    // Skip probe if estimated matches exceed 5% of segment. At that density,
    // docs are spread across nearly all RGs — probing would return all-1s.
    long cost = scorer.iterator().cost();
    long segMaxDoc = leaf.reader().maxDoc();
    if (cost * 20 > segMaxDoc) {
        Arrays.fill(outMatch, (byte) 1);
        return ((long) 0 << 32) | (collectorKey & 0xFFFFFFFFL);
    }

    // === Phase C: Probe scorer for RG-level skip ===
    // Create a second scorer — cheap (just seeks to posting list start)
    Scorer probeScorer = weight.scorer(leaf);
    DocIdSetIterator probeIter = probeScorer.iterator();
    int probeDoc = probeIter.nextDoc();  // position at first match
    int firstDoc = probeDoc;

    for (int i = 0; i < rgMins.length; i++) {
        if (probeDoc == DocIdSetIterator.NO_MORE_DOCS) {
            // No more matches — all remaining RGs are empty
            outMatch[i] = 0;
            continue;
        }
        if (probeDoc < rgMins[i]) {
            probeDoc = probeIter.advance(rgMins[i]);
        }
        outMatch[i] = (probeDoc < rgMaxs[i]) ? (byte) 1 : (byte) 0;
    }

    // Pack collectorKey + firstDoc into i64 return
    return ((long) firstDoc << 32) | (collectorKey & 0xFFFFFFFFL);
}
```

### Why a Probe Scorer is Cheap

| Query Type | Cost of creating 2nd scorer |
|-----------|----------------------------|
| TermQuery | O(1) — seek to posting list start (term dict lookup cached) |
| BooleanQuery (AND) | O(clauses) — each sub-iterator seeks to start |
| BooleanQuery (OR) | O(clauses) — same |
| PhraseQuery | O(terms) — each positional iterator seeks |
| Complex nested | Same as original scorer creation (cached Weight internals) |

The Weight already holds the compiled query state (automata, term states,
etc.). Creating a second Scorer from the same Weight just creates new iterators
pointing to the same posting lists. This is the same cost as the original
`createCollector` call itself.

### Rust Side

```rust
/// Result of createCollectorWithProbe FFM call.
pub struct CollectorProbeResult {
    pub collector_key: i32,
    pub first_doc: i32,
    pub rg_can_match: Vec<bool>,
}

pub enum CreateCollectorOutcome {
    /// Segment has matches; collector ready + per-RG match bitmap.
    Active(CollectorProbeResult),
    /// Segment has zero matches. Skip all RGs.
    SegmentEmpty,
}

pub fn create_collector_with_probe(
    context_id: i64,
    provider_key: i32,
    writer_generation: i64,
    doc_min: i32,
    doc_max: i32,
    rg_boundaries: &[(i32, i32)], // (min, max) per RG
) -> Result<CreateCollectorOutcome, String> {
    let create_fn = load_create_collector_with_probe()?;
    let num_ranges = rg_boundaries.len();
    let rg_mins: Vec<i32> = rg_boundaries.iter().map(|(m, _)| *m).collect();
    let rg_maxs: Vec<i32> = rg_boundaries.iter().map(|(_, m)| *m).collect();
    let mut out_match = vec![0u8; num_ranges];

    let result = unsafe {
        create_fn(
            context_id, provider_key, writer_generation,
            doc_min, doc_max,
            num_ranges as i32,
            rg_mins.as_ptr(), rg_maxs.as_ptr(),
            out_match.as_mut_ptr(),
        )
    };

    match result {
        -1 => Err("createCollectorWithProbe failed".into()),
        -2 => Ok(CreateCollectorOutcome::SegmentEmpty),
        packed => {
            let collector_key = (packed & 0xFFFFFFFF) as i32;
            let first_doc = (packed >> 32) as i32;
            let rg_can_match = out_match.iter().map(|&b| b != 0).collect();
            Ok(CreateCollectorOutcome::Active(CollectorProbeResult {
                collector_key,
                first_doc,
                rg_can_match,
            }))
        }
    }
}
```

### Integration in `prefetch_rg`

```rust
impl RowGroupBitsetSource for SingleCollectorEvaluator {
    fn prefetch_rg(&self, rg: &RowGroupInfo, min_doc: i32, max_doc: i32)
        -> Result<Option<PrefetchedRg>, String>
    {
        // NEW: Check probe result first — zero cost
        if let Some(ref rg_can_match) = self.rg_can_match {
            if let Some(&pos) = self.rg_index_to_pos.get(&rg.index) {
                if !rg_can_match[pos] {
                    return Ok(None);  // Skip — no FFM call
                }
            }
        }

        // Existing cascade (stats prune, page prune, bloom, collectDocs)
        // ... unchanged ...
    }
}
```

## Placement in the Pruning Cascade

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ [NEW] Segment-empty (createCollector → -2)         → EmptySegmentEvaluator  │
│   ↓ segment has matches                                                     │
│ [NEW] Probe-scorer RG skip (rg_can_match[pos])     → Ok(None)              │
│   ↓ RG may have matches                                                     │
│ RG-level column stats (StatsPruneTree — PR #22051)                          │
│   ↓ survives                                                                │
│ Page-level stats (PruningPredicate)                                         │
│   ↓ survives                                                                │
│ Bloom filter                                                                │
│   ↓ survives                                                                │
│ collectDocs FFM call (ONLY for RGs that survived ALL above)                 │
│   ↓ bitmap                                                                  │
│ Page-range intersection + peer consultation                                 │
│   ↓ final candidates                                                        │
│ is_empty() → skip or decode parquet                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

Note: The new probe check is placed BEFORE the parquet-metadata-based checks
(stats, page, bloom) because it's purely in-memory (a Vec<bool> lookup) and
catches cases those checks can't — e.g., a term query where the term exists in
the parquet file's stats (min/max include the value) but the Lucene posting
list has no docs in that range.

## Performance Analysis

### Cost-Based Gate: Avoiding Probe Overhead for Dense Queries

Benchmarking revealed that the probe scanner imposes a constant ~16ms
overhead per query (across all segment-chunks) due to creating a second
scorer and scanning all RG boundaries. For dense queries (terms matching
a large fraction of docs), probing always returns all-1s — pure waste.

**Solution:** Gate the probe on `scorer.iterator().cost()`:

```java
long cost = scorer.iterator().cost();   // estimated matches in segment
long segMaxDoc = leaf.reader().maxDoc();
if (cost * 20 > segMaxDoc) {            // cost > 5% of segment
    Arrays.fill(outMatch, (byte) 1);    // skip probe, assume all match
    return packed;
}
```

**Why 5%?** On a uniformly-distributed dataset with N row groups, a term
matching >5% of docs will statistically appear in nearly every RG. The
probability that a random RG of size `segMaxDoc/N` contains zero matches
when selectivity is 5% is `(1 - 0.05)^(segMaxDoc/N)` — negligible for
typical RG sizes (100K+ rows). Probing would find 0 skippable RGs.

**`cost()` is O(1):** `DocIdSetIterator.cost()` returns the estimated
match count from term metadata (docFreq for TermQuery, min/sum of
clauses for BooleanQuery). No iteration or allocation required.

### Dense case (cost > 5%) — ZERO REGRESSION

```
createCollectorWithProbe:
  - Creates 1 scorer (collection scorer only)
  - Checks cost() vs segMaxDoc — O(1)
  - Returns all-1s outMatch immediately
  - No probe scorer created, no advance scan

Net cost: ~0 extra vs old createCollector (same single scorer + one
          cost() call). No per-RG overhead.
```

### Selective case (5 of 100 RGs have matches)

```
createCollectorWithProbe:
  - Probe scan: advance jumps over empty ranges via skip list
  - outMatch = [0,0,0,1,0,...,0,1,0,...] (5 ones)
  - 95 RGs: immediate Ok(None) — no FFM collectDocs call
  - 5 RGs: proceed through full cascade + collectDocs

Savings: 95 × ~3μs (FFM overhead) = ~285μs saved
Cost: ~5μs extra in createCollector
Net: ~280μs improvement per segment-chunk
```

### Segment-empty case (term doesn't exist in segment)

```
createCollector returns -2:
  - EmptySegmentEvaluator for all RGs
  - Zero per-RG work whatsoever

Savings: ALL collectDocs calls eliminated for that segment
```

### Summary Table (20 segments × 5 RGs/segment = 100 RGs total)

| Scenario | Baseline FFM calls | After | Additional FFM calls |
|----------|-------------------|-------|---------------------|
| S1: Term in 1 segment | 100 collectDocs | 2-3 collectDocs | **0** |
| S2: Term in 5 segments | 100 collectDocs | ~10 collectDocs | **0** |
| S3: All segs, few RGs each | 100 collectDocs | ~10-20 collectDocs | **0** |
| S4: Dense (all match) | 100 collectDocs | 100 collectDocs | **0** |

## Backwards Compatibility

### Approach: New callback alongside existing one

Keep `createCollector` unchanged for backwards compat. Register the new
`createCollectorWithProbe` as a 6th FFM callback. On the Rust side:

```rust
// If new callback is registered → use it
// If not registered (e.g., non-Lucene backend) → fall back to old createCollector
if load_create_collector_with_probe().is_ok() {
    // New path with probe
} else {
    // Legacy path — existing behavior
}
```

The `FilterDelegationHandle` interface gets a default method:

```java
default long createCollectorWithProbe(
    int providerKey, long writerGeneration,
    int minDoc, int maxDoc,
    int[] rgMins, int[] rgMaxs, byte[] outMatch
) {
    // Default: no probing, delegate to old createCollector
    Arrays.fill(outMatch, (byte) 1);  // conservative: all may match
    return createCollector(providerKey, writerGeneration, minDoc, maxDoc);
}
```

## Alternative Considered: BulkScorer.score() Return Value

The research revealed that `BulkScorer.score(collector, bits, min, max)` returns
an under-estimate of the next matching doc >= max. When it returns
`NO_MORE_DOCS`, all subsequent ranges can be skipped.

However, this requires actually executing the scorer with a collector — it
doesn't offer a pre-check. And the "next doc" return value is only useful for
trailing-RG elimination (once exhausted), not for interior gaps. The probe
scorer approach is strictly more powerful.

## Alternative Considered: advanceShallow for Non-Destructive Check

`Scorer.advanceShallow(target)` positions skip metadata without consuming the
iterator. But it only gives block boundaries (level0LastDocID / level1LastDocID)
— not whether docs actually exist within that block. A block boundary of
[target, level0LastDocID] means "if there are docs, they're in this range" —
not "there ARE docs." Insufficient for our needs.

## Alternative Considered: firstDoc/lastDoc Only (No Per-RG Probe)

We could just extract `firstDoc` from the probe scorer (the first `nextDoc()`)
and approximate `lastDoc`. This eliminates leading/trailing RGs but misses
interior gaps. The probe-scan approach costs ~the same as getting firstDoc
(one forward pass) but gives complete per-RG information.

## File Impact

| File | Change |
|------|--------|
| `analytics-framework/.../FilterDelegationHandle.java` | Add `createCollectorWithProbe` default method |
| `analytics-backend-lucene/.../LuceneFilterDelegationHandle.java` | Implement `createCollectorWithProbe`: two-scorer creation, probe scan, packed return |
| `analytics-backend-datafusion/.../FilterTreeCallbacks.java` | Add static `createCollectorWithProbe` router |
| `analytics-backend-datafusion/.../NativeBridge.java` | Register 6th FFM callback |
| `rust/src/indexed_table/ffm_callbacks.rs` | Add callback type, AtomicPtr slot, `create_collector_with_probe()` wrapper, `CreateCollectorOutcome` enum |
| `rust/src/indexed_table/eval/mod.rs` | Add `EmptySegmentEvaluator` |
| `rust/src/indexed_table/eval/single_collector.rs` | Add `rg_can_match: Option<Vec<bool>>` field; check in `prefetch_rg` before cascade |
| `rust/src/indexed_executor.rs` | Call new probe API; pass `rg_can_match` to evaluator; handle -2 with EmptySegmentEvaluator |
| `rust/src/indexed_table/eval/bitmap_tree.rs` | Tree path: handle segment-empty + per-leaf rg_can_match |

## Metrics / Observability

New counters (EXPLAIN ANALYZE):

| Metric | Meaning |
|--------|---------|
| `segment_empty_skip` | Segments where probe returned -2 |
| `rg_probe_skip` | RGs skipped by probe bitmap (before stats/bloom/collectDocs) |
| `probe_scan_time` | Time spent in createCollectorWithProbe (amortized per segment) |

Existing counters reflecting improvement:
- `rg_skipped` — increases (more RGs hitting Ok(None))
- `ffm_collector_calls` — decreases (fewer collectDocs calls)

## Correctness

1. **Probe scorer is independent:** Two scorers from the same Weight on the same
   leaf are explicitly supported by Lucene's contract. The probe scanner's
   `advance()` calls don't affect the collection scorer.

2. **Probe is conservative on `true`:** If `advance(rgMin) < rgMax`, there IS
   at least one doc in [rgMin, rgMax). This is exact — no false positives.

3. **Probe is exact on `false`:** If `advance(rgMin) >= rgMax`, the
   forward-only iterator guarantees no docs exist in [rgMin, rgMax). No false
   negatives.

4. **Ascending RG order:** RG boundaries are passed in ascending order (same
   as the stream processes them). The probe iterator advances monotonically.

5. **Collection scorer unaffected:** The collection scorer's
   `DocIdSetIterator` starts at position -1 (pre-`nextDoc()`). Subsequent
   `collectDocs` calls advance it forward from there — identical to today.

6. **Default method is safe:** Non-Lucene backends that don't override
   `createCollectorWithProbe` get outMatch filled with 1s — identical to
   current behavior (no skips, no regressions).

## Testing Strategy

1. **Unit test (Java):** Term in 1 of 3 segments → `createCollectorWithProbe`
   returns -2 for 2 segments; third returns key + outMatch with correct pattern.

2. **Unit test (Java):** 10-RG segment, term matches in RGs 3 and 7 → outMatch
   = [0,0,0,1,0,0,0,1,0,0].

3. **Unit test (Java):** Dense match → outMatch all 1s (no false negatives).

4. **Unit test (Rust):** `EmptySegmentEvaluator` always returns Ok(None).

5. **Unit test (Rust):** `SingleCollectorEvaluator` with `rg_can_match =
   [false, true, false]` → only middle RG calls `collect_packed_u64_bitset`.

6. **Integration test:** Multi-segment index with selective query. EXPLAIN
   ANALYZE shows `segment_empty_skip > 0`, `rg_probe_skip > 0`,
   `ffm_collector_calls` reduced.

7. **Regression test:** Dense query shows latency within noise of baseline.

## Rollout Plan

Single PR:
1. Add `createCollectorWithProbe` to `FilterDelegationHandle` (default method).
2. Implement in `LuceneFilterDelegationHandle` (probe scorer logic).
3. Register 6th FFM callback in `NativeBridge` + `FilterTreeCallbacks`.
4. Rust: new callback type, probe result handling, `EmptySegmentEvaluator`,
   `rg_can_match` field in evaluators.
5. `indexed_executor.rs`: wire up new API, pass probe results to evaluators.
6. Tests + benchmarks.

Fallback: If the new callback isn't registered (older Java side), Rust
automatically falls back to existing behavior.
