# Search Support for Deleted Documents

## Context

After PRs #22406 (ingestion-side deletes) and #21597 (merge-time dead row removal) land, the indexing side delivers:
- Hard deletes tracked in Lucene's `liveDocs` bitset (composite mode)
- Rows stay in parquet at original offset — 1:1 correspondence maintained
- Fully-deleted segments dropped from catalog snapshot via `droppedGenerations`
- During merge, dead rows are physically removed from parquet output

The search side must filter deleted rows at query time for rows not yet merged away.

## Execution Paths

| Path | Name | Description |
|---|---|---|
| **A** | Pure DF | No Collector leaves — plain DataFusion parquet scan |
| **B** | SingleCollector | `AND(Collector, Predicate?, ...)` — single Lucene collector + optional DF predicates |
| **C** | BitmapTree | Multiple Collectors, OR, NOT, nested — full boolean tree evaluation |

## When Live Docs Filtering Is Needed

| Scenario | Deletes already handled? | Needs filtering? |
|---|---|---|
| Path A (no filter) | No — no Lucene involvement | **Yes** |
| Path B with correctness Collector | Yes — `Weight.scorer(leaf)` respects `liveDocs` | No |
| Path B with only performance delegations | Partially — selectivity gate may skip Lucene path for some RGs | **Yes** |
| Path C with Predicate-only branches | No — Predicate branch bypasses Lucene | **Yes** |

## Approach

Reuse the existing `collectDocs` delegation infrastructure. Inject a **MatchAll** delegation as a conjunctive Collector when the segment has deletions. The MatchAll scorer's iterator automatically skips deleted docs because `Weight.scorer(leaf)` operates on a `LeafReader` that respects `liveDocs` internally. This avoids any new FFM callbacks.

### How It Works

1. **Detection:** `hasDeletedDocs(reader)` — iterate leaves, return `true` if any `leafReader.getLiveDocs() != null`

2. **Tree coverage check:** Run the coverage algorithm (below) on the filter tree. If root evaluates to `1`, the existing tree already guarantees live-docs filtering — skip injection.

3. **Injection:** When root evaluates to `0`, add a MatchAll delegation ANDed with the existing filter tree:
   - Path A → promoted to Path B (MatchAll becomes the sole correctness Collector)
   - Path B (performance-only) → MatchAll added as correctness Collector
   - Path C → `AND(MatchAll, OriginalTree)`

4. **Execution:** The MatchAll scorer iterates all live docs in the range via `Weight.scorer(leaf)` — Lucene's `LeafReader` automatically skips deleted docs through `liveDocs`.

### When to Inject MatchAll — Tree Coverage Algorithm

Rather than hard-coding per-path rules, we determine whether the filter tree already guarantees live-docs coverage by propagating a boolean signal up the tree:

**Leaf values:**
- Correctness Collector (`DelegatedPredicate`) → `1` (scorer respects `liveDocs`)
- Performance Collector (`DelegationPossible`) → `0` (may skip Lucene path)
- DF Predicate → `0` (no Lucene involvement)

**Node propagation:**
- `AND(children)` → `OR(children)` — if ANY child is covered, the AND intersection is safe (the covered child constrains the result to live docs only)
- `OR(children)` → `AND(children)` — ALL children must be covered (any uncovered branch can contribute deleted rows)
- `NOT(child)` → passes through child value

**Decision:**
- Root evaluates to `1` → **no injection needed** (tree already guarantees all results are live)
- Root evaluates to `0` → **inject MatchAll** as conjunctive live-docs Collector

**Example:**
```
                        OR  → AND(1, 0) = 0 ✗ inject MatchAll
                      /    \
                AND = 1     NOT → 0
                  / | \        \
                /   |   \      [DF]=0
               /    |    \
          [C]=1  [DF]=0   OR → AND(0, 0) = 0
                         /  \
                     [P]=0  AND → OR(0, 0) = 0
                           /   \
                       [DF]=0  [P]=0

Evaluation (bottom-up):
  inner AND(DF=0, P=0)       → OR(0, 0)  = 0
  OR(P=0, inner AND=0)       → AND(0, 0) = 0
  left AND(C=1, DF=0, OR=0)  → OR(1, 0, 0) = 1
  NOT(DF=0)                  → 0
  root OR(left AND=1, NOT=0) → AND(1, 0) = 0  → inject MatchAll

The left AND subtree is covered (has a correctness Collector), but the
root OR means both branches must be independently safe. The NOT(DF)
branch can contribute deleted rows — so MatchAll is injected.

Legend: [C] = Correctness, [P] = Performance, [DF] = DF Predicate
```

See [Appendix](#appendix-additional-tree-examples) for simpler examples.

**Pre-condition:**
- `hasDeletedDocs == false` → skip evaluation entirely, never inject

## Changes Required

### Java Side

| File | Change |
|---|---|
| `AnalyticsSearchBackendPlugin` (SPI) | Add `hasDeletedDocs(Reader)` default method returning `false` |
| `LuceneAnalyticsBackendPlugin` | Override: iterate leaves, check `getLiveDocs() != null` |
| `AnalyticsSearchService` | Detect deletes, call `buildLiveDocsDelegation()` when needed |
| `ShardScanInstructionHandler` | When live-docs delegation present: adjust filter tree shape + delegated predicate count |
| `LuceneFilterDelegationHandle` | In `createCollector`: fetch `leaf.reader().getLiveDocs()` and store on `ScorerHandle`. In `collectDocs`: check `liveDocs.get(docId)` before setting bit |

### No Rust / FFM Changes

The Rust side already handles conjunctive delegated predicates. The live-docs delegation appears as a normal Collector leaf to the evaluator — no new callback needed.

## Performance Considerations

### Segment-Level Optimization
- Segments with no deletions: zero overhead (no delegation injected)
- Segments with deletions + correctness Collector: zero overhead (already filtered)
- Segments with deletions + no correctness Collector: one additional `collectDocs` FFM call per RG with MatchAll scorer
- After merge: dead rows physically removed — no filtering needed for merged segments

### Piggyback on `collectDocs` via nextDoc (PR #22493)
The per-RG FFM round-trip cost for the live-docs MatchAll Collector is eliminated by [PR #22493](https://github.com/opensearch-project/OpenSearch/pull/22493), which piggybacks `nextDoc` advancement on the `collectDocs` call. The live-docs bitset for the next RG is returned as part of the same FFM call — no separate round-trip needed.

## Benchmarking Plan

### Baseline
- 100M documents, no deletions — establish query performance baseline

### Delete Scenarios
| Deletion % | Documents Deleted | Purpose |
|---|---|---|
| 10% | 10M | Low-deletion typical workload |
| 20% | 20M | Moderate churn |
| 25% | 25M | Heavy update workload |
| 50% | 50M | Stress test / worst case before merge |

### What to Measure
- Query latency (p50, p95, p99) across all three paths (A, B, C)
- FFM round-trip overhead per RG for live-docs Collector
- Merge impact: latency improvement after segments are merged (dead rows removed)
- Memory overhead of segment-level bitset caching

### Test Matrix
- Vary query type: match_all (Path A), single term (Path B), boolean OR (Path C)
- Vary segment count: 1 segment vs. many segments
- Compare: before/after merge compaction

---

## Appendix: Additional Tree Examples

Simple cases showing the coverage algorithm:

```
Skip — correctness Collector covers AND:

        AND → OR(1, 0) = 1 ✓
       /   \
  [C]=1    [DF]=0
```

```
Inject — OR requires all branches covered:

        OR → AND(1, 0) = 0 ✗
       /   \
  [C]=1    [DF]=0
```

```
Skip — correctness Collector at top AND covers uncovered subtree:

            AND → OR(1, 0) = 1 ✓
           /   \
      [C]=1     OR → AND(0, 0) = 0
               /  \
          [P]=0   [DF]=0
```

```
Inject — OR at root, right subtree uncovered:

            OR → AND(1, 0) = 0 ✗
           /   \
      [C]=1     AND → OR(0, 0) = 0
               /   \
          [P]=0    [DF]=0
```

```
Skip — all branches fully covered:

              AND → OR(1, 1) = 1 ✓
             /   \
           OR     AND → OR(1, 0) = 1
          / \        /   \
     [C]=1 [C]=1  [C]=1  [DF]=0

  left OR  → AND(1, 1) = 1
  right AND → OR(1, 0) = 1
  root AND  → OR(1, 1) = 1
```

Legend: [C] = Correctness, [P] = Performance, [DF] = DF Predicate
