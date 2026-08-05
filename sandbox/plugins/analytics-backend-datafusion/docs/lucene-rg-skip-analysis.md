# Lucene RG-Skip Probe: Analysis & Benchmark Data

## How Delegation Maps to Evaluator Path

| Java `treeShape` | `delegatedExpressions` | Rust `FilterClass` | Evaluator | Probe Implemented? |
|---|---|---|---|---|
| `NO_DELEGATION(0)` | 0 | `None` | PredicateOnlyEvaluator | No — no collector |
| `CONJUNCTIVE(1)` | ≥1 | `SingleCollector` | **SingleCollectorEvaluator** | **Yes** |
| `CONJUNCTIVE` (logged as 0) | 0 (empty delegated list) | `None` | PredicateOnlyEvaluator | No — falls to plain scan |
| `INTERLEAVED_BOOLEAN_EXPRESSION(2)` | ≥1 | `Tree` | BitmapTreeEvaluator | **Not yet — TODO** |

### Key Insight: What "delegated" means

A predicate is **delegated** when its target backend differs from the driving backend:
- `match(Body, 'error')` on a text field → delegated to Lucene (peer backend)
- `SeverityNumber >= 13` → native DataFusion predicate (driving backend)
- `ServiceName = 'frontend'` on keyword → delegated to Lucene (peer backend)

The `DelegatedPredicateCombiner` may fuse multiple same-backend predicates into
a single `DelegatedExpression`. So `match(Body, 'error') AND ServiceName = 'frontend'`
might become 1 or 2 delegated expressions depending on whether they're combined.

### Why Some CONJUNCTIVE Queries Get 0 Delegations

When ALL predicates are served by the same backend (e.g., all Lucene), there's
nothing to delegate to a peer. Example:

`match(Body, 'connection') AND match(Body, 'timeout') AND ServiceName = 'frontend'`
→ `CONJUNCTIVE(0)` → PredicateOnlyEvaluator (no collector, no probe)

The probe only fires when there's a MIX: Lucene-delegated AND DataFusion-native.

## SingleCollector Classification (Rust side)

`classify_filter(tree: &BoolNode)` returns `SingleCollector` when:
- Every `Collector`/`DelegationPossible` leaf is reachable ONLY through AND nodes
- OR/NOT nodes are allowed only if they wrap pure `Predicate` subtrees (no collectors under OR/NOT)

Examples that get SingleCollector:
```
AND(Collector, Predicate)                     → SingleCollector
AND(Collector, Predicate, Predicate)          → SingleCollector
AND(Collector, OR(Predicate, Predicate))      → SingleCollector (OR wraps only natives)
AND(Collector, NOT(Predicate))                → SingleCollector
```

Examples that get BitmapTree:
```
OR(Collector, Predicate)                      → Tree
NOT(Collector)                                → Tree
AND(Collector, OR(Collector, Predicate))      → Tree (Collector under OR)
```

## TextBench Dataset: Selectivity Reference

**1 Billion docs, 30 segments (14 large ~65M docs, 16 small ~1M docs), ~65 RGs per large segment**

### Body Field (text — Lucene delegated)

| Term | Docs | Selectivity | Probe fires? (< 5%) |
|------|------|-------------|---------------------|
| info | 252,975,675 | 25.30% | No |
| error | 93,638,388 | 9.36% | No |
| cache | 61,453,928 | 6.15% | No |
| order | 52,904,796 | 5.29% | No |
| product | 50,594,906 | 5.06% | No |
| server | 46,558,378 | 4.66% | **Yes** (borderline) |
| message | 70,336,664 | 7.03% | No |
| level | 51,202,236 | 5.12% | No |
| failed | 114,192,078 | 11.42% | No |
| connection | 19,519,370 | 1.95% | **Yes** |
| shipping | 19,648,925 | 1.96% | **Yes** |
| currency | 19,627,677 | 1.96% | **Yes** |
| http | 15,111,666 | 1.51% | **Yes** |
| trace | 5,146,579 | 0.51% | **Yes** |
| connect | 2,344,337 | 0.23% | **Yes** |
| payment | 2,066,686 | 0.21% | **Yes** |
| warning | 1,273,536 | 0.13% | **Yes** |
| memory | 361,340 | 0.04% | **Yes** |
| retry | 14,674 | 0.001% | **Yes** |
| exception | 16,146 | 0.002% | **Yes** |
| timeout | 1,613 | 0.0002% | **Yes** |

### ServiceName Field (keyword — Lucene delegated)

| Service | Docs | Selectivity |
|---------|------|-------------|
| cart | 428,637,901 | 42.86% |
| frontend | 292,303,194 | 29.23% |
| email | 88,714,071 | 8.87% |
| recommendation | 50,514,365 | 5.05% |
| payment | 37,233,183 | 3.72% |
| shipping | 28,034,269 | 2.80% |
| checkout | 27,086,483 | 2.71% |
| currency | 16,532,534 | 1.65% |
| fraud-detection | 1,622,565 | 0.16% |
| kafka | 1,035,097 | 0.10% |
| accounting | 962,868 | 0.10% |
| product-catalog | 184,483 | 0.02% |
| valkey-cart | 22,870 | 0.002% |
| load-generator | 261 | 0.00003% |

### SeverityNumber (byte — DataFusion native)

| Value | Docs | Selectivity |
|-------|------|-------------|
| 9 | 790,289,848 | 79.03% |
| 17 | 75,558,003 | 7.56% |
| 0 | 74,546,310 | 7.45% |
| 1 | 58,331,402 | 5.83% |
| 13 | 1,264,716 | 0.13% |
| 21 | 9,721 | 0.001% |

### Compound Query Selectivity (AND intersections)

| Query | Docs | Selectivity |
|-------|------|-------------|
| error + load-generator | 261 | 0.000026% |
| error + valkey-cart | 0 | 0% |
| error + product-catalog | 1,827 | 0.000183% |
| error + accounting | 0 | 0% |
| error + kafka | 1,247 | 0.000125% |
| error + fraud-detection | 387 | 0.000039% |
| timeout + frontend | 0 | 0% |
| timeout + cart | 0 | 0% |
| retry + frontend | 0 | 0% |
| exception + cart | 2,958 | 0.000296% |
| memory + frontend | 202,934 | 0.020293% |
| memory + cart | 0 | 0% |

### Cost-Based Gate Threshold

```
Threshold: cost * 20 > segMaxDoc  (i.e., cost > 5% of segment)

Large segment (65M docs): threshold = 3.25M docs
Small segment (1M docs):  threshold = 50K docs

scorer.iterator().cost() for AND queries = min(clause costs)
  → match(timeout) AND ServiceName='frontend'
    cost = min(1613, 292M) = 1613 → well below 5% → PROBE FIRES
```

## Benchmark Results: Probe Optimization (SingleCollector path)

### CONJUNCTIVE(2) — Selective Lucene + Native Parquet Predicate

These are the queries where the probe delivers measurable wins:

| Query | Main | Custom | Speedup |
|-------|------|--------|---------|
| retry+frontend+Sev>=13 | 808ms | 438ms | **1.8x** |
| timeout+frontend+Sev>=13 | 795-821ms | 429-448ms | **1.8-1.9x** |
| memory+frontend+Sev>=13 | 777-807ms | 506-552ms | **1.4-1.6x** |
| timeout+cart+Sev>=13 | 524-554ms | 404-435ms | **1.2-1.3x** |
| retry+cart+Sev17 | 515-546ms | 399-426ms | **1.2-1.3x** |
| deep AND impossible combo | 823ms | 443ms | **1.9x** |
| selective AND + count,min,max | 813ms | 412ms | **2.0x** |
| selective + group by SevNum | 783ms | 577ms | **1.4x** |

### CONJUNCTIVE(1) — Dense Lucene + Native (gate skips probe)

| Query | Main | Custom | Result |
|-------|------|--------|--------|
| error+Sev>=13 | 937-958ms | 962-966ms | **TIE** (no regression) |
| server+Sev>=13 | 709-716ms | 692-747ms | **TIE** |
| info+Sev17 | 742-749ms | 740-749ms | **TIE** |

### NO_DELEGATION / Fast Queries

| Query | Main | Custom | Result |
|-------|------|--------|--------|
| count() | 7-8ms | 7ms | **TIE** |
| match(error) | 8-9ms | 8ms | **TIE** |
| match(info) | 9ms | 8-9ms | **TIE** |
| ServiceName='cart' | 7ms | 6-7ms | **TIE** |

## TODO: Implement Probe for BitmapTree (INTERLEAVED_BOOLEAN_EXPRESSION)

### Current State

The BitmapTreeEvaluator handles `INTERLEAVED_BOOLEAN_EXPRESSION` queries where
Lucene-delegated and DataFusion-native predicates are mixed under OR/NOT.

Currently, BitmapTreeEvaluator creates a collector per leaf and calls `collectDocs`
for every RG on every collector leaf — no probe-based skipping.

### Challenge

Unlike SingleCollector where "Lucene empty → skip RG" is always safe (AND semantics),
in the tree case a Lucene-empty RG might still produce results from the native OR branch.

However, for each **individual collector leaf** in the tree, if the probe shows zero
docs for that leaf's posting list in a given RG, we can skip that leaf's `collectDocs`
call for that RG. The tree evaluation still proceeds with the native branches.

### Proposed Approach

1. At `createCollectorWithProbe` time for each tree leaf, return per-RG match flags
2. In `BitmapTreeEvaluator::prefetch_rg`, before calling `collectDocs` for a leaf,
   check that leaf's `rg_can_match[pos]`. If false, use an empty bitset for that
   leaf (zero docs), saving the FFM round-trip.
3. The tree evaluation logic combines leaf bitsets as before — empty bitsets just
   mean that branch contributes nothing for this RG.

### Expected Benefit

For queries like `match(timeout) OR SeverityNumber = 21`:
- `timeout` has 1,613 docs across 1B — most RGs have zero Lucene matches
- Currently: `collectDocs` FFM call for EVERY RG on the timeout leaf
- After: probe identifies empty RGs → skip `collectDocs` for timeout leaf
- The `SeverityNumber = 21` native branch still evaluates normally
- Net: eliminate ~95% of FFM calls for the Lucene leaf

### Files to Modify

| File | Change |
|------|--------|
| `rust/src/indexed_table/eval/bitmap_tree.rs` | Add `probe_rg_can_match` per leaf; check before `collect_docs` |
| `rust/src/indexed_executor.rs` | Call `create_collector_with_probe` for each tree leaf (not just the single collector) |
| `LuceneFilterDelegationHandle.java` | No change needed — same API works for tree leaves |
