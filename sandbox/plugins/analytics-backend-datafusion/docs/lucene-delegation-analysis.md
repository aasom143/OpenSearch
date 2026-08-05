# Lucene Delegation: Complete Analysis

## 1. Architecture Overview

### High-Level Flow

```
SQL/PPL Query
  -> Calcite Logical Plan (RelNode tree)
  -> HEP Marking Phase (LogicalFilter -> OpenSearchFilter, etc.)
  -> Volcano CBO (distribution traits, exchange insertion, split rules)
  -> Post-CBO Rewrites (TopK, Late Materialization)
  -> DAGBuilder (cuts plan at exchange boundaries into Stages)
  -> PlanForker (generates per-backend StagePlan alternatives)
  -> BackendPlanAdapter (adapts functions per-backend)
  -> PlanAlternativeSelector (scores and picks winner per stage)
  -> FragmentConversionDriver (serializes plan to backend-native bytes)
  -> Transport Dispatch (sends FragmentExecutionRequest per shard)
  -> Data-Node Execution (InstructionHandler + SearchExecEngine)
  -> Arrow Flight streaming back to coordinator
```

### Where Lucene Fits vs DataFusion

**DataFusion** is the universal execution engine. It reads parquet-format doc-values, supports all field types (numeric, keyword, date, boolean, text, IP, arrays, maps), all aggregate functions (SUM, AVG, MIN, MAX, COUNT, percentiles, etc.), joins, window functions, sorts, projections with 100+ scalar functions, and spill-to-disk for memory-heavy operations.

**Lucene** fills two narrow but critical roles:

1. **Metadata driver (Lucene-as-driver):** For `COUNT(*)` queries over indexed keyword/text fields with compatible filters, Lucene can execute the entire stage via `IndexSearcher.count(query)` -- avoiding parquet I/O entirely. This is the "count fast path."

2. **Filter delegation peer (Lucene-as-peer):** When DataFusion drives execution but encounters predicates that only Lucene can evaluate (full-text MATCH, MATCH_PHRASE, etc.) or that Lucene can evaluate more efficiently (keyword EQUALS via term dictionary), those predicates are delegated to Lucene. DataFusion calls back into Lucene via FFM (Foreign Function & Memory) upcalls to obtain doc-ID bitmaps, which it uses to prune parquet row groups.

The relationship is asymmetric: DataFusion declares `supportedDelegations = {FILTER}` (it can initiate delegation); Lucene declares `acceptedDelegations = {FILTER}` (it can receive delegation requests). Lucene never initiates delegation to another backend.

---

## 2. What Actually Goes to Lucene Today (Production Default)

By default, the cluster setting `analytics.delegation.lucene.blocked_predicates` blocks most standard predicates. Only the following are **actually delegated** in production:

### Filter Predicates Delegated by Default

| Function | Sample Query | Lucene QueryBuilder |
|----------|-------------|-------------------|
| EQUALS | `field = 'apple'` | TermQueryBuilder |
| MATCH | `match(field, 'apple')` | MatchQueryBuilder |
| MATCH_PHRASE | `match_phrase(field, 'red apple')` | MatchPhraseQueryBuilder |
| MATCH_BOOL_PREFIX | `match_bool_prefix(field, 'app')` | MatchBoolPrefixQueryBuilder |
| MATCH_PHRASE_PREFIX | `match_phrase_prefix(field, 'red app')` | MatchPhrasePrefixQueryBuilder |
| MULTI_MATCH | `multi_match(['f1', 'f2'], 'apple')` | MultiMatchQueryBuilder |
| QUERY_STRING | `query_string(['field'], 'apple OR banana')` | QueryStringQueryBuilder (with fields) |
| SIMPLE_QUERY_STRING | `simple_query_string(['field'], 'apple')` | SimpleQueryStringBuilder |
| QUERY | `query('apple OR banana')` | QueryStringQueryBuilder (all fields) |
| WILDCARD_QUERY | `wildcard_query(field, 'app*e')` | WildcardQueryBuilder |
| REGEXP | `field REGEXP 'app.*'` | RegexpQueryBuilder |
| FUZZY | `fuzzy(field, 'aple')` | (registered) |
| WILDCARD | `wildcard(field, 'app*')` | (registered) |
| MATCHALL | `match_all()` | MatchAllQueryBuilder |

Field types: KEYWORD, TEXT, MATCH_ONLY_TEXT (full-text also supports CONSTANT_KEYWORD, WILDCARD_FIELD).

### Aggregations Delegated by Default

- **COUNT(*)** with empty group-set on KEYWORD, TEXT, MATCH_ONLY_TEXT in "lucene" format -- the "count fast path" via `IndexSearcher.count(query)`.
- Example: `SELECT COUNT(*) FROM index WHERE field = 'apple'`

### What CANNOT Go to Lucene

- Any numeric field type (INT, LONG, FLOAT, DOUBLE, DATE) -- no scan, filter, or aggregate capability
- Any aggregate other than COUNT (no SUM, AVG, MIN, MAX, PERCENTILE)
- Any sort or TopK operation
- Any projection or scalar function evaluation
- Any join or window function
- Any GROUP BY aggregation (only empty group-set COUNT qualifies)
- Predicates where both operands are expressions (column-vs-column, function-vs-literal)
- NOT IN (complemented point sets), Sarg.isAll(), Sarg.isNone()
- Parquet-format fields -- Lucene only handles its own "lucene" format

---

## 3. Additional Lucene Capabilities (Blocked by Default)

These predicates are registered in Lucene's capability set but are **blocked by default** via `analytics.delegation.lucene.blocked_predicates`. They can be enabled by removing them from the blocklist.

| Function | Sample Query | Lucene QueryBuilder | Why Blocked |
|----------|-------------|-------------------|-------------|
| NOT_EQUALS | `field != 'apple'` | BoolQuery{mustNot: TermQuery} | Performance concern |
| GREATER_THAN | `field > 'apple'` | RangeQuery.gt() | Performance concern |
| GREATER_THAN_OR_EQUAL | `field >= 'apple'` | RangeQuery.gte() | Performance concern |
| LESS_THAN | `field < 'apple'` | RangeQuery.lt() | Performance concern |
| LESS_THAN_OR_EQUAL | `field <= 'apple'` | RangeQuery.lte() | Performance concern |
| IS_NULL | `field IS NULL` | BoolQuery{mustNot: ExistsQuery} | Performance concern |
| IS_NOT_NULL | `field IS NOT NULL` | ExistsQueryBuilder | Performance concern |
| LIKE | `field LIKE '%apple%'` | WildcardQuery (case-insensitive) | Performance concern |
| SARG_PREDICATE | `field IN ('apple', 'banana')` | TermsQuery or RangeQuery(s) | Performance concern |

Note: LIKE is restricted to KEYWORD only. SARG only handles keyword/text fields.

**Blocklist setting:** `analytics.delegation.lucene.blocked_predicates` -- hot-reloadable, no restart required.

---

## 4. Two Types of Delegation: Correctness vs Performance

When DataFusion drives execution, predicates delegated to Lucene fall into two fundamentally different categories:

### Correctness Delegation (`delegated_predicate`)

Predicates that ONLY Lucene can evaluate — DataFusion has no way to compute them from parquet.

**Example:** `match(field, 'apple')` — requires Lucene's inverted index.

**Behavior:**
- Original expression is replaced with a TRUE placeholder in the plan.
- Lucene Provider (Weight) created eagerly at query start.
- Collector called on **every RG** — the bitmap is the sole source of truth.
- If Lucene fails → query fails.

### Performance Delegation (`delegation_possible`)

Predicates that BOTH DataFusion AND Lucene can evaluate. DataFusion evaluates natively; Lucene is consulted **only when DataFusion's own pruning isn't selective enough**.

**Example:** `field = 'apple'` on a KEYWORD field — DataFusion pushes to parquet stats/bloom, but Lucene's term dictionary may prune better on high-cardinality data.

**Behavior:**
- Original expression is preserved — DataFusion evaluates it natively for page-pruning.
- Lucene Provider created **lazily** (first time it's needed).
- Per RG, selectivity gate decides: if DataFusion kept >5% of the RG → consult Lucene, else skip.
- When consulting: creates a fresh collector per-RG (2 FFM round-trips), AND-intersects bitmap with DataFusion's candidates.
- If Lucene unavailable → DataFusion still evaluates correctly (no failure).

### How it's determined

A predicate becomes performance-delegated when it is **dual-viable** (both DataFusion and Lucene declare capability for it). At plan narrowing time, the non-driving backends become "performance delegation peers."

### Constraint: Performance delegation is AND-only

`delegation_possible` is only supported in `SingleCollector` (AND-only filter trees). Under OR or NOT, performance-delegated predicates are promoted to correctness-delegation (always call Lucene).

| Filter Shape | What happens |
|---|---|
| `match(f,'x') AND field='apple'` | `match` → correctness, `field='apple'` → performance (consulted if DF >5%) |
| `match(f,'x') OR field='apple'` | Both → correctness (Lucene always called for both) |
| `field='apple' AND field2='banana'` | Both → performance (consulted only if DF keeps >5%) |
| `NOT(field='apple')` | Promoted to correctness (Lucene always called) |

### Summary

| Aspect | Correctness | Performance |
|--------|-------------|-------------|
| Can DataFusion evaluate? | No | Yes |
| Provider creation | Eager | Lazy |
| Collector called | Always | Only when DF >5% selective |
| Supported under OR/NOT? | Yes | No (promoted to correctness) |
| Example | `match(...)`, `match_phrase(...)` | `field = 'apple'` (KEYWORD EQUALS) |
| If Lucene fails | Query fails | DF still correct |

---

## 5. Opportunities to Improve Lucene Delegation (POC Ideas)

| # | POC | Problem | Investigation | Key Question | Complexity |
|---|-----|---------|---------------|--------------|------------|
| 1 | Evaluate blocked predicates | Range/IS_NULL/NOT_EQUALS/LIKE/IN are blocked by default. Are there cases where Lucene delegation actually helps for these? | Unblock individually, benchmark on representative workloads. Measure per-predicate-type: does Lucene bitmap narrow results beyond what DF page-pruning already achieves? | For which predicate types does Lucene add value over parquet column stats + bloom filters? | Small |
| 2 | Segment-scoped collector for performance delegation | For correctness collectors, we create once per segment and call `collectDocs` with per-RG `[min, max)` ranges. But for performance delegation, we create a fresh collector per-RG (because the gate decides per-RG). This means 2 FFM calls per qualifying RG instead of 1. | POC: on first RG where the gate fires, create a segment-scoped collector. Reuse it for all subsequent RGs where the gate fires. Saves one `create` FFM call per qualifying RG after the first. | Does the existing `OnceLock` pattern for the Provider extend naturally to a per-segment collector, or do we need a new lazy-init structure per segment? | Medium |
| 3 | Merge `delegation_possible` into correctness collector under AND | When AND tree has both a `Collector` (correctness) and `DelegationPossible` (performance) leaves, the correctness collector is already called for every RG. The performance leaf adds a second FFM round-trip for marginal extra pruning. Could we merge both predicates into a single BoolQuery on the Lucene side (one collector call returns the AND'd bitmap)? | POC: at `DelegatedPredicateCombiner` level, when both correctness and performance target the same backend under AND, combine into a single `DelegatedPredicateFunction` with a merged BoolQuery. Measure: is one Lucene call with a bigger query faster than DF evaluating the performance predicate natively? | Does DF page-pruning on the performance predicate provide more value than Lucene's merged bitmap? Need to compare: (DF page-prune + optional Lucene call) vs (single merged Lucene call, no DF pushdown for that predicate). | Medium |
| 4 | MIN/MAX on keyword via term dictionary | `SELECT MIN(keyword_field)` or `MAX(keyword_field)` — Lucene's `TermsEnum` gives first/last term in O(1) without parquet I/O. Same pattern as count fast path but for MIN/MAX. Today these go through full DataFusion scan. | Extend `LuceneSearchExecEngine` with a min/max path that reads first/last term from `TermsEnum`. Wire capability + plan selection. | How does this interact with filters? Need `TermsEnum` + Weight intersection for filtered MIN/MAX. | Medium |
| 5 | Within-query feedback on selectivity gate | The 5% gate decides per-RG independently. If Lucene adds zero value on the first N RGs (bitmap ≈ DF candidates), we still pay FFM cost on subsequent RGs. No learning within a query. | Track per-query running stats (how much Lucene narrows vs DF-only). After K RGs where Lucene pruned <X%, stop consulting for remaining RGs. | Risk of stopping too early — later RGs with different data distribution might benefit. Need a safe fallback. | Medium |
| 6 | Support `DelegationPossible` in Tree path | Currently performance-delegated predicates under OR/NOT are promoted to correctness (always call Lucene). This means a query like `match(f,'x') OR field='apple'` calls Lucene for both, even though DF can evaluate `field='apple'` natively. Could we support opportunistic consultation in the Tree evaluator? | POC: extend `BitmapTreeEvaluator` to handle `DelegationPossible` leaves — evaluate `original_expr` via Arrow kernels at refinement stage, optionally consult Lucene at candidate stage when the subtree cost justifies it. | What's the cost model? Under OR, skipping a performance leaf means the OR may over-estimate (safe for candidate stage superset). But refinement must evaluate the original_expr anyway for correctness. Is the Lucene candidate-stage bitmap worth the FFM call when the expr will be evaluated at refinement regardless? | Large |

---

## Appendix (Low-Level Details)

### A. How Delegation is Decided

**Resolution steps:**
1. **Marking:** `OpenSearchFilterRule` walks filter tree, looks up each predicate's viable backends via `CapabilityRegistry` (keyed by function+fieldType+format), applies blocklist, wraps as `AnnotatedPredicate`.
2. **DAG Building:** Plan cut at exchange boundaries into `Stage` objects.
3. **Plan Forking:** One `StagePlan` alternative generated per viable backend. Lucene alternative only for count-fast-path (empty GROUP BY, all COUNT, lucene-format fields).
4. **Alternative Selection:** Scored via `BackendShardPreference`. Lucene scores positive for count-fast-path when `analytics.planner.prefer_metadata_driver=true`.
5. **Fragment Conversion:** `AnnotationResolver` unwraps native predicates, serializes delegated predicates into `DelegatedExpression(annotationId, backendId, bytes)`.

**Blocklist:** `analytics.delegation.<backend>.blocked_predicates` — hot-reloadable, O(1) lookup, removes backend from viable set during marking.

**Key classes:** `CapabilityRegistry`, `OpenSearchFilterRule`, `DAGBuilder`, `PlanForker`, `PlanAlternativeSelector`, `FragmentConversionDriver`, `DelegationBlockList`.

### B. How Lucene Delegates Get Merged

**DelegatedPredicateCombiner** classifies filter tree nodes bottom-up and combines adjacent delegated predicates targeting the same backend into a single `DelegatedExpression` (minimizes cross-backend round-trips).

**Merging rules:**
- AND: all children to same backend → bubble up as single delegation
- OR/NOT: performance-delegated children demoted to correctness-delegation
- Mixed (delegated + native): correctness children combined; native stays

**Count fast path fusion:** When Lucene drives, Aggregate + Filter + TableScan collapse into one `IndexSearcher.count(query)` call. Filter tree converted: AND→BoolQuery.must, OR→BoolQuery.should, NOT→BoolQuery.mustNot.

**Filter tree shapes** (determines execution strategy):
- CONJUNCTIVE (AND-only) → single collector + bitset intersection
- INTERLEAVED_BOOLEAN_EXPRESSION (OR/NOT) → tree evaluator with per-row boolean logic

**Key classes:** `DelegatedPredicateCombiner`, `FilterTreeShapeDeriver`, `LuceneSubtreeConvertor`.

### C. Execution Path - From Plan to Lucene Call

**Lucene-as-driver (count fast path):**
1. `FragmentExecutionRequest` dispatched to shard
2. `LuceneScanInstructionHandler` → `LuceneSearchExecEngine`
3. Single synchronous `IndexSearcher.count(query)` call
4. Result: one-row Arrow batch with count, streamed via Arrow Flight

**DataFusion with Lucene delegation (indexed executor):**
1. Filter classified into `FilterClass`: None, SingleCollector, or Tree
2. Per correctness leaf: Provider (Weight) created eagerly via FFM upcall
3. Per row-group: collector fills doc-ID bitmaps, DataFusion intersects with parquet candidates
4. Execution on `DedicatedExecutor` (Rust tokio), FFM upcalls cross Rust→Java synchronously

**IndexSearcher lifecycle:** One shared reader per `CatalogSnapshot`, one shared `IndexSearcher` per reader (thread-safe). Released on snapshot deletion.

**Key classes:** `LuceneSearchExecEngine`, `LuceneScanInstructionHandler`, `LuceneReaderManager`, `indexed_executor.rs`, `single_collector.rs`.

### D. Query Serialization - RexNode to Lucene Query

**How it works:** Each `ScalarFunction` maps to a `DelegatedPredicateSerializer` in `QuerySerializerRegistry`. Serializer calls `buildQueryBuilder()` then serializes via OpenSearch NamedWriteable protocol.

**Key behaviors:**
- Exact-match subfield routing: text fields with `.keyword` subfield get routed to `field.keyword` (for EQUALS, comparisons, etc.) — relevance queries bypass this
- Boolean composition handled upstream (not per-serializer): AND→must, OR→should, NOT→mustNot
- All serializers require column-vs-literal (no column-vs-column or expressions)

**Limitations:**
- No NOT IN / complement
- LIKE always case-insensitive, column-on-left only
- REGEXP wraps with `.*...*` (substring semantics)
- No expressions/functions/casts as operands
- Numeric/date fields never reach Lucene serializers (excluded at capability level)

**Key classes:** `QuerySerializerRegistry`, `AbstractQuerySerializer`, `AbstractRelevanceSerializer`, `LuceneQueryConversionUtils`.