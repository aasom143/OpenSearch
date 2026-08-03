/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Lucene implementation of {@link FilterDelegationHandle}. Compiles delegated expressions
 * into Lucene Queries, creates Weights on demand, and produces bitsets via Scorers.
 *
 * <p>Segments are resolved by <b>writer generation</b>. The mapping
 * {@code generation → Lucene leaf index} is provided by {@link LuceneReader}, which is
 * built once at refresh time in {@link LuceneReaderManager}.
 *
 * @opensearch.internal
 */
final class LuceneFilterDelegationHandle implements FilterDelegationHandle {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFilterDelegationHandle.class);

    // TODO: lazy query compilation for performance-delegated predicates. Today
    // every delegated expression is compiled (QueryBuilder → Lucene Query) at
    // ctor time. For correctness-delegated predicates (always called) this is
    // fine. For performance-delegated predicates that DF page-pruning may never
    // consult, the compile cost is wasted. Deferring needs a way to distinguish
    // the two kinds (e.g. add a kind field on DelegatedExpression) and clear
    // semantics for compile-failure timing (eager = fail at ctor, lazy = fail
    // at first use). Revisit if this surfaces as a real cost — needs revisiting.
    private final Map<Integer, Query> queriesByAnnotationId;
    private final DirectoryReader directoryReader;
    private final IndexSearcher searcher;
    private final List<LeafReaderContext> leaves;
    private final BooleanSupplier isCancelledSupplier;
    private final Map<Long, String> generationToSegmentName;

    /** Sentinel annotation ID for the injected live-docs delegation (no MatchAll query needed). */
    private static final int LIVE_DOCS_ANNOTATION_ID = Integer.MAX_VALUE;

    private final ConcurrentHashMap<Integer, Weight> weightsByProviderKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ScorerHandle> scorersByCollectorKey = new ConcurrentHashMap<>();
    /** Provider keys that are live-docs-only (no Weight — just read liveDocs directly). */
    private final ConcurrentHashMap<Integer, Boolean> liveDocsProviderKeys = new ConcurrentHashMap<>();
    private final AtomicInteger nextProviderKey = new AtomicInteger(1);
    private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

    LuceneFilterDelegationHandle(
        List<DelegatedExpression> expressions,
        QueryShardContext queryShardContext,
        LuceneReader luceneReader,
        CatalogSnapshot catalogSnapshot,
        NamedWriteableRegistry namedWriteableRegistry,
        BooleanSupplier isCancelledSupplier
    ) {
        assert luceneReader != null : "luceneReader must not be null";
        assert catalogSnapshot != null : "catalogSnapshot must not be null";
        this.directoryReader = luceneReader.directoryReader();
        this.searcher = queryShardContext.searcher();
        this.leaves = directoryReader.leaves();
        this.generationToSegmentName = luceneReader.generationToSegmentName();
        this.queriesByAnnotationId = compileQueries(expressions, queryShardContext, namedWriteableRegistry);
        this.isCancelledSupplier = isCancelledSupplier;
    }

    private static Map<Integer, Query> compileQueries(
        List<DelegatedExpression> expressions,
        QueryShardContext context,
        NamedWriteableRegistry registry
    ) {
        Map<Integer, Query> queries = new HashMap<>();
        for (DelegatedExpression expr : expressions) {
            // Live-docs sentinel: no query compilation needed — handled via direct liveDocs read.
            if (expr.getAnnotationId() == LIVE_DOCS_ANNOTATION_ID) {
                continue;
            }
            try {
                StreamInput rawInput = StreamInput.wrap(expr.getExpressionBytes());
                StreamInput input = new NamedWriteableAwareStreamInput(rawInput, registry);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                // Rewrite FieldExistsQuery → a postings-only equivalent: the lucene-secondary segment
                // has no doc_values/norms (they live in the parquet primary), so a FieldExistsQuery
                // built from an _exists_ clause (PPL `search field!=value`) would throw at rewrite().
                Query query = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(queryBuilder.toQuery(context));
                queries.put(expr.getAnnotationId(), query);
            } catch (IOException exception) {
                throw new IllegalStateException(
                    "Failed to deserialize delegated expression for annotationId=" + expr.getAnnotationId(),
                    exception
                );
            }
        }
        return queries;
    }

    @Override
    public int createProvider(int annotationId) {
        // Live-docs provider: no Weight/Scorer needed — collectDocs reads liveDocs bits directly.
        if (annotationId == LIVE_DOCS_ANNOTATION_ID) {
            int providerKey = nextProviderKey.getAndIncrement();
            liveDocsProviderKeys.put(providerKey, Boolean.TRUE);
            LOGGER.debug("[scf] createProvider annotationId=LIVE_DOCS → providerKey={}", providerKey);
            return providerKey;
        }
        Query query = queriesByAnnotationId.get(annotationId);
        if (query == null) {
            return -1;
        }
        try {
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            int providerKey = nextProviderKey.getAndIncrement();
            weightsByProviderKey.put(providerKey, weight);
            LOGGER.debug("[scf] createProvider annotationId={} → providerKey={}", annotationId, providerKey);
            return providerKey;
        } catch (IOException exception) {
            LOGGER.error("createProvider failed for annotationId=" + annotationId, exception);
            return -1;
        }
    }

    @Override
    public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        boolean isLiveDocsProvider = liveDocsProviderKeys.containsKey(providerKey);
        Weight weight = isLiveDocsProvider ? null : weightsByProviderKey.get(providerKey);
        if (!isLiveDocsProvider && weight == null) {
            return -1;
        }
        String segName = generationToSegmentName.get(writerGeneration);
        if (segName == null) {
            LOGGER.error(
                "createCollector: no Lucene segment for writer_generation={} (providerKey={}). Known generations: {}",
                writerGeneration,
                providerKey,
                generationToSegmentName.keySet()
            );
            return -1;
        }
        LeafReaderContext leaf = null;
        for (LeafReaderContext lrc : leaves) {
            if (unwrapSegmentReader(lrc.reader()).getSegmentInfo().info.name.equals(segName)) {
                leaf = lrc;
                break;
            }
        }
        if (leaf == null) {
            LOGGER.error(
                "createCollector: segment name [{}] not found in leaves (writerGeneration={}, providerKey={})",
                segName,
                writerGeneration,
                providerKey
            );
            return -1;
        }

        int leafMaxDoc = leaf.reader().maxDoc();
        assert minDoc >= 0 && minDoc <= maxDoc && maxDoc <= leafMaxDoc : "createCollector(providerKey="
            + providerKey
            + ", writerGeneration="
            + writerGeneration
            + " -> segment="
            + segName
            + "): partition ["
            + minDoc
            + ","
            + maxDoc
            + ") exceeds leaf maxDoc="
            + leafMaxDoc;

        try {
            org.apache.lucene.util.Bits liveDocs = leaf.reader().getLiveDocs();
            int collectorKey = nextCollectorKey.getAndIncrement();
            if (isLiveDocsProvider) {
                // Live-docs-only: no scorer needed, just the liveDocs bitset reference.
                scorersByCollectorKey.put(collectorKey, ScorerHandle.liveDocsOnly(liveDocs, minDoc, maxDoc));
            } else {
                Scorer scorer = weight.scorer(leaf);
                scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, liveDocs, minDoc, maxDoc));
            }
            LOGGER.debug(
                "[scf] createCollector providerKey={} writerGeneration={} range=[{},{}) → collectorKey={} liveDocsOnly={}",
                providerKey,
                writerGeneration,
                minDoc,
                maxDoc,
                collectorKey,
                isLiveDocsProvider
            );
            return collectorKey;
        } catch (IOException exception) {
            LOGGER.error(
                "createCollector failed for providerKey=" + providerKey + ", writerGeneration=" + writerGeneration + ", segment=" + segName,
                exception
            );
            return -1;
        }
    }

    @Override
    public boolean isCancelled() {
        return isCancelledSupplier != null && isCancelledSupplier.getAsBoolean();
    }

    @Override
    public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
        ScorerHandle handle = scorersByCollectorKey.get(collectorKey);
        if (handle == null) {
            return -1;
        }
        if (maxDoc <= minDoc) {
            return 0;
        }
        int span = maxDoc - minDoc;
        int wordCount = (span + 63) >>> 6;

        if (handle.liveDocsOnly) {
            // Fast path: directly copy liveDocs bits without Scorer overhead.
            fillLiveDocsBitset(handle.liveDocs, minDoc, span, wordCount, out);
            return wordCount;
        }

        FixedBitSet bits = new FixedBitSet(span);
        if (handle.scorer != null) {
            int scanFrom = Math.max(minDoc, handle.partitionMinDoc);
            int scanTo = Math.min(maxDoc, handle.partitionMaxDoc);

            if (scanFrom < scanTo) {
                try {
                    DocIdSetIterator iterator = handle.scorer.iterator();
                    org.apache.lucene.util.Bits liveDocs = handle.liveDocs;
                    int docId = handle.currentDoc;
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        if (docId < scanFrom) {
                            docId = iterator.advance(scanFrom);
                        }
                        while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                            if (liveDocs == null || liveDocs.get(docId)) {
                                bits.set(docId - minDoc);
                            }
                            docId = iterator.nextDoc();
                        }
                        handle.currentDoc = docId;
                    }
                } catch (IOException exception) {
                    LOGGER.warn("IOException during collectDocs, returning partial bitset", exception);
                }
            }
        }

        long[] words = bits.getBits();
        MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, wordCount);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "[scf] collectDocs collectorKey={} range=[{},{}) → cardinality={} words={}",
                collectorKey,
                minDoc,
                maxDoc,
                bits.cardinality(),
                wordCount
            );
        }
        return wordCount;
    }

    /**
     * Directly fills the output buffer with the liveDocs bitset for [minDoc, minDoc+span).
     * No Scorer, no iterator — just a tight bit-copy loop. When liveDocs is null (no deletions),
     * fills all-ones.
     */
    private static void fillLiveDocsBitset(
        org.apache.lucene.util.Bits liveDocs,
        int minDoc,
        int span,
        int wordCount,
        MemorySegment out
    ) {
        if (liveDocs == null) {
            // No deletions: all docs alive — fill all-ones, then clear trailing bits.
            for (int i = 0; i < wordCount; i++) {
                out.setAtIndex(ValueLayout.JAVA_LONG, i, -1L);
            }
            int trailing = span % 64;
            if (trailing != 0) {
                long mask = (1L << trailing) - 1;
                out.setAtIndex(ValueLayout.JAVA_LONG, wordCount - 1, mask);
            }
        } else {
            // Has deletions: build bitset from liveDocs.get(docId) for each doc in range.
            long word = 0;
            int wordIdx = 0;
            for (int i = 0; i < span; i++) {
                if (liveDocs.get(minDoc + i)) {
                    word |= (1L << (i & 63));
                }
                if ((i & 63) == 63) {
                    out.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word);
                    word = 0;
                    wordIdx++;
                }
            }
            if ((span & 63) != 0) {
                out.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word);
            }
        }
    }

    @Override
    public void releaseCollector(int collectorKey) {
        scorersByCollectorKey.remove(collectorKey);
    }

    @Override
    public void releaseProvider(int providerKey) {
        weightsByProviderKey.remove(providerKey);
    }

    @Override
    public void close() {
        weightsByProviderKey.clear();
        scorersByCollectorKey.clear();
    }

    private SegmentReader unwrapSegmentReader(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader flr) {
            current = flr.getDelegate();
        }
        return (SegmentReader) current;
    }

    private static final class ScorerHandle {
        final Scorer scorer;
        final org.apache.lucene.util.Bits liveDocs;
        final boolean liveDocsOnly;
        final int partitionMinDoc;
        final int partitionMaxDoc;
        int currentDoc = -1;

        ScorerHandle(Scorer scorer, org.apache.lucene.util.Bits liveDocs, int partitionMinDoc, int partitionMaxDoc) {
            this(scorer, liveDocs, false, partitionMinDoc, partitionMaxDoc);
        }

        private ScorerHandle(
            Scorer scorer,
            org.apache.lucene.util.Bits liveDocs,
            boolean liveDocsOnly,
            int partitionMinDoc,
            int partitionMaxDoc
        ) {
            this.scorer = scorer;
            this.liveDocs = liveDocs;
            this.liveDocsOnly = liveDocsOnly;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
        }

        static ScorerHandle liveDocsOnly(org.apache.lucene.util.Bits liveDocs, int partitionMinDoc, int partitionMaxDoc) {
            return new ScorerHandle(null, liveDocs, true, partitionMinDoc, partitionMaxDoc);
        }
    }
}
