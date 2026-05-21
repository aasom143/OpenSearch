/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.List;

/**
 * Static registry of QueryBuilder NamedWriteable entries for deserialization
 * during predicate combining at the coordinator.
 */
final class QueryBuilderRegistry {

    private static final NamedWriteableRegistry INSTANCE = new NamedWriteableRegistry(List.of(
        new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchQueryBuilder.NAME, MatchQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhraseQueryBuilder.NAME, MatchPhraseQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchBoolPrefixQueryBuilder.NAME, MatchBoolPrefixQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhrasePrefixQueryBuilder.NAME, MatchPhrasePrefixQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, MultiMatchQueryBuilder.NAME, MultiMatchQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, QueryStringQueryBuilder.NAME, QueryStringQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, SimpleQueryStringBuilder.NAME, SimpleQueryStringBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new),
        new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
    ));

    private QueryBuilderRegistry() {}

    static NamedWriteableRegistry get() {
        return INSTANCE;
    }
}
