/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

/**
 * Implemented by a per-format reader that can serve core DSL search, i.e. one that exposes a
 * Lucene view of its data. For a multi-format index exactly one format is expected to be
 * searchable (e.g. the Lucene secondary alongside a Parquet primary).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchableFormatReader {

    /**
     * Returns the Lucene reader backing DSL search for this format. Must be an
     * {@link OpenSearchDirectoryReader} so that {@code IndexShard} can wrap it, and must be the
     * same reader instance whose leaves callers search, so that {@code Weight}s stay bound to a
     * single top-level reader context.
     */
    OpenSearchDirectoryReader openSearchDirectoryReader();
}
