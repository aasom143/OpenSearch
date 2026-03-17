/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatafusionQuery {
    private static final Logger logger = LogManager.getLogger(DatafusionQuery.class);
    
    private String indexName;
    private final byte[] substraitBytes;

    // List of Search executors which returns a result iterator which contains row id which can be joined in datafusion
    private final List<SearchExecutor> searchExecutors;
    private Boolean isFetchPhase;
    private List<Long> queryPhaseRowIds;
    private List<String> includeFields;
    private List<String> excludeFields;
    private Boolean isQueryPlanExplainEnabled;

    // S3 partition download configuration
    private Boolean useDownloadedPartition;
    private String localDownloadDir;
    private String tableBucketArn;
    private String databaseName;
    private String partitionColumn;
    private String partitionValue;
    private Map<String, String> s3Options;
    private String icebergTableName;

    public DatafusionQuery(String indexName, byte[] substraitBytes, List<SearchExecutor> searchExecutors) {
        this.indexName = indexName;
        this.substraitBytes = substraitBytes;
        this.searchExecutors = searchExecutors;
        this.isFetchPhase = false;
        this.isQueryPlanExplainEnabled = false;
        this.useDownloadedPartition = false;
    }

    public void setSource(List<String> includeFields, List<String> excludeFields) {
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
    }

    public void setFetchPhaseContext(List<Long> queryPhaseRowIds) {
        this.queryPhaseRowIds = queryPhaseRowIds;
        this.isFetchPhase = true;
    }

    public void setQueryPlanExplainEnabled(Boolean queryPlanExplainEnabled) {
        isQueryPlanExplainEnabled = queryPlanExplainEnabled;
    }

    public boolean getQueryPlanExplainEnabled() {
       return isQueryPlanExplainEnabled;
    }

    public boolean isFetchPhase() {
        return this.isFetchPhase;
    }

    public List<Long> getQueryPhaseRowIds() {
        return this.queryPhaseRowIds;
    }

    public List<String> getIncludeFields() {
        return this.includeFields;
    }

    public List<String> getExcludeFields() {
        return this.excludeFields;
    }

    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    public List<SearchExecutor> getSearchExecutors() {
        return searchExecutors;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIcebergTableName() {
        return icebergTableName != null ? icebergTableName : indexName;
    }

    // S3 partition download configuration methods

    public void configureDownloadedPartition(
        String localDownloadDir,
        String tableBucketArn,
        String databaseName,
        String partitionColumn,
        String partitionValue,
        Map<String, String> s3Options,
        String resolvedS3TableName
    ) {
        if (s3Options != null) {
            for (Map.Entry<String, String> entry : s3Options.entrySet()) {
            }
        }
        
        this.useDownloadedPartition = true;
        this.localDownloadDir = localDownloadDir;
        this.tableBucketArn = tableBucketArn;
        this.databaseName = databaseName;
        this.partitionColumn = partitionColumn;
        this.partitionValue = partitionValue;
        this.s3Options = s3Options;
        this.s3Options.put("s3_table_name", resolvedS3TableName.toLowerCase().replace("-", "_"));
        this.icebergTableName = this.indexName;
        
    }

    public boolean useDownloadedPartition() {
        return this.useDownloadedPartition != null && this.useDownloadedPartition;
    }

    public String getLocalDownloadDir() {
        return this.localDownloadDir;
    }

    public String getTableBucketArn() {
        return this.tableBucketArn;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getPartitionColumn() {
        return this.partitionColumn;
    }

    public String getPartitionValue() {
        return this.partitionValue;
    }

    public Map<String, String> getS3Options() {
        return this.s3Options;
    }
}
