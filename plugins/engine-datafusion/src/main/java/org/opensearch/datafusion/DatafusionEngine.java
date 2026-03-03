/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.AsyncRecordBatchIterator;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.DatafusionSearcherSupplier;
import org.opensearch.datafusion.search.RecordBatchIterator;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.FileDeletionListener;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FileStats;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.DerivedFieldGenerator;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.lookup.SourceLookup;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DataFusionService datafusionService;
    private CacheManager cacheManager;
    private final RootAllocator rootAllocator;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService, ShardPath shardPath) throws IOException {
        logger.info("[FLOW] DatafusionEngine constructor: format={}, shardPath={}, fileCount={}",
            dataFormat.getName(), shardPath.getShardId(), formatCatalogSnapshot.size());
        this.dataFormat = dataFormat;
        this.datafusionReaderManager = new DatafusionReaderManager(
            shardPath.getDataPath().resolve(dataFormat.getName()).toString(), formatCatalogSnapshot, dataFormat.getName()
        );
        this.datafusionService = dataFusionService;
        this.cacheManager = datafusionService.getCacheManager();
        this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
        if (this.cacheManager != null) {
            datafusionReaderManager.setOnFilesAdded(files -> {
                // Handle new files added during refresh
                logger.info("[FLOW] Files added to cache: count={}", files.size());
                cacheManager.addFilesToCacheManager(files);
            });
        }
        logger.info("[FLOW] DatafusionEngine initialized successfully");
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays, SearchContext originalContext, ClusterService clusterService) throws IOException {
        logger.info("[TRACE] DatafusionEngine.createContext() ENTRY");
        logger.info("[TRACE] Parameters: readerContext={}, request={}, searchShardTarget={}, task={}, originalContext={}", 
            readerContext != null ? "present" : "null",
            request != null ? "present" : "null", 
            searchShardTarget != null ? "present" : "null",
            task != null ? "present" : "null",
            originalContext != null ? "present" : "null");
            
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, searchShardTarget, task, this, bigArrays, originalContext, clusterService);
        
        String indexName = request.shardId().getIndexName();
        DatafusionQuery query = new DatafusionQuery(indexName, request.source().queryPlanIR(), new ArrayList<>());
        
        int shardId = request.shardId().id();
        String shardIdStr = String.valueOf(shardId);
        String icebergTableName = indexName;
        
        logger.info("[TRACE] ShardSearchRequest details:");
        logger.info("[TRACE] - shardId: {}", request.shardId());
        logger.info("[TRACE] - source: {}", request.source() != null ? "present" : "null");
        if (request.source() != null) {
            SearchSourceBuilder source = request.source();
            logger.info("[TRACE] - SearchSourceBuilder.toString(): {}", source.toString());
            logger.info("[TRACE] - queryPlanIR: {} bytes", source.queryPlanIR() != null ? source.queryPlanIR().length : "null");
        }
        
        String localDir = "/tmp/s3_partition_" + indexName + "_" + shardIdStr;
        String tableBucketArn = "arn:aws:s3tables:us-west-2:339712837375:bucket/srirasac-test";
        String databaseName = "opensearch";
        String partitionColumn = "shard_id";
        
        // Get cross-account parameters from cluster settings
        String roleArn = clusterService.getClusterSettings().get(
            Setting.simpleString("datafusion.s3.role_arn", Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
        String s3Bucket = clusterService.getClusterSettings().get(
            Setting.simpleString("datafusion.s3.bucket", Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
        String region = clusterService.getClusterSettings().get(
            Setting.simpleString("datafusion.s3.region", "us-east-1", Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
        
        // Apply fallback defaults if not set
        if (roleArn == null || roleArn.isEmpty()) {
            roleArn = "arn:aws:iam::691585341994:role/opensearch-snapshot-role";
            logger.info("[FLOW] Using fallback role_arn: {}", roleArn);
        }
        if (s3Bucket == null || s3Bucket.isEmpty()) {
            s3Bucket = "customer-s3tables-bucket";
            logger.info("[FLOW] Using fallback s3_bucket: {}", s3Bucket);
        }
        if (region == null || region.isEmpty()) {
            region = "us-east-1";
            logger.info("[FLOW] Using fallback region: {}", region);
        }
        
        logger.info("[FLOW] Retrieved from cluster settings: roleArn={}, s3Bucket={}, region={}", 
            roleArn, s3Bucket, region);
        
        // Debug: Log all available request information
        Map<String, String> s3Options = null;
        logger.info("[FLOW] Checking cross-account condition: roleArn={}, s3Bucket={}, region={}", 
            roleArn != null ? "present" : "null", 
            s3Bucket != null ? "present" : "null", 
            region != null ? "present" : "null");
            
        if (roleArn != null && s3Bucket != null && region != null) {
            logger.info("[FLOW] Cross-account query detected: role={}, bucket={}, region={}", roleArn, s3Bucket, region);
            // For now, pass the role ARN to Rust - Rust will assume the role
            s3Options = new HashMap<>();
            s3Options.put("role_arn", roleArn);
            s3Options.put("region", region);
            tableBucketArn = String.format(java.util.Locale.ROOT, "arn:aws:s3tables:%s:%s:bucket/%s", 
                region, extractAccountId(roleArn), s3Bucket);
        } else {
            logger.info("[FLOW] Cross-account condition not met, using default s3Options (null)");
        }
        
        query.configureDownloadedPartition(
            localDir,
            tableBucketArn,
            databaseName,
            partitionColumn,
            shardIdStr,
            s3Options
        );
        
        logger.info("[FLOW] Configured downloaded partition: localDir={}, partition={}={}, table={}",
            localDir, partitionColumn, shardIdStr, icebergTableName);
        
        datafusionContext.datafusionQuery(query);
        
        return datafusionContext;
    }
    
    private String extractAccountId(String roleArn) {
        String[] parts = roleArn.split(":");
        return parts.length > 4 ? parts[4] : "";
    }
    
    /**
     * Simple JSON value extractor for parameter parsing
     */
    private String extractJsonValue(String json, String key) {
        try {
            String searchKey = "\"" + key + "\"";
            int keyIndex = json.indexOf(searchKey);
            if (keyIndex == -1) return null;
            
            int colonIndex = json.indexOf(":", keyIndex);
            if (colonIndex == -1) return null;
            
            int startQuote = json.indexOf("\"", colonIndex);
            if (startQuote == -1) return null;
            
            int endQuote = json.indexOf("\"", startQuote + 1);
            if (endQuote == -1) return null;
            
            return json.substring(startQuote + 1, endQuote);
        } catch (Exception e) {
            logger.debug("Failed to extract JSON value for key {}: {}", key, e.getMessage());
            return null;
        }
    }
    
    private ThreadLocal<?>[] getThreadLocals() {
        try {
            // This is a simplified approach - in practice, thread-locals are harder to access
            return new ThreadLocal<?>[0];
        } catch (Exception e) {
            return null;
        }
    }
    


    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        return acquireSearcherSupplier(wrapper, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        // TODO : wrapper is ignored
        EngineSearcherSupplier<DatafusionSearcher> searcher = null;
        // TODO : refcount needs to be revisited - add proper tests for exception etc
        try {
            DatafusionReader reader = datafusionReaderManager.acquire();
            searcher = new DatafusionSearcherSupplier(null) {
                @Override
                protected DatafusionSearcher acquireSearcherInternal(String source) {
                    return new DatafusionSearcher(source, reader,
                         () -> {});

                }

                @Override
                protected void doClose() {
                    try {
                        datafusionReaderManager.release(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        } catch (Exception ex) {
            logger.error("Failed to acquire searcher", ex);
            throw new RuntimeException(ex);
        }
        return searcher;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return acquireSearcher(source, scope, Function.identity());
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        DatafusionSearcherSupplier releasable = null;
        try {
            DatafusionSearcherSupplier searcherSupplier = releasable = (DatafusionSearcherSupplier) acquireSearcherSupplier(wrapper, scope);
            DatafusionSearcher searcher = searcherSupplier.acquireSearcher(source);
            releasable = null;

            return new DatafusionSearcher(
                source,
                searcher.getReader(),
                () -> Releasables.close(searcher, searcherSupplier)
            );
        } finally {
            Releasables.close(releasable);
        }
    }

    @Override
    public DatafusionReaderManager getReferenceManager(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public CatalogSnapshotAwareRefreshListener getRefreshListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public FileDeletionListener getFileDeletionListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }

    @Override
    public void close() {
        rootAllocator.close();
    }


    @Override
    public void executeQueryPhase(DatafusionContext context) {
        Map<String, Object[]> finalRes = new HashMap<>();
        List<Long> rowIdResult = new ArrayList<>();
        RecordBatchStream stream = null;

        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
            stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer(), rootAllocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchIterator> collector = iterator -> {
                while (iterator.hasNext()) {
                    VectorSchemaRoot root = iterator.next();
                    for (Field field : root.getSchema().getFields()) {
                        String fieldName = field.getName();
                        FieldVector fieldVector = root.getVector(fieldName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                            FieldVector rowIdVector = root.getVector(fieldName);
                            for(int i=0; i<fieldVector.getValueCount(); i++) {
                                rowIdResult.add((long) rowIdVector.getObject(i));
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        else {
                            for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        finalRes.put(fieldName, fieldValues);
                    }
                }
            };

            collector.collect(new RecordBatchIterator(stream));

//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }


//            logger.info("Memory Pool Allocation Post Query ShardID:{}", context.getQueryShardContext().getShardId());
//            printMemoryPoolAllocation(datafusionService.getRuntimePointer());


//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw new RuntimeException(exception);
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        context.setDFResults(finalRes);
        context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(), TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(), Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
    }

    @Override
    public void executeQueryPhaseAsync(DatafusionContext context, Executor executor, ActionListener<Map<String, Object[]>> listener) {
        logger.info("[FLOW] executeQueryPhaseAsync started: shardId={}, indexName={}",
            context.indexShard().shardId(), context.request().shardId().getIndexName());
        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            context.getDatafusionQuery().setQueryPlanExplainEnabled(context.evaluateSearchQueryExplainMode());

            datafusionSearcher.searchAsync(context.getDatafusionQuery(), datafusionService.getRuntimePointer()).whenCompleteAsync((streamPointer, error)-> {
                Map<String, Object[]> finalRes = new HashMap<>();
                List<Long> rowIdResult = new ArrayList<>();
                if(streamPointer == null) {
                    logger.error("[FLOW] Query phase failed with error", error);
                    throw new RuntimeException(error);
                }
                logger.info("[FLOW] Query phase stream created, streamPointer={}", streamPointer);
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer() , allocator);
                SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                    @Override
                    public void collect(RecordBatchStream value) {
                        VectorSchemaRoot root = value.getVectorSchemaRoot();
                        for (Field field : root.getSchema().getFields()) {
                            String fieldName = field.getName();
                            FieldVector fieldVector = root.getVector(fieldName);
                            Object[] fieldValues = new Object[fieldVector.getValueCount()];
                            if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                                FieldVector rowIdVector = root.getVector(fieldName);
                                for(int i=0; i<fieldVector.getValueCount(); i++) {
                                    rowIdResult.add((long) rowIdVector.getObject(i));
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            else {
                                for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            finalRes.put(fieldName, fieldValues);
                        }
                    }
                };
                loadNextBatch(stream, executor, collector, finalRes, allocator, listener, context, rowIdResult);
                logger.info("Final Results:");
                for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
                    logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
                }
            });

//            logger.info("Memory Pool Allocation Post Query ShardID:{}", context.getQueryShardContext().getShardId());
//            printMemoryPoolAllocation(datafusionService.getRuntimePointer());


//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw new RuntimeException(exception);
        }
        //return finalRes;
    }

    private void loadNextBatch(
        RecordBatchStream stream,
        Executor executor,
        SearchResultsCollector<RecordBatchStream> collector,
        Map<String, Object[]> finalRes,
        RootAllocator allocator,
        ActionListener<Map<String, Object[]>> listener,
        DatafusionContext context,
        List<Long> rowIdResult
    ) {
        AsyncRecordBatchIterator iterator = new AsyncRecordBatchIterator(stream);
        iterator.nextAsync(ActionListener.wrap(hasMore -> {
            if (hasMore) {
                try {
                    collector.collect(stream);
                    // Recursively load next batch - TODO : anyway to Change this to iteration ?
                    loadNextBatch(stream, executor, collector, finalRes, allocator, listener, context, rowIdResult);
                } catch (Exception e) {
                    cleanup(stream, allocator);
                    listener.onFailure(e);
                }
            } else {
                cleanup(stream, allocator);
                context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(),
                    TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(),
                    Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
                logger.info("[FLOW] Query phase completed: totalHits={}, absoluteRowIds={}",
                    rowIdResult.size(),
                    rowIdResult.size() <= 20 ? rowIdResult : rowIdResult.subList(0, 20) + "...");
                listener.onResponse(finalRes);
            }
        }, error -> {
            cleanup(stream, allocator);
            listener.onFailure(new RuntimeException("Error loading batch", error));
        }));
    }
    private void cleanup(RecordBatchStream stream, RootAllocator allocator) {
        try {
            if (stream != null) stream.close();
            if (allocator != null) allocator.close();
        } catch (Exception e) {
            logger.error("Cleanup error", e);
        }
    }


    /**
     * Executes fetch phase, DataFusion query should contain projections for fields
     * @param context DataFusion context
     * @throws IOException
     */
    @Override
    public void executeFetchPhase(DatafusionContext context) throws IOException {

        List<Long> rowIds = Arrays.stream(context.docIdsToLoad()).mapToObj(Long::valueOf).toList();
        logger.info("[FLOW] executeFetchPhase started: shardId={}, rowIdCount={}, absoluteRowIds={}",
            context.indexShard().shardId(),
            rowIds.size(),
            rowIds.size() <= 20 ? rowIds : rowIds.subList(0, 20) + "...");
        if (rowIds.isEmpty()) {
            // no individual hits to process, so we shortcut
            logger.info("[FLOW] No row IDs to fetch, returning empty hits");
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
            return;
        }

        // preprocess
        context.getDatafusionQuery().setFetchPhaseContext(rowIds);

        List<String>  includeFields =
            Optional.ofNullable(context.request().source())
                .map(SearchSourceBuilder::fetchSource)
                .map(FetchSourceContext::includes)
                .map(list -> new ArrayList<>(Arrays.asList(list)))
                .orElseGet(ArrayList::new);

        List<String>  excludeFields =
            Optional.ofNullable(context.request().source())
                .map(SearchSourceBuilder::fetchSource)
                .map(FetchSourceContext::excludes)
                .map(list -> new ArrayList<>(Arrays.asList(list)))
                .orElseGet(ArrayList::new);

        if(!includeFields.isEmpty()) {
            includeFields.add(CompositeDataFormatWriter.ROW_ID);
        }
        excludeFields.addAll(context.mapperService().documentMapper().mapping().getMetadataStringNames());
        excludeFields.add(SeqNoFieldMapper.PRIMARY_TERM_NAME);

        context.getDatafusionQuery().setSource(includeFields, excludeFields);
        logger.info("[FLOW] Fetch phase: includeFields={}, excludeFields={}", includeFields, excludeFields);
        DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
        long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
        logger.info("[FLOW] Fetch phase stream created: streamPointer={}", streamPointer);
        RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer(), rootAllocator);

        Map<Long, Integer> rowIdToIndex = new HashMap<>();
        for (int idx = 0; idx < rowIds.size(); idx++) {
            rowIdToIndex.put(rowIds.get(idx), idx);
        }

        MapperService mapperService = context.mapperService();
        MappingLookup mappingLookup = mapperService.documentMapper().mappers();
        SearchResultsCollector<RecordBatchIterator> collector = iterator -> {
            List<BytesReference> byteRefs = new ArrayList<>();
            SearchHit[] hits = new SearchHit[rowIds.size()];
            int totalHits = 0;
            while (iterator.hasNext()) {
                VectorSchemaRoot vectorSchemaRoot = iterator.next();
                List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
                for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    String _id = "_id";
                    Long row_id = null;

                    try {
                        for (FieldVector valueVectors : fieldVectorList) {
                            if (valueVectors.getName().equals(CompositeDataFormatWriter.ROW_ID)) {
                                row_id = (long) valueVectors.getObject(i);
                                continue;
                            }
                            Mapper mapper = mappingLookup.getMapper(valueVectors.getName());
                            DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();

                            Object value = valueVectors.getObject(i);
                            if(value == null) {
                                builder.nullField(valueVectors.getName());
                            } else {
                                if(valueVectors instanceof ViewVarCharVector) {
                                    BytesRef bytesRef = new BytesRef(((ViewVarCharVector) valueVectors).get(i));
                                    derivedFieldGenerator.generate(builder, List.of(bytesRef));
                                } else if (valueVectors instanceof TimeStampMilliVector) {
                                    long timestamp = ((TimeStampMilliVector) valueVectors).get(i);
                                    derivedFieldGenerator.generate(builder, List.of(timestamp));
                                } else {
                                    derivedFieldGenerator.generate(builder, List.of(value));
                                }
                                if (valueVectors.getName().equals(IdFieldMapper.NAME)) {
                                    BytesRef idRef = new BytesArray((byte[]) value).toBytesRef();
                                    _id = Uid.decodeId(idRef.bytes, idRef.offset, idRef.length);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Failed to derive source for doc id [{i}]: {}", i, e);
                        throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
                    } finally {
                        builder.endObject();
                    }
                    assert row_id != null || rowIds.get(i) != null;
                    assert rowIdToIndex.containsKey(row_id);
                    assert _id != null;
                    BytesReference document = BytesReference.bytes(builder);
                    byteRefs.add(document);
                    SearchHit hit = new SearchHit(Math.toIntExact(rowIds.get(i)), _id, emptyMap(), emptyMap());
                    hit.sourceRef(document);
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(hit, null, Math.toIntExact(rowIds.get(i)), new SourceLookup()); //TODO: make source lookup one per thread
                    hitContext.sourceLookup().setSource(document);
                    int index = rowIdToIndex.get(row_id);
                    hits[index] = hit;
                    totalHits++;
                }
            }
            context.fetchResult().hits(new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), context.queryResult().getMaxScore()));
            logger.info("[FLOW] Fetch phase completed: totalHits={}", totalHits);
        };

        try {
            collector.collect(new RecordBatchIterator(stream));
        } catch (IOException exception) {
            logger.error("Failed to perform fetch phase", exception);
            throw new RuntimeException(exception);
        } finally {
            try {
                stream.close();
            } catch (Exception e) {
                logger.error("Failed to close stream", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Map<String, FileStats> fetchSegmentStats() throws IOException {
        try (DatafusionReader datafusionReader = datafusionReaderManager.acquire()) {
            return datafusionReader.fetchSegmentStats();
        }
    }
}
