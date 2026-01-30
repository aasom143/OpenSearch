/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.service;

import org.apache.iceberg.Table;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.plugin.iceberg.catalog.S3TablesIcebergManager;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Service for syncing Parquet files from OpenSearch remote store to Iceberg catalogs.
 * Reads remote store metadata to discover active files and syncs to Iceberg.
 */
public class IcebergService {
    
    private static final Logger logger = LogManager.getLogger(IcebergService.class);
    
    public static final Setting<String> CATALOG_TYPE_SETTING = Setting.simpleString(
        "iceberg.catalog.type",
        "s3tables",
        Setting.Property.NodeScope
    );
    
    public static final Setting<String> S3_TABLES_BUCKET_ARN_SETTING = Setting.simpleString(
        "iceberg.s3tables.bucket.arn",
        "",
        Setting.Property.NodeScope
    );
    
    public static final Setting<String> AWS_REGION_SETTING = Setting.simpleString(
        "iceberg.aws.region",
        "us-west-2",
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final ThreadPool threadPool;
    private final Settings settings;

    public IcebergService(
        ClusterService clusterService,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        Settings settings
    ) {
        this.clusterService = clusterService;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.settings = settings;
    }

    /**
     * Sync an index to Iceberg catalog.
     * Automatically discovers active files from remote store metadata.
     * 
     * @param indexName Name of the index to sync
     * @return Map with sync statistics
     * @throws IOException if reading metadata fails
     */
    public Map<String, Object> syncIndex(String indexName) throws IOException {
        logger.info("[Iceberg Plugin] Starting sync for index: {}", indexName);
        
        // 1. Get index metadata from cluster state
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        
        String indexUUID = indexMetadata.getIndexUUID();
        logger.info("[Iceberg Plugin] Index UUID: {}", indexUUID);
        
        // 2. Read active files from shard 0 (for compatibility with old API)
        Map<String, UploadedSegmentMetadata> activeFiles = readShardFiles(indexName, indexUUID, 0, indexMetadata);
        logger.info("[Iceberg Plugin] Found {} active files in remote store metadata", activeFiles.size());
        
        // 3. Infer schema from index mappings
        org.apache.iceberg.Schema icebergSchema = null;
        try {
            icebergSchema = inferSchemaFromIndex(indexMetadata);
            logger.info("[Iceberg Plugin] Inferred schema with {} fields from index mappings", 
                       icebergSchema.columns().size());
        } catch (Exception e) {
            logger.warn("[Iceberg Plugin] Failed to infer schema from index mappings: {}", e.getMessage());
        }
        
        // 4. Get Iceberg table (will be created if doesn't exist and schema available)
        Table table = S3TablesIcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
        if (table == null) {
            throw new IllegalStateException("Failed to get or create Iceberg table for index: " + indexName);
        }
        
        // 4. Get current files in Iceberg catalog
        Set<String> catalogFiles = new HashSet<>();
        if (table.currentSnapshot() != null) {
            try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks = 
                    table.newScan().planFiles()) {
                for (org.apache.iceberg.FileScanTask task : tasks) {
                    catalogFiles.add(task.file().path().toString());
                }
            }
        }
        logger.info("[Iceberg Plugin] Found {} files in Iceberg catalog", catalogFiles.size());
        
        // 5. Compute diff
        Map<String, UploadedSegmentMetadata> filesToAdd = new HashMap<>();
        for (Map.Entry<String, UploadedSegmentMetadata> entry : activeFiles.entrySet()) {
            if (!catalogFiles.contains(entry.getKey())) {
                filesToAdd.put(entry.getKey(), entry.getValue());
            }
        }
        
        Set<String> filesToRemove = new HashSet<>(catalogFiles);
        filesToRemove.removeAll(activeFiles.keySet());  // Files in catalog but not in remote store
        
        int filesKept = activeFiles.size() - filesToAdd.size();
        
        logger.info("[Iceberg Plugin] Sync plan: add={}, remove={}, keep={}", 
                   filesToAdd.size(), filesToRemove.size(), filesKept);
        
        // 6. Apply changes
        if (!filesToAdd.isEmpty()) {
            addFilesToCatalog(table, filesToAdd);
        }
        
        if (!filesToRemove.isEmpty()) {
            removeFilesFromCatalog(table, filesToRemove);
        }
        
        // 7. Return results
        Map<String, Object> result = new HashMap<>();
        result.put("files_added", filesToAdd.size());
        result.put("files_removed", filesToRemove.size());
        result.put("files_kept", filesKept);
        result.put("snapshot_id", table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null);
        
        logger.info("[Iceberg Plugin] Sync completed for index: {}", indexName);
        return result;
    }

    /**
     * Sync a specific shard to Iceberg catalog.
     * Called by TransportShardSyncIcebergAction for each shard.
     */
    public Map<String, Object> syncShard(ShardId shardId) throws IOException {
        String indexName = shardId.getIndexName();
        String indexUUID = shardId.getIndex().getUUID();
        int shardNum = shardId.id();
        
        logger.info("[Iceberg Shard Sync] Syncing shard: {}", shardId);
        
        // Get index metadata
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        
        // Read active files for THIS shard only
        Map<String, UploadedSegmentMetadata> activeFiles = readShardFiles(indexName, indexUUID, shardNum, indexMetadata);
        logger.info("[Iceberg Shard Sync] Shard {}: found {} files", shardId, activeFiles.size());
        
        // Infer schema from index mappings (only once, cached in table)
        org.apache.iceberg.Schema icebergSchema = null;
        try {
            icebergSchema = inferSchemaFromIndex(indexMetadata);
        } catch (Exception e) {
            logger.warn("[Iceberg Shard Sync] Failed to infer schema: {}", e.getMessage());
        }
        
        // Get or create table
        org.apache.iceberg.Table table = S3TablesIcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
        if (table == null) {
            throw new IllegalStateException("Failed to get Iceberg table for: " + indexName);
        }
        
        // Get current files in catalog for comparison
        Set<String> catalogFiles = new HashSet<>();
        if (table.currentSnapshot() != null) {
            try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks = 
                    table.newScan().planFiles()) {
                for (org.apache.iceberg.FileScanTask task : tasks) {
                    catalogFiles.add(task.file().path().toString());
                }
            }
        }
        
        // Compute diff for this shard
        Map<String, UploadedSegmentMetadata> filesToAdd = new HashMap<>();
        for (Map.Entry<String, UploadedSegmentMetadata> entry : activeFiles.entrySet()) {
            if (!catalogFiles.contains(entry.getKey())) {
                filesToAdd.put(entry.getKey(), entry.getValue());
            }
        }
        
        Set<String> filesToRemove = new HashSet<>(catalogFiles);
        filesToRemove.removeAll(activeFiles.keySet());
        
        int filesKept = activeFiles.size() - filesToAdd.size();
        
        logger.info("[Iceberg Shard Sync] Shard {}: add={}, remove={}, keep={}", 
                   shardId, filesToAdd.size(), filesToRemove.size(), filesKept);
        
        // Apply changes
        if (!filesToAdd.isEmpty()) {
            addFilesToCatalog(table, filesToAdd);
        }
        
        if (!filesToRemove.isEmpty()) {
            removeFilesFromCatalog(table, filesToRemove);
        }
        
        // Return results
        Map<String, Object> result = new HashMap<>();
        result.put("files_added", filesToAdd.size());
        result.put("files_removed", filesToRemove.size());
        result.put("files_kept", filesKept);
        
        return result;
    }
    
    /**
     * Read active files for a specific shard.
     */
    private Map<String, UploadedSegmentMetadata> readShardFiles(
        String indexName,
        String indexUUID,
        int shardNum,
        IndexMetadata indexMetadata
    ) throws IOException {
        Map<String, UploadedSegmentMetadata> activeFiles = new HashMap<>();
        
        try {
            // Create RemoteSegmentStoreDirectory to read metadata
            String remoteStoreRepo = "test-rs-repo";  // From node attributes
            
            RemoteSegmentStoreDirectoryFactory factory = new RemoteSegmentStoreDirectoryFactory(
                repositoriesServiceSupplier,
                threadPool,
                remoteStoreRepo
            );
            
            // Create ShardId for the specific shard
            ShardId shardId = new ShardId(indexName, indexUUID, shardNum);
            
            // Get path strategy from index metadata
            org.opensearch.index.remote.RemoteStorePathStrategy pathStrategy = 
                org.opensearch.index.remote.RemoteStoreUtils.determineRemoteStorePathStrategy(indexMetadata);
            
            RemoteSegmentStoreDirectory remoteDir = (RemoteSegmentStoreDirectory) factory.newDirectory(
                remoteStoreRepo,
                indexUUID,
                shardId,
                pathStrategy
            );
            
            // Get repository configuration for bucket and base path
            org.opensearch.repositories.Repository repository = repositoriesServiceSupplier.get().repository(remoteStoreRepo);
            org.opensearch.repositories.blobstore.BlobStoreRepository blobRepo = 
                (org.opensearch.repositories.blobstore.BlobStoreRepository) repository;
            
            // Get bucket and base path from repository metadata
            org.opensearch.cluster.metadata.RepositoryMetadata repoMetadata = blobRepo.getMetadata();
            Settings repoSettings = repoMetadata.settings();
            
            // Extract bucket name from repository settings
            String bucketName = repoSettings.get("bucket");
            String basePath = repoSettings.get("base_path", "");
            
            logger.info("[Iceberg Plugin] Repository: bucket={}, basePath={}", bucketName, basePath);
            
            // Read latest metadata
            RemoteSegmentMetadata metadata = remoteDir.readLatestMetadataFile();
            
            if (metadata != null) {
                // Extract active file paths from metadata
                String finalBucket = bucketName;
                String finalBase = basePath;
                int finalShardId = shardId.id();
                
                metadata.getMetadata().forEach((fileNameKey, uploadedMetadata) -> {
                    // Check if it's a parquet file
                    if (fileNameKey.contains("parquet")) {
                        // Construct full S3 URI from repository config + metadata
                        String s3Path = String.format("s3://%s/%s/%s/%d/segments/data/parquet/%s",
                            finalBucket,
                            finalBase,
                            indexUUID,
                            finalShardId,
                            uploadedMetadata.getUploadedFilename());
                        activeFiles.put(s3Path, uploadedMetadata);
                        logger.debug("[Iceberg Plugin] Discovered file: {}", s3Path);
                    }
                });
            }
            
            logger.info("[Iceberg Plugin] Read {} active files from metadata", activeFiles.size());
            
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Failed to read metadata for index {}: {}", indexName, e.getMessage(), e);
            throw new IOException("Failed to read remote store metadata", e);
        }
        
        return activeFiles;
    }

    /**
     * Infer Iceberg schema from OpenSearch index mappings.
     * Adds hidden partition fields: index_uuid and shard_id.
     */
    private org.apache.iceberg.Schema inferSchemaFromIndex(IndexMetadata indexMetadata) throws IOException {
        try {
            org.opensearch.cluster.metadata.MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                throw new IOException("No mapping found for index");
            }
            
            Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMap = (Map<String, Object>) mappingSource.get("properties");
            
            if (propertiesMap == null) {
                throw new IOException("No 'properties' field found in mapping");
            }
            
            Map<String, Object> properties = propertiesMap;
            
            if (properties == null || properties.isEmpty()) {
                throw new IOException("No properties found in index mapping");
            }
            
            List<org.apache.iceberg.types.Types.NestedField> fields = new java.util.ArrayList<>();
            int fieldId = 1;
            
            // Add hidden partition fields FIRST (for efficient queries)
            fields.add(org.apache.iceberg.types.Types.NestedField.optional(
                fieldId++, "index_uuid", org.apache.iceberg.types.Types.StringType.get()
            ));
            fields.add(org.apache.iceberg.types.Types.NestedField.optional(
                fieldId++, "shard_id", org.apache.iceberg.types.Types.IntegerType.get()
            ));
            
            // Add data fields from index mappings
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String fieldName = entry.getKey();
                @SuppressWarnings("unchecked")
                Map<String, Object> fieldDef = (Map<String, Object>) entry.getValue();
                String type = (String) fieldDef.get("type");
                
                // Convert OpenSearch type to Iceberg type
                org.apache.iceberg.types.Type icebergType = convertOpenSearchTypeToIceberg(type);
                fields.add(org.apache.iceberg.types.Types.NestedField.optional(
                    fieldId++, fieldName, icebergType
                ));
            }
            
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(fields);
            logger.info("[Iceberg Plugin] Created schema with {} total fields (2 partition + {} data fields)", 
                       fields.size(), properties.size());
            
            return schema;
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Failed to infer schema from index mappings: {}", e.getMessage());
            throw new IOException("Failed to convert index mappings to Iceberg schema", e);
        }
    }
    
    /**
     * Convert OpenSearch field type to Iceberg type.
     */
    private org.apache.iceberg.types.Type convertOpenSearchTypeToIceberg(String opensearchType) {
        if (opensearchType == null) {
            return org.apache.iceberg.types.Types.StringType.get();
        }
        
        return switch(opensearchType) {
            case "text", "keyword" -> org.apache.iceberg.types.Types.StringType.get();
            case "long" -> org.apache.iceberg.types.Types.LongType.get();
            case "integer" -> org.apache.iceberg.types.Types.IntegerType.get();
            case "short" -> org.apache.iceberg.types.Types.IntegerType.get();
            case "byte" -> org.apache.iceberg.types.Types.IntegerType.get();
            case "double" -> org.apache.iceberg.types.Types.DoubleType.get();
            case "float" -> org.apache.iceberg.types.Types.FloatType.get();
            case "boolean" -> org.apache.iceberg.types.Types.BooleanType.get();
            case "date" -> org.apache.iceberg.types.Types.TimestampType.withoutZone();  // Parquet stores as timestamp millis
            case "timestamp" -> org.apache.iceberg.types.Types.TimestampType.withoutZone();
            default -> org.apache.iceberg.types.Types.StringType.get();
        };
    }
    
    /**
     * Add new files to Iceberg catalog with actual file size and row count.
     * Copies files to S3 Tables warehouse location, preserving original path structure.
     */
    private void addFilesToCatalog(Table table, Map<String, UploadedSegmentMetadata> filesToAdd) {
        // Set thread context classloader for Iceberg's dynamic class loading
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());
        
        try {
            // Get S3 Tables warehouse location from table
            String warehouseLocation = table.location();
            logger.info("[Iceberg Plugin] Table warehouse location: {}", warehouseLocation);
            
            // Get S3FileIO for copy operations
            org.apache.iceberg.aws.s3.S3FileIO s3FileIO = S3TablesIcebergManager.getInstance().getFileIO();
            
            AppendFiles append = table.newAppend();
            
            for (Map.Entry<String, UploadedSegmentMetadata> entry : filesToAdd.entrySet()) {
                String sourceS3Path = entry.getKey();
                UploadedSegmentMetadata metadata = entry.getValue();
                
                // Extract partition values from source path
                // Format: s3://bucket/base/indexUUID/shardId/segments/data/parquet/filename
                PartitionValues partitionValues = extractPartitionValues(sourceS3Path);
                
                // Extract relative path from source to preserve directory structure
                // Source format: s3://bucket/base_path/indexUUID/shardId/segments/data/parquet/filename
                // We want to preserve: indexUUID/shardId/segments/data/parquet/filename
                String relativePath = extractRelativePath(sourceS3Path);
                
                // Construct destination path in S3 Tables warehouse, preserving structure
                String destinationPath = warehouseLocation + "/data/" + relativePath;
                
                logger.info("[Iceberg Plugin] Copying file from {} to {} (index_uuid={}, shard_id={})", 
                           sourceS3Path, destinationPath, partitionValues.indexUuid, partitionValues.shardId);
                
                // Copy file to warehouse location using S3 CopyObject
                try {
                    copyS3File(s3FileIO, sourceS3Path, destinationPath);
                } catch (Exception e) {
                    logger.error("[Iceberg Plugin] Failed to copy file to warehouse: {}", e.getMessage(), e);
                    throw new RuntimeException("Failed to copy file to S3 Tables warehouse", e);
                }
                
                // Get actual file size from metadata
                long fileSize = metadata.getLength();
                // Estimate rows (actual rows not available in metadata)
                long recordCount = fileSize > 0 ? 100L : 1L;
                
                logger.info("[Iceberg Plugin] Adding file to catalog: path={}, size={} bytes, partitions=[{}, {}]", 
                           destinationPath, fileSize, partitionValues.indexUuid, partitionValues.shardId);
                
                // Create DataFile with warehouse path and partition values
                DataFiles.Builder builder = DataFiles.builder(table.spec())
                    .withPath(destinationPath)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount);
                
                // Add partition path if table is partitioned (Hive-style: field1=value1/field2=value2)
                if (table.spec().isPartitioned()) {
                    String partitionPath = String.format("index_uuid=%s/shard_id=%d", 
                                                        partitionValues.indexUuid, 
                                                        partitionValues.shardId);
                    builder.withPartitionPath(partitionPath);
                    logger.debug("[Iceberg Plugin] Set partition path: {}", partitionPath);
                }
                
                DataFile dataFile = builder.build();
                append.appendFile(dataFile);
            }
            
            append.commit();
            logger.info("[Iceberg Plugin] Added {} files to catalog with preserved path structure", filesToAdd.size());
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    /**
     * Extract relative path from full S3 path to preserve directory structure.
     * Source format: s3://bucket/base_path/indexUUID/shardId/segments/data/parquet/filename
     * Returns: indexUUID/shardId/segments/data/parquet/filename
     */
    private String extractRelativePath(String s3Path) {
        try {
            logger.info("[Iceberg Plugin] Extracting relative path from: {}", s3Path);
            
            // Remove s3:// prefix
            String pathWithoutProtocol = s3Path.substring(5); // Remove "s3://"
            
            // Split by '/' and find the indexUUID (UUID format)
            String[] parts = pathWithoutProtocol.split("/");
            logger.info("[Iceberg Plugin] Path parts: {}", String.join(" | ", parts));
            
            // Find the start of the UUID (36 character format with dashes)
            int uuidIndex = -1;
            for (int i = 0; i < parts.length; i++) {
                // UUID format: 8-4-4-4-12 characters (total 36 with dashes)
                // Check both length and pattern
                logger.debug("[Iceberg Plugin] Checking part[{}]: '{}' (length={})", i, parts[i], parts[i].length());
                
                if (parts[i].length() == 36 && parts[i].matches("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")) {
                    uuidIndex = i;
                    logger.info("[Iceberg Plugin] Found UUID at index {}: {}", i, parts[i]);
                    break;
                }
            }
            
            if (uuidIndex == -1) {
                logger.warn("[Iceberg Plugin] Could not find UUID in path: {}", s3Path);
                logger.warn("[Iceberg Plugin] Falling back to full path after bucket");
                
                // Fallback: preserve everything after bucket name (first part)
                StringBuilder fallbackPath = new StringBuilder();
                for (int i = 1; i < parts.length; i++) {  // Skip bucket name (index 0)
                    if (i > 1) {
                        fallbackPath.append('/');
                    }
                    fallbackPath.append(parts[i]);
                }
                String result = fallbackPath.toString();
                logger.info("[Iceberg Plugin] Using fallback path: {}", result);
                return result;
            }
            
            // Reconstruct path from UUID onwards
            StringBuilder relativePath = new StringBuilder();
            for (int i = uuidIndex; i < parts.length; i++) {
                if (i > uuidIndex) {
                    relativePath.append('/');
                }
                relativePath.append(parts[i]);
            }
            
            String result = relativePath.toString();
            logger.info("[Iceberg Plugin] Extracted relative path: {}", result);
            return result;
            
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Failed to extract relative path from {}: {}", 
                        s3Path, e.getMessage(), e);
            // Fallback to just filename
            String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
            logger.warn("[Iceberg Plugin] Using filename only: {}", filename);
            return filename;
        }
    }
    
    /**
     * Copy file from source to destination using S3 CopyObject.
     */
    private void copyS3File(org.apache.iceberg.aws.s3.S3FileIO fileIO, String sourcePath, String destPath) throws IOException {
        try {
            // Read from source
            org.apache.iceberg.io.InputFile sourceFile = fileIO.newInputFile(sourcePath);
            byte[] data;
            try (java.io.InputStream in = sourceFile.newStream()) {
                data = in.readAllBytes();
            }
            
            // Write to destination
            org.apache.iceberg.io.OutputFile destFile = fileIO.newOutputFile(destPath);
            try (java.io.OutputStream out = destFile.create()) {
                out.write(data);
            }
            
            logger.info("[Iceberg Plugin] Successfully copied file: {} bytes", data.length);
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] File copy failed: {}", e.getMessage());
            throw new IOException("S3 copy operation failed", e);
        }
    }

    /**
     * Extract partition values (index_uuid, shard_id) from S3 path.
     * Format: s3://bucket/base_path/indexUUID/shardId/segments/data/parquet/filename
     * Uses position-based detection: finds "segments" anchor, then extracts UUID and shard_id before it.
     */
    private PartitionValues extractPartitionValues(String s3Path) {
        try {
            logger.info("[Iceberg Plugin] Extracting partition values from: {}", s3Path);
            
            String pathWithoutProtocol = s3Path.substring(5); // Remove "s3://"
            String[] parts = pathWithoutProtocol.split("/");
            
            logger.info("[Iceberg Plugin] Partition extraction - path parts: {}", String.join(" | ", parts));
            
            // Find "segments" anchor - it's always present in the path
            int segmentsIndex = -1;
            for (int i = 0; i < parts.length; i++) {
                if ("segments".equals(parts[i])) {
                    segmentsIndex = i;
                    logger.info("[Iceberg Plugin] Found 'segments' at index {}", i);
                    break;
                }
            }
            
            if (segmentsIndex < 2) {
                logger.warn("[Iceberg Plugin] Could not find 'segments' anchor or not enough parts before it: {}", s3Path);
                return new PartitionValues("unknown", 0);
            }
            
            // UUID is 2 parts before "segments": bucket/base_path/UUID/shard_id/segments/...
            String indexUuid = parts[segmentsIndex - 2];
            logger.info("[Iceberg Plugin] Extracted index_uuid from part[{}]: {}", segmentsIndex - 2, indexUuid);
            
            // Shard ID is 1 part before "segments"
            String shardIdPart = parts[segmentsIndex - 1];
            logger.info("[Iceberg Plugin] Attempting to parse shard_id from part[{}]: '{}'", segmentsIndex - 1, shardIdPart);
            
            Integer shardId = null;
            try {
                shardId = Integer.parseInt(shardIdPart);
                logger.info("[Iceberg Plugin] Successfully parsed shard_id: {}", shardId);
            } catch (NumberFormatException e) {
                logger.warn("[Iceberg Plugin] Failed to parse shard_id from '{}': {}", shardIdPart, e.getMessage());
                return new PartitionValues(indexUuid, 0);
            }
            
            logger.info("[Iceberg Plugin] Successfully extracted partition values: index_uuid={}, shard_id={}", 
                       indexUuid, shardId);
            return new PartitionValues(indexUuid, shardId);
            
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Failed to extract partition values from {}: {}", 
                        s3Path, e.getMessage(), e);
            return new PartitionValues("unknown", 0);
        }
    }
    
    /**
     * Remove stale files from Iceberg catalog.
     */
    private void removeFilesFromCatalog(Table table, Set<String> filesToRemove) {
        // Set thread context classloader for Iceberg's dynamic class loading
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());
        
        try {
            DeleteFiles delete = table.newDelete();
            
            for (String filePath : filesToRemove) {
                delete.deleteFile(filePath);
            }
            
            delete.commit();
            logger.info("[Iceberg Plugin] Removed {} stale files from catalog", filesToRemove.size());
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    /**
     * Helper class to hold partition values extracted from file paths.
     */
    private static class PartitionValues {
        final String indexUuid;
        final int shardId;
        
        PartitionValues(String indexUuid, int shardId) {
            this.indexUuid = indexUuid;
            this.shardId = shardId;
        }
    }
}
