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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
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

    public static final Setting<String> S3TABLES_ROLE_ARN_SETTING = Setting.simpleString(
        "datafusion.iceberg.s3tables.role_arn",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> S3TABLES_BUCKET_SETTING = Setting.simpleString(
        "datafusion.iceberg.s3tables.bucket",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> S3TABLES_REGION_SETTING = Setting.simpleString(
        "datafusion.iceberg.s3tables.region",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> S3TABLES_NAMESPACE_SETTING = Setting.simpleString(
        "datafusion.iceberg.s3tables.namespace",
        "opensearch",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Full path to the Iceberg credentials file.
     * File should contain credentials in format:
     * aws_access_key_id=[access key]
     * aws_secret_access_key=[secret access key]
     * aws_session_token=[session token]
     */
    public static final Setting<String> CREDENTIALS_FILE_PATH_SETTING = Setting.simpleString(
        "iceberg.credentials.file",
        "/home/es2user/creds-iceberg/credentials.txt",
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final S3TablesIcebergManager s3TablesManager;

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
        
        // Create manager with settings (reads bucket ARN from configuration)
        this.s3TablesManager = new S3TablesIcebergManager(settings);
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
        Table table = s3TablesManager.getOrCreateTable(indexName, icebergSchema);
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
            org.apache.iceberg.aws.s3.S3FileIO sourceFileIO = createSourceFileIO(indexMetadata);
            try {
                addFilesToCatalog(table, filesToAdd, sourceFileIO);
            } finally {
                // Close source FileIO after use
                try {
                    sourceFileIO.close();
                } catch (Exception e) {
                    logger.warn("[Iceberg Plugin] Failed to close source FileIO: {}", e.getMessage());
                }
            }
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
        return syncShard(shardId, null, null, null);
    }

    /**
     * Sync a specific shard to Iceberg catalog in customer account using role assumption.
     */
    public Map<String, Object> syncShard(ShardId shardId, String roleArn, String s3Bucket, String region) throws IOException {
        // CRITICAL: Set classloader for ALL Iceberg operations to prevent DynMethods issues
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());
        
        try {
            String indexName = shardId.getIndexName();
            String indexUUID = shardId.getIndex().getUUID();
            int shardNum = shardId.id();

            logger.info("[Iceberg Shard Sync] Syncing shard: {} (role={}, bucket={}, region={})",
                       shardId, roleArn, s3Bucket, region);

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

            // Create source FileIO BEFORE role assumption to use service account credentials
            org.apache.iceberg.aws.s3.S3FileIO sourceFileIO = createSourceFileIO(indexMetadata);

            // Get or create table (with or without role assumption)
            org.apache.iceberg.Table table;
            if (roleArn != null && s3Bucket != null && region != null) {
                // Cross-account: create catalog directly with assumed role
                table = createTableWithAssumedRole(indexName, icebergSchema, roleArn, s3Bucket, region);
            } else {
                table = s3TablesManager.getOrCreateTable(indexName, icebergSchema);
            }

            if (table == null) {
                throw new IllegalStateException("Failed to get Iceberg table for: " + indexName);
            }

            // Get current files in catalog for comparison (classloader already set!)
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

            // Apply changes - pass pre-created source FileIO
            if (!filesToAdd.isEmpty()) {
                try {
                    addFilesToCatalog(table, filesToAdd, sourceFileIO);
                } finally {
                    // Close source FileIO after use
                    try {
                        sourceFileIO.close();
                    } catch (Exception e) {
                        logger.warn("[Iceberg Plugin] Failed to close source FileIO: {}", e.getMessage());
                    }
                }
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
            
        } finally {
            // CRITICAL: Always restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
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
            String remoteStoreRepo = indexMetadata.getSettings().get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
            if (remoteStoreRepo == null || remoteStoreRepo.isEmpty()) {
                throw new IllegalArgumentException("Remote segment store repository is not configured for index " + indexName);
            }

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
     * Add new files to Iceberg catalog.
     * Copies files from service account to S3 Tables warehouse in customer account.
     * Uses two separate credential contexts:
     * 1. Service account credentials for reading source files (pre-created sourceFileIO)
     * 2. Customer role credentials for writing to S3 Tables warehouse (table.io())
     */
    private void addFilesToCatalog(Table table, Map<String, UploadedSegmentMetadata> filesToAdd,
                                   org.apache.iceberg.aws.s3.S3FileIO sourceFileIO) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());

        try {
            // Use pre-created source FileIO (has service account credentials)
            // Use table's FileIO which has CUSTOMER ROLE credentials for writing to warehouse
            org.apache.iceberg.io.FileIO destFileIO = table.io();

            for (Map.Entry<String, UploadedSegmentMetadata> entry : filesToAdd.entrySet()) {
                String sourceS3Path = entry.getKey();
                UploadedSegmentMetadata metadata = entry.getValue();

                PartitionValues partitionValues = extractPartitionValues(sourceS3Path);
                String relativePath = extractRelativePath(sourceS3Path);
                String destinationPath = table.location() + "/" + relativePath;

                logger.info("[Iceberg Plugin] Copying file: {} -> {}", sourceS3Path, destinationPath);

                // Copy file using separate credential contexts
                copyS3File(sourceFileIO, destFileIO, sourceS3Path, destinationPath);

                long fileSize = metadata.getLength();
                long recordCount = fileSize > 0 ? 100L : 1L;

                logger.info("[Iceberg Plugin] Registering file: path={}, size={} bytes, partitions=[{}, {}]",
                           destinationPath, fileSize, partitionValues.indexUuid, partitionValues.shardId);

                DataFiles.Builder builder = DataFiles.builder(table.spec())
                    .withPath(destinationPath)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount);

                if (table.spec().isPartitioned()) {
                    String partitionPath = String.format("index_uuid=%s/shard_id=%d",
                                                        partitionValues.indexUuid,
                                                        partitionValues.shardId);
                    builder.withPartitionPath(partitionPath);
                }

                table.newAppend().appendFile(builder.build()).commit();
                logger.info("[Iceberg Plugin] File copied and registered successfully");
            }
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Failed to copy files: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to copy files to S3 Tables warehouse", e);
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

            // Find the start of the UUID (22 character base64-like format)
            int uuidIndex = -1;
            for (int i = 0; i < parts.length; i++) {
                // UUID can be either:
                // 1. Standard format: 8-4-4-4-12 characters (36 chars with dashes)
                // 2. Base64 format: 22 characters with letters, numbers, hyphens, underscores
                logger.debug("[Iceberg Plugin] Checking part[{}]: '{}' (length={})", i, parts[i], parts[i].length());

                if ((parts[i].length() == 36 && parts[i].matches("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"))
                    || (parts[i].length() == 22 && parts[i].matches("[a-zA-Z0-9_-]{22}"))) {
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
     * Copy file from source to destination using separate FileIO instances.
     * @param sourceFileIO FileIO with service account credentials for reading
     * @param destFileIO FileIO with customer role credentials for writing
     */
    private void copyS3File(org.apache.iceberg.io.FileIO sourceFileIO,
                           org.apache.iceberg.io.FileIO destFileIO,
                           String sourcePath, String destPath) throws IOException {
        try {
            logger.info("[Iceberg Plugin] Copying: {} -> {}", sourcePath, destPath);

            org.apache.iceberg.io.InputFile inputFile = sourceFileIO.newInputFile(sourcePath);
            org.apache.iceberg.io.OutputFile outputFile = destFileIO.newOutputFile(destPath);

            try (java.io.InputStream in = inputFile.newStream();
                 org.apache.iceberg.io.PositionOutputStream out = outputFile.createOrOverwrite()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }

            logger.info("[Iceberg Plugin] Copy completed");
        } catch (Exception e) {
            logger.error("[Iceberg Plugin] Copy failed: {}", e.getMessage());
            throw new IOException("File copy failed", e);
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

    /**
     * Create table in customer account using assumed role credentials.
     */
    private Table createTableWithAssumedRole(String indexName, org.apache.iceberg.Schema schema,
                                            String roleArn, String s3Bucket, String region) {
        logger.info("[Iceberg S3Tables] Creating table in customer account: role={}, bucket={}, region={}",
                   roleArn, s3Bucket, region);

        RESTCatalog customerCatalog = null;
        try {
            // Get file credentials to use for STS authentication
            Map<String, String> fileCreds = s3TablesManager.getFileCredentials();
            
            // Build credentials provider from file
            software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider;
            if (fileCreds.containsKey("access_key") && fileCreds.containsKey("secret_key")) {
                software.amazon.awssdk.auth.credentials.AwsCredentials credentials;
                if (fileCreds.containsKey("session_token")) {
                    credentials = software.amazon.awssdk.auth.credentials.AwsSessionCredentials.create(
                        fileCreds.get("access_key"),
                        fileCreds.get("secret_key"),
                        fileCreds.get("session_token")
                    );
                } else {
                    credentials = software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                        fileCreds.get("access_key"),
                        fileCreds.get("secret_key")
                    );
                }
                credentialsProvider = software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(credentials);
                logger.info("[Iceberg S3Tables] Using file credentials for STS client");
            } else {
                throw new RuntimeException("File credentials not available for STS client");
            }
            
            // Check if we're already using the target role
            software.amazon.awssdk.services.sts.StsClient stsClient = software.amazon.awssdk.services.sts.StsClient.builder()
                .region(software.amazon.awssdk.regions.Region.of(region))
                .credentialsProvider(credentialsProvider)
                .build();

            software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse callerIdentity =
                stsClient.getCallerIdentity();
            String currentArn = callerIdentity.arn();

            logger.info("[Iceberg S3Tables] Current identity: {}", currentArn);

            // If already using target role, skip AssumeRole
            if (currentArn.contains(roleArn.substring(roleArn.lastIndexOf('/') + 1))) {
                logger.info("[Iceberg S3Tables] Already using target role, skipping AssumeRole");
                // Use existing credentials from default chain
            } else {
                // Assume customer role
                logger.info("[Iceberg S3Tables] Assuming role: {}", roleArn);

                software.amazon.awssdk.services.sts.model.AssumeRoleRequest assumeRoleRequest =
                    software.amazon.awssdk.services.sts.model.AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName("opensearch-iceberg-sync")
                        .durationSeconds(3600)
                        .build();

                software.amazon.awssdk.services.sts.model.AssumeRoleResponse assumeRoleResponse =
                    stsClient.assumeRole(assumeRoleRequest);

                software.amazon.awssdk.services.sts.model.Credentials credentials = assumeRoleResponse.credentials();

                logger.info("[Iceberg S3Tables] Successfully assumed role");

                // Set AWS credentials as system properties
                System.setProperty("aws.accessKeyId", credentials.accessKeyId());
                System.setProperty("aws.secretAccessKey", credentials.secretAccessKey());
                System.setProperty("aws.sessionToken", credentials.sessionToken());
                System.setProperty("aws.region", region);
            }

            // Extract account ID from role ARN
            String accountId = roleArn.split(":")[4];

            // Create catalog with assumed credentials
            customerCatalog = new RESTCatalog();
            Map<String, String> properties = new HashMap<>();

            String bucketArn = String.format("arn:aws:s3tables:%s:%s:bucket/%s", region, accountId, s3Bucket);
            String s3TablesEndpoint = String.format("https://s3tables.%s.amazonaws.com/iceberg", region);

            logger.info("[Iceberg S3Tables] Bucket ARN: {}, Endpoint: {}", bucketArn, s3TablesEndpoint);

            properties.put("uri", s3TablesEndpoint);
            properties.put("warehouse", bucketArn);
            properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", String.format("https://s3.%s.amazonaws.com", region));
            properties.put("client.region", region);
            properties.put("rest.sigv4-enabled", "true");
            properties.put("rest.signing-name", "s3tables");
            properties.put("rest.signing-region", region);

            ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());

            try {
                logger.info("[Iceberg S3Tables] Initializing customer catalog...");
                customerCatalog.initialize("customer_catalog", properties);
                logger.info("[Iceberg S3Tables] Customer catalog initialized successfully");

                // Create or load table
                String tableName = indexName.toLowerCase().replace("-", "_");
                String ns = clusterService.getClusterSettings().get(S3TABLES_NAMESPACE_SETTING);
                if (ns == null || ns.isEmpty()) ns = "opensearch";
                Namespace namespace = Namespace.of(ns);

                try {
                    customerCatalog.loadNamespaceMetadata(namespace);
                } catch (Exception e) {
                    try {
                        customerCatalog.createNamespace(namespace, new HashMap<>());
                        logger.info("[Iceberg S3Tables] Created namespace '{}' in customer account", ns);
                    } catch (Exception createEx) {
                        logger.warn("[Iceberg S3Tables] Failed to create namespace: {}", createEx.getMessage());
                    }
                }

                TableIdentifier tableId = TableIdentifier.of(ns, tableName);

                try {
                    Table table = customerCatalog.loadTable(tableId);
                    logger.info("[Iceberg S3Tables] Loaded existing table: {}", indexName);
                    return table;
                } catch (Exception e) {
                    logger.info("[Iceberg S3Tables] Table not found, creating new table: {}", indexName);

                    org.apache.iceberg.PartitionSpec partitionSpec = null;
                    if (schema.findField("index_uuid") != null && schema.findField("shard_id") != null) {
                        partitionSpec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                            .identity("index_uuid")
                            .identity("shard_id")
                            .build();
                    }

                    try {
                        if (partitionSpec != null) {
                            return customerCatalog.buildTable(tableId, schema)
                                .withPartitionSpec(partitionSpec)
                                .withProperty("write.format.default", "parquet")
                                .withProperty("write.parquet.compression-codec", "snappy")
                                .create();
                        } else {
                            return customerCatalog.buildTable(tableId, schema)
                                .withProperty("write.format.default", "parquet")
                                .withProperty("write.parquet.compression-codec", "snappy")
                                .create();
                        }
                    } catch (org.apache.iceberg.exceptions.AlreadyExistsException alreadyExists) {
                        // Race condition: table was created between load and create attempts
                        logger.info("[Iceberg S3Tables] Table created concurrently, loading it now");
                        return customerCatalog.loadTable(tableId);
                    }
                }
            } finally {
                Thread.currentThread().setContextClassLoader(originalClassLoader);
            }
        } catch (software.amazon.awssdk.services.sts.model.StsException e) {
            logger.error("[Iceberg S3Tables] STS AssumeRole failed: {} - {}", e.awsErrorDetails().errorCode(), e.getMessage());
            if (customerCatalog != null) {
                try { customerCatalog.close(); } catch (Exception ignored) {}
            }
            throw new RuntimeException("Failed to assume role: " + e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Failed to create table in customer account: {}", e.getMessage(), e);
            if (customerCatalog != null) {
                try { customerCatalog.close(); } catch (Exception ignored) {}
            }
            throw new RuntimeException("Failed to create table in customer account", e);
        }
    }

    /**
     * Create source FileIO with credentials from file.
     */
    private org.apache.iceberg.aws.s3.S3FileIO createSourceFileIO(IndexMetadata indexMetadata) {
        String sourceRegion = getSourceBucketRegion(indexMetadata);
        logger.info("[Iceberg Plugin] Creating source FileIO with region: {}", sourceRegion);
        
        // Get credentials from file (loaded by S3TablesIcebergManager)
        Map<String, String> fileCreds = s3TablesManager.getFileCredentials();
        
        if (!fileCreds.containsKey("access_key") || !fileCreds.containsKey("secret_key")) {
            throw new RuntimeException("Iceberg credentials not found in file. " +
                "Please ensure credentials file exists at configured path with required keys: " +
                "aws_access_key_id, aws_secret_access_key");
        }
        
        // Create credentials from file
        software.amazon.awssdk.auth.credentials.AwsCredentials credentials;
        if (fileCreds.containsKey("session_token")) {
            credentials = software.amazon.awssdk.auth.credentials.AwsSessionCredentials.create(
                fileCreds.get("access_key"),
                fileCreds.get("secret_key"),
                fileCreds.get("session_token")
            );
            logger.info("[Iceberg Plugin] Using session credentials from file");
        } else {
            credentials = software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                fileCreds.get("access_key"),
                fileCreds.get("secret_key")
            );
            logger.info("[Iceberg Plugin] Using basic credentials from file");
        }
        
        logger.info("[Iceberg Plugin] Using credentials with accessKeyId: {}...",
                   credentials.accessKeyId().substring(0, Math.min(4, credentials.accessKeyId().length())));
        
        // Create S3 client with file credentials
        software.amazon.awssdk.services.s3.S3Client s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
            .region(software.amazon.awssdk.regions.Region.of(sourceRegion))
            .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(credentials))
            .build();
        
        // Create custom S3FileIO that uses our pre-configured S3 client
        org.apache.iceberg.aws.s3.S3FileIO sourceFileIO = new org.apache.iceberg.aws.s3.S3FileIO(() -> s3Client);
        sourceFileIO.initialize(new HashMap<>());
        
        return sourceFileIO;
    }

    /**
     * Get source bucket region from repository settings.
     */
    private String getSourceBucketRegion(IndexMetadata indexMetadata) {
        try {
            String remoteStoreRepo = indexMetadata.getSettings().get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
            if (remoteStoreRepo == null || remoteStoreRepo.isEmpty()) {
                logger.warn("[Iceberg Plugin] Remote segment store repository not configured, defaulting region to us-west-2");
                return "us-west-2";
            }
            org.opensearch.repositories.Repository repository = repositoriesServiceSupplier.get().repository(remoteStoreRepo);
            org.opensearch.repositories.blobstore.BlobStoreRepository blobRepo =
                (org.opensearch.repositories.blobstore.BlobStoreRepository) repository;
            
            org.opensearch.cluster.metadata.RepositoryMetadata repoMetadata = blobRepo.getMetadata();
            Settings repoSettings = repoMetadata.settings();
            
            String region = repoSettings.get("region", "us-west-2");
            logger.info("[Iceberg Plugin] Detected source bucket region from repository: {}", region);
            return region;
        } catch (Exception e) {
            logger.warn("[Iceberg Plugin] Failed to detect source bucket region, defaulting to us-west-2: {}", e.getMessage());
            return "us-west-2";
        }
    }
}

