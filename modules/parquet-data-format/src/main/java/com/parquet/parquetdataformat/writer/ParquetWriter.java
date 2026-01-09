package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.iceberg.IcebergFileTracker;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.vsr.VSRManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.RemoteUploadCallback;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Parquet file writer implementation that integrates with OpenSearch's Writer interface.
 *
 * <p>This writer provides a high-level interface for writing Parquet documents to disk
 * using the underlying VSRManager for Arrow-based data management and native Rust
 * backend for efficient Parquet file generation.
 *
 * <p>Key features:
 * <ul>
 *   <li>Arrow schema-based document structure</li>
 *   <li>Batch-oriented writing with memory management</li>
 *   <li>Integration with OpenSearch indexing pipeline</li>
 *   <li>Native Rust backend for high-performance Parquet operations</li>
 *   <li>Iceberg metadata tracking for table management</li>
 * </ul>
 *
 * <p>The writer manages the complete lifecycle from document addition through
 * flushing and cleanup, delegating the actual Arrow and Parquet operations
 * to the {@link VSRManager}.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private static final Logger logger = LogManager.getLogger(ParquetWriter.class);

    private final String file;
    private final Schema schema;
    private final VSRManager vsrManager;
    private final long writerGeneration;
    private final IcebergFileTracker icebergTracker;
    private final RemoteUploadCallback remoteUploadCallback;

    public ParquetWriter(String file, Schema schema, long writerGeneration, ArrowBufferPool arrowBufferPool) {
        this.file = file;
        this.schema = schema;
        this.vsrManager = new VSRManager(file, schema, arrowBufferPool);
        this.writerGeneration = writerGeneration;
        this.icebergTracker = new IcebergFileTracker();
        
        // Create callback for remote upload success notifications
        this.remoteUploadCallback = new RemoteUploadCallback() {
            @Override
            public void onRemoteUploadSuccess(Collection<FileMetadata> uploadedFiles) {
                try {
                    if (uploadedFiles.isEmpty()) {
                        return;
                    }
                    
                    // FileMetadata from RemoteStoreRefreshListener contains:
                    // - dataFormat: index name (for Iceberg table identification)  
                    // - file: full S3 path
                    
                    // Extract index name from first file (all files belong to same index)
                    String indexName = uploadedFiles.iterator().next().dataFormat();
                    
                    // Extract S3 paths
                    List<String> s3Paths = uploadedFiles.stream()
                        .map(FileMetadata::file)
                        .collect(Collectors.toList());
                    
                    logger.info("[Iceberg] Received {} S3 paths for index '{}', committing to Iceberg", 
                               s3Paths.size(), indexName);
                    
                    // Commit these S3 paths to Iceberg (with index name for table identification)
                    icebergTracker.commitFilesForIndex(indexName, s3Paths);
                    
                    logger.info("[Iceberg] Successfully committed {} files to Iceberg catalog for index '{}'", 
                                s3Paths.size(), indexName);
                } catch (Exception e) {
                    logger.error("[Iceberg] Failed to commit files to Iceberg catalog after remote upload", e);
                    // Don't throw - we don't want to fail the upload if Iceberg commit fails
                }
            }
        };
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        return vsrManager.addToManagedVSR(d);
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        String fileName = vsrManager.flush(flushIn);
        // no data flushed
        if (fileName == null) {
            return FileInfos.empty();
        }
        Path file = Path.of(fileName);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(file.getParent())
            .writerGeneration(writerGeneration)
            .addFile(file.getFileName().toString())
            .build();

       // Track file for Iceberg (will be committed after successful remote upload)
       icebergTracker.trackFile(fileName);

       // NOTE: Iceberg commit removed from here - will happen after successful remote upload
       // via the remoteUploadCallback.onRemoteUploadSuccess() callback
       
        return FileInfos.builder().putWriterFileSet(PARQUET_DATA_FORMAT, writerFileSet).build();
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() {
        vsrManager.close();
    }

    @Override
    public ParquetDocumentInput newDocumentInput() {
        try {
            vsrManager.maybeRotateActiveVSR();
        } catch (IOException e) {
            logger.error("Failed to handle VSR rotation: {}", e.getMessage(), e);
        }

        // Get a new ManagedVSR from VSRManager for this document input
        return new ParquetDocumentInput(vsrManager.getActiveManagedVSR());
    }

    /**
     * Get the Iceberg file tracker for this writer.
     * Used by refresh operations to commit pending files.
     */
    public IcebergFileTracker getIcebergTracker() {
        return icebergTracker;
    }

    /**
     * Get the remote upload callback for this writer.
     * Called by RemoteStoreRefreshListener after successful S3 upload to update Iceberg metadata.
     * 
     * @return the callback that commits files to Iceberg catalog
     */
    @Override
    public RemoteUploadCallback getRemoteUploadCallback() {
        return remoteUploadCallback;
    }
}
