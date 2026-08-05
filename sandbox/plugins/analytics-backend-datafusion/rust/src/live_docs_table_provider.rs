/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! TableProvider that wraps a standard parquet scan with per-file liveDocs RowSelection.
//! When deleted docs exist, builds a `ParquetAccessPlan` per file from the liveDocs bitset
//! so parquet physically skips deleted rows during I/O.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use object_store::ObjectMeta;

use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::parquet_bridge;
use datafusion::execution::cache::cache_manager::FileMetadataCache;

/// Per-file info needed to build the liveDocs RowSelection.
pub struct LiveDocsFileInfo {
    pub object_meta: ObjectMeta,
    pub writer_generation: i64,
    pub num_rows: u64,
    pub row_group_row_counts: Vec<u64>,
}

pub struct LiveDocsTableProvider {
    schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    store: Arc<dyn object_store::ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    context_id: i64,
}

impl LiveDocsTableProvider {
    pub fn new(
        schema: SchemaRef,
        files: Vec<LiveDocsFileInfo>,
        store_url: ObjectStoreUrl,
        store: Arc<dyn object_store::ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        context_id: i64,
    ) -> Self {
        Self {
            schema,
            files,
            store_url,
            store,
            metadata_cache,
            context_id,
        }
    }

    /// Build a ParquetAccessPlan from a liveDocs bitset for one file.
    /// Converts the per-doc bitset into per-row-group RowSelections.
    fn build_access_plan(
        live_docs: &[u64],
        row_group_row_counts: &[u64],
    ) -> ParquetAccessPlan {

        let num_rgs = row_group_row_counts.len();
        let mut access_plan = ParquetAccessPlan::new_all(num_rgs);

        let mut doc_offset: usize = 0;
        for (rg_idx, &rg_rows) in row_group_row_counts.iter().enumerate() {
            let rg_rows = rg_rows as usize;
            let mut selectors = Vec::new();
            let mut i = 0;
            while i < rg_rows {
                // Find next run of live or dead docs
                let abs_pos = doc_offset + i;
                let word_idx = abs_pos / 64;
                let bit_idx = abs_pos % 64;
                let is_live = word_idx < live_docs.len() && (live_docs[word_idx] >> bit_idx) & 1 == 1;

                let run_start = i;
                while i < rg_rows {
                    let abs = doc_offset + i;
                    let w = abs / 64;
                    let b = abs % 64;
                    let live = w < live_docs.len() && (live_docs[w] >> b) & 1 == 1;
                    if live != is_live {
                        break;
                    }
                    i += 1;
                }
                let run_len = i - run_start;
                if is_live {
                    selectors.push(RowSelector::select(run_len));
                } else {
                    selectors.push(RowSelector::skip(run_len));
                }
            }
            doc_offset += rg_rows;

            // If all rows are live, keep as Scan (no selection needed)
            let all_live = selectors.len() == 1
                && matches!(selectors.first(), Some(s) if s.row_count == rg_rows && !s.skip);
            if !all_live {
                access_plan.set(rg_idx, RowGroupAccess::Selection(RowSelection::from(selectors)));
            }
        }
        access_plan
    }
}

impl std::fmt::Debug for LiveDocsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveDocsTableProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitioned_files: Vec<PartitionedFile> = Vec::with_capacity(self.files.len());

        for file_info in &self.files {
            let mut pf = PartitionedFile::from(file_info.object_meta.clone());

            // Load parquet metadata from cache to get RG row counts and actual num_rows.
            let pq_meta_result = parquet_bridge::load_parquet_metadata_with_meta(
                Arc::clone(&self.store),
                &file_info.object_meta.location,
                file_info.object_meta.clone(),
                Arc::clone(&self.metadata_cache),
            )
            .await;

            if let Ok((_schema, _size, pq_meta)) = pq_meta_result {
                let num_rows: i64 = pq_meta
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows())
                    .sum();
                let rg_row_counts: Vec<u64> = pq_meta
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows() as u64)
                    .collect();

                // Fetch liveDocs for this file's segment
                let live_docs_result = get_live_docs(
                    self.context_id,
                    file_info.writer_generation,
                    0,
                    num_rows as i32,
                );

                match live_docs_result {
                    Ok(Some(bitset)) => {
                        let access_plan = Self::build_access_plan(&bitset, &rg_row_counts);
                        pf = pf.with_extensions(Arc::new(access_plan));
                    }
                    Ok(None) => {
                        // All alive — no access plan needed
                    }
                    Err(_) => {
                        // Error fetching liveDocs — fall back to reading all rows
                    }
                }
            }

            partitioned_files.push(pf);
        }

        let file_groups = vec![FileGroup::new(partitioned_files)];
        let table_schema = TableSchema::new(self.schema.clone(), vec![]);
        let parquet_source = ParquetSource::new(table_schema);

        let mut builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(file_groups);

        if let Some(proj) = projection {
            builder = builder
                .with_projection_indices(Some(proj.clone()))
                .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{}", e)))?;
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
