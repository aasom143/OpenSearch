/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Deleted-doc filtering as a parquet **RowFilter predicate** on the virtual row-number column.
//!
//! Instead of a post-decode wrapper exec that masks every decoded row, this attaches each file's
//! file-local deleted positions to its `PartitionedFile` (as a `DeletedRowNumbers` extension) and
//! enables the virtual `row_number` column. The vendored opener appends a `RowFilter` predicate
//! that drops those rows using `row_number` — applied **after** the query predicate, so it only
//! evaluates on rows surviving pushdown, and unlike a `RowSelection` it is a boolean predicate (no
//! run-length encoding), so it stays flat as deletions scatter. The scan is a plain `DataSourceExec`
//! (no wrapper, no `row_base` column); DataFusion pruning/pushdown and parallelism are unaffected.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::DeletedRowNumbers;
use native_bridge_common::log_info;

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::live_docs_table_provider::LiveDocsFileInfo;

/// Name of the appended virtual row-number column (0-based file-local physical position).
const ROW_NUMBER_COL: &str = "__row_number__";
/// Arrow extension-type name the parquet reader recognizes as the row-number virtual column.
const VIRTUAL_ROW_NUMBER_EXT: &str = "parquet.virtual.row_number";
/// Arrow metadata key that carries an extension type's name.
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

/// The virtual row-number field enabled via `ArrowReaderOptions::with_virtual_columns`. Used only
/// by the delete `RowFilter` predicate; stripped from the scan output by the vendored opener.
fn row_number_field() -> FieldRef {
    let mut md = HashMap::new();
    md.insert(
        ARROW_EXTENSION_NAME_KEY.to_string(),
        VIRTUAL_ROW_NUMBER_EXT.to_string(),
    );
    Arc::new(Field::new(ROW_NUMBER_COL, DataType::Int64, false).with_metadata(md))
}

/// File-local dead-doc positions (strictly ascending) from a segment's liveDocs (alive bits).
/// Empty when the segment has no deletions. The virtual row-number is file-local, so no `row_base`.
fn build_file_deleted(context_id: i64, writer_generation: i64, num_rows: u64) -> Vec<u64> {
    let mut deleted = Vec::new();
    if let Ok(Some(alive)) = get_live_docs(context_id, writer_generation, 0, num_rows as i32) {
        for (w, &word) in alive.iter().enumerate() {
            if word == u64::MAX {
                continue;
            }
            let base = (w as u64) * 64;
            let mut dead = !word;
            while dead != 0 {
                let local = base + dead.trailing_zeros() as u64;
                if local < num_rows {
                    deleted.push(local);
                }
                dead &= dead - 1;
            }
        }
    }
    deleted
}

/// TableProvider that scans parquet with the virtual row-number enabled and attaches each file's
/// deleted positions as a `DeletedRowNumbers` extension, so the opener drops deleted rows via a
/// RowFilter predicate. Returns a plain `DataSourceExec` — no wrapper exec, no `row_base` column.
pub struct LiveDocsRowFilterProvider {
    file_schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    context_id: i64,
}

impl LiveDocsRowFilterProvider {
    pub fn new(
        file_schema: SchemaRef,
        files: Vec<LiveDocsFileInfo>,
        store_url: ObjectStoreUrl,
        context_id: i64,
    ) -> Self {
        Self {
            file_schema,
            files,
            store_url,
            context_id,
        }
    }
}

impl std::fmt::Debug for LiveDocsRowFilterProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveDocsRowFilterProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsRowFilterProvider {
    fn schema(&self) -> SchemaRef {
        self.file_schema.clone()
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // One FileGroup per file (parallel); attach each file's file-local deleted positions as a
        // DeletedRowNumbers extension the vendored opener turns into a RowFilter predicate.
        let mut total_deleted: usize = 0;
        let mut file_groups: Vec<FileGroup> = Vec::with_capacity(self.files.len());
        for f in &self.files {
            let deleted = build_file_deleted(self.context_id, f.writer_generation, f.num_rows);
            total_deleted += deleted.len();
            let mut pf = PartitionedFile::from(f.object_meta.clone());
            if deleted.is_empty() == false {
                pf = pf.with_extension(DeletedRowNumbers(Arc::new(deleted)));
            }
            file_groups.push(FileGroup::new(vec![pf]));
        }
        log_info!(
            "LiveDocsRowFilter scan: files={} total_deleted={}",
            self.files.len(),
            total_deleted
        );

        // Enable the virtual row-number (for the delete predicate only; opener strips it from output)
        // and push the query predicate into the scan for row-group/page pruning.
        let mut parquet_source = ParquetSource::new(TableSchema::new(self.file_schema.clone(), vec![]))
            .with_virtual_columns(vec![row_number_field()]);
        if let Some(pred) = filters.iter().cloned().reduce(|a, b| a.and(b)) {
            if let Ok(df_schema) = DFSchema::try_from(self.file_schema.as_ref().clone()) {
                if let Ok(phys) = state.create_physical_expr(pred, &df_schema) {
                    parquet_source = parquet_source.with_predicate(phys);
                }
            }
        }

        let mut builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(file_groups);
        if let Some(proj) = projection {
            builder = builder
                .with_projection_indices(Some(proj.clone()))
                .map_err(|e| DataFusionError::Internal(format!("{e}")))?;
        }
        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
