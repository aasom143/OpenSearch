/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Deleted-doc filtering on the standard `DataSourceExec` path via the parquet virtual row-number
//! column (backport of DF55).
//!
//! Enables `parquet.virtual.row_number` on the `ParquetSource` so each decoded row carries its
//! file-local physical position (computed from row-group metadata — zero column I/O, and correct
//! under row-group/page pruning and predicate pushdown). The scan is wrapped in
//! [`LiveDocsRowNumberFilterExec`], which drops rows whose position is deleted and strips the
//! virtual column, so the output matches the requested projection.
//!
//! One file group per file, so a `DataSourceExec` partition maps to exactly one segment and the
//! per-partition deleted set is that segment's file-local dead positions. The caller must disable
//! file-scan repartitioning (`datafusion.optimizer.repartition_file_scans = false`) so that
//! partition↔file mapping holds.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::compute::FilterBuilder;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use futures::StreamExt;

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::live_docs_table_provider::LiveDocsFileInfo;

/// Name of the appended virtual row-number column (0-based file-local physical position).
const ROW_NUMBER_COL: &str = "__row_number__";
/// Arrow extension-type name the parquet reader recognizes as the row-number virtual column.
const VIRTUAL_ROW_NUMBER_EXT: &str = "parquet.virtual.row_number";
/// Arrow metadata key that carries an extension type's name.
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

/// The virtual row-number field the vendored parquet source enables via
/// `ArrowReaderOptions::with_virtual_columns`. It is appended after the projected file columns.
fn row_number_field() -> FieldRef {
    let mut md = HashMap::new();
    md.insert(
        ARROW_EXTENSION_NAME_KEY.to_string(),
        VIRTUAL_ROW_NUMBER_EXT.to_string(),
    );
    Arc::new(Field::new(ROW_NUMBER_COL, DataType::Int64, false).with_metadata(md))
}

/// File-local dead-doc positions (strictly ascending) derived from a segment's liveDocs (alive
/// bits). Empty when the segment has no deletions. The row-number indexes this directly (no
/// `row_base`) because each file group holds a single file/segment.
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

/// TableProvider that scans parquet with the virtual row-number column enabled and filters deleted
/// docs post-decode, keeping DataFusion's row-group/page pruning and predicate pushdown.
pub struct LiveDocsRowNumberProvider {
    schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    context_id: i64,
}

impl LiveDocsRowNumberProvider {
    pub fn new(
        schema: SchemaRef,
        files: Vec<LiveDocsFileInfo>,
        store_url: ObjectStoreUrl,
        context_id: i64,
    ) -> Self {
        Self {
            schema,
            files,
            store_url,
            context_id,
        }
    }
}

impl std::fmt::Debug for LiveDocsRowNumberProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveDocsRowNumberProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsRowNumberProvider {
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
        // One file group per file → a scan partition maps to exactly one segment, so its deleted
        // set is that segment's file-local dead positions. Requires file-scan repartitioning off.
        let mut groups: Vec<FileGroup> = Vec::with_capacity(self.files.len());
        let mut deleted_per_partition: Vec<Arc<Vec<u64>>> = Vec::with_capacity(self.files.len());
        for f in &self.files {
            groups.push(FileGroup::new(vec![PartitionedFile::from(f.object_meta.clone())]));
            deleted_per_partition.push(Arc::new(build_file_deleted(
                self.context_id,
                f.writer_generation,
                f.num_rows,
            )));
        }

        let projected_schema: SchemaRef = match projection {
            Some(proj) => Arc::new(
                self.schema
                    .project(proj)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ),
            None => self.schema.clone(),
        };
        let num_out_cols = projected_schema.fields().len();

        let table_schema = TableSchema::new(self.schema.clone(), vec![]);
        // Enable the virtual row-number column; the vendored opener appends it after the projected
        // file columns and RowNumberReader fills it (zero I/O, skip-aware).
        let parquet_source =
            ParquetSource::new(table_schema).with_virtual_columns(vec![row_number_field()]);
        let mut builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(groups);
        if let Some(proj) = projection {
            builder = builder
                .with_projection_indices(Some(proj.clone()))
                .map_err(|e| DataFusionError::Internal(format!("{e}")))?;
        }
        let scan = DataSourceExec::from_data_source(builder.build());

        Ok(Arc::new(LiveDocsRowNumberFilterExec::new(
            scan,
            projected_schema,
            num_out_cols,
            Arc::new(deleted_per_partition),
        )))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

/// Physical node that drops deleted rows using the appended virtual row-number column and strips
/// that column from the output. Wraps the parquet `DataSourceExec` directly (one partition per
/// file/segment) so repartitioning is disabled to keep the partition↔segment mapping.
#[derive(Debug)]
pub struct LiveDocsRowNumberFilterExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    /// Number of requested output columns; the virtual row-number sits at this index in the
    /// decoded batch (real columns [0, num_out_cols), row-number at num_out_cols).
    num_out_cols: usize,
    /// One entry per input partition (= per file/segment): its file-local dead positions, ascending.
    deleted_per_partition: Arc<Vec<Arc<Vec<u64>>>>,
    properties: Arc<PlanProperties>,
}

impl LiveDocsRowNumberFilterExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        num_out_cols: usize,
        deleted_per_partition: Arc<Vec<Arc<Vec<u64>>>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            output_schema,
            num_out_cols,
            deleted_per_partition,
            properties,
        }
    }

    /// Emit only the requested (real) columns, no filtering.
    fn project_only(
        batch: &RecordBatch,
        num_out_cols: usize,
        output_schema: &SchemaRef,
        n: usize,
    ) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = (0..num_out_cols).map(|p| batch.column(p).clone()).collect();
        RecordBatch::try_new_with_options(
            output_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Drop rows whose file-local row-number is deleted, then emit only the real columns (strip the
    /// virtual row-number). `deleted` is file-local, ascending. The row-number is contiguous within
    /// a batch when no rows were skipped during decode (range-clear, O(deletes-in-batch)); when
    /// pushdown/pruning gapped the batch it falls back to a per-row binary search (still correct).
    fn mask_batch(
        batch: &RecordBatch,
        deleted: &[u64],
        num_out_cols: usize,
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let n = batch.num_rows();
        // No deletes, empty batch, or the virtual column wasn't appended (all-alive segment) → project.
        if deleted.is_empty() || n == 0 || batch.num_columns() <= num_out_cols {
            return Self::project_only(batch, num_out_cols, output_schema, n);
        }

        let rownum = batch
            .column(num_out_cols)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("virtual row_number column is not Int64".into())
            })?;

        let mut keep = vec![true; n];
        let mut cleared = 0usize;
        let g0 = rownum.value(0);
        let g_last = rownum.value(n - 1);
        if g_last - g0 == (n as i64 - 1) {
            // Contiguous: clear the deleted ids in [g0, g0 + n) via a range lookup.
            let gn = g0 + n as i64;
            let start = deleted.partition_point(|&x| (x as i64) < g0);
            let mut idx = start;
            while idx < deleted.len() && (deleted[idx] as i64) < gn {
                keep[(deleted[idx] as i64 - g0) as usize] = false;
                cleared += 1;
                idx += 1;
            }
        } else {
            for i in 0..n {
                if deleted.binary_search(&(rownum.value(i) as u64)).is_ok() {
                    keep[i] = false;
                    cleared += 1;
                }
            }
        }

        if cleared == 0 {
            return Self::project_only(batch, num_out_cols, output_schema, n);
        }

        let kept = n - cleared;
        let mask = BooleanArray::from(keep);
        let predicate = FilterBuilder::new(&mask).optimize().build();
        // Filter only the output columns — never the virtual row-number (read solely to build the mask).
        let cols: Vec<ArrayRef> = (0..num_out_cols)
            .map(|p| predicate.filter(batch.column(p)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        RecordBatch::try_new_with_options(
            output_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(kept)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for LiveDocsRowNumberFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LiveDocsRowNumberFilterExec: partitions={}",
            self.deleted_per_partition.len()
        )
    }
}

impl ExecutionPlan for LiveDocsRowNumberFilterExec {
    fn name(&self) -> &str {
        "LiveDocsRowNumberFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .unwrap_or_else(|| Arc::clone(&self.input));
        Ok(Arc::new(LiveDocsRowNumberFilterExec {
            input,
            output_schema: self.output_schema.clone(),
            num_out_cols: self.num_out_cols,
            deleted_per_partition: Arc::clone(&self.deleted_per_partition),
            properties: Arc::clone(&self.properties),
        }))
    }

    /// Repartitioning is disabled: the partition↔file/segment mapping (and thus the per-partition
    /// deleted set) must be preserved, so we never add or split partitions.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted = self
            .deleted_per_partition
            .get(partition)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()));
        let num_out_cols = self.num_out_cols;
        let output_schema = self.output_schema.clone();
        let mapped = input_stream.map(move |res| {
            res.and_then(|batch| Self::mask_batch(&batch, &deleted, num_out_cols, &output_schema))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            mapped,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
