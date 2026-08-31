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
//! under row-group/page pruning and predicate pushdown), and exposes a `row_base` partition column
//! that DataFusion stamps onto every row of a file (a literal, per `PartitionedFile`). The scan is
//! wrapped in [`LiveDocsRowNumberFilterExec`], which drops rows whose **shard-global** id
//! (`row_base + row_number`) is in a single deleted-docs bitmap and strips both helper columns.
//!
//! Keying on the global id (not on partition index) makes filtering **order-independent**:
//! DataFusion may assign file groups to partitions in any order and split them across partitions,
//! but each row still carries its own `row_base`, so the global id — and therefore the mask — is
//! always correct regardless of how the scan is partitioned.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::compute::FilterBuilder;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use futures::StreamExt;
use native_bridge_common::log_info;

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::live_docs_table_provider::LiveDocsFileInfo;

/// Name of the appended virtual row-number column (0-based file-local physical position).
const ROW_NUMBER_COL: &str = "__row_number__";
/// Arrow extension-type name the parquet reader recognizes as the row-number virtual column.
const VIRTUAL_ROW_NUMBER_EXT: &str = "parquet.virtual.row_number";
/// Arrow metadata key that carries an extension type's name.
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// Name of the per-file `row_base` partition column (shard-global offset of the file's first row).
const ROW_BASE_COL: &str = "row_base";

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

/// Build the shard-global deleted-docs bitmap by folding each segment's liveDocs into
/// `row_base + local_dead_position`. `row_bases[i]` is the global offset of `files[i]`'s first row
/// (cumulative row count in file order). The result is strictly ascending — files are visited in
/// ascending `row_base`, and locals ascending within a file — so it is used directly as a sorted
/// slice for the per-batch lookup, no sort needed.
fn build_global_deleted(context_id: i64, files: &[LiveDocsFileInfo], row_bases: &[u64]) -> Vec<u64> {
    let mut deleted: Vec<u64> = Vec::new();
    for (f, &row_base) in files.iter().zip(row_bases.iter()) {
        if let Ok(Some(alive)) =
            get_live_docs(context_id, f.writer_generation, 0, f.num_rows as i32)
        {
            for (w, &word) in alive.iter().enumerate() {
                if word == u64::MAX {
                    continue;
                }
                let base = (w as u64) * 64;
                let mut dead = !word;
                while dead != 0 {
                    let local = base + dead.trailing_zeros() as u64;
                    if local < f.num_rows {
                        deleted.push(row_base + local);
                    }
                    dead &= dead - 1;
                }
            }
        }
    }
    debug_assert!(
        deleted.windows(2).all(|w| w[0] < w[1]),
        "global deleted ids must be strictly ascending"
    );
    deleted
}

/// TableProvider that scans parquet with the virtual row-number column enabled plus a `row_base`
/// partition column, and drops shard-globally-deleted rows post-decode — keeping DataFusion's
/// row-group/page pruning and predicate pushdown.
pub struct LiveDocsRowNumberProvider {
    /// The real file schema (what the planner projects against).
    file_schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    context_id: i64,
}

impl LiveDocsRowNumberProvider {
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
        let num_file_cols = self.file_schema.fields().len();

        // Assign each file a shard-global row_base (cumulative row count in file order) and stamp it
        // as the file's partition value. Emit one FileGroup per file so DataFusion runs the files in
        // parallel (one partition each); the partition↔file order is irrelevant because the global
        // id (row_base + row_number) keeps the mask correct regardless of the partition layout.
        let mut row_bases: Vec<u64> = Vec::with_capacity(self.files.len());
        let mut acc: u64 = 0;
        let mut file_groups: Vec<FileGroup> = Vec::with_capacity(self.files.len());
        for f in &self.files {
            row_bases.push(acc);
            let mut pf = PartitionedFile::from(f.object_meta.clone());
            pf.partition_values = vec![ScalarValue::Int64(Some(acc as i64))];
            file_groups.push(FileGroup::new(vec![pf]));
            acc += f.num_rows;
        }

        // Build the deleted-docs bitmap now (scan time — the getLiveDocs FFM binding exists).
        let deleted = Arc::new(build_global_deleted(self.context_id, &self.files, &row_bases));
        log_info!(
            "LiveDocsRowNumber scan: files={} global_deleted={}",
            self.files.len(),
            deleted.len()
        );

        // Output schema = requested real columns (row_base and row_number are stripped by the exec).
        let out_indices: Vec<usize> = match projection {
            Some(p) => p.iter().copied().filter(|&i| i < num_file_cols).collect(),
            None => (0..num_file_cols).collect(),
        };
        let out_fields: Vec<FieldRef> =
            out_indices.iter().map(|&i| self.file_schema.fields()[i].clone()).collect();
        let output_schema = Arc::new(Schema::new(out_fields));
        let num_out_cols = output_schema.fields().len();

        // table_schema = file schema + row_base partition column (appended last, index num_file_cols).
        let table_schema = TableSchema::new(
            self.file_schema.clone(),
            vec![Arc::new(Field::new(ROW_BASE_COL, DataType::Int64, true))],
        );
        // Enable the virtual row-number column; the vendored opener appends it after the projected
        // (real + row_base) columns and RowNumberReader fills it (zero I/O, skip-aware).
        let mut parquet_source =
            ParquetSource::new(table_schema).with_virtual_columns(vec![row_number_field()]);
        // Push the query predicate into the scan so parquet row-group / page-index pruning runs.
        // DataFusion can't push the FilterExec through this wrapper exec into the DataSourceExec
        // automatically, so without this the scan reads every row. The virtual row-number stays
        // correct under pruning (row-group-metadata based); mask_batch's binary-search path handles
        // the resulting gapped (non-contiguous) batches.
        if let Some(pred) = filters.iter().cloned().reduce(|a, b| a.and(b)) {
            if let Ok(df_schema) = DFSchema::try_from(self.file_schema.as_ref().clone()) {
                if let Ok(phys) = state.create_physical_expr(pred, &df_schema) {
                    parquet_source = parquet_source.with_predicate(phys);
                }
            }
        }

        // Projection into the table schema: requested real columns (in requested order) followed by
        // the row_base partition column. The decoded batch is therefore
        // [real cols (num_out_cols)] + [row_base] + [row_number].
        let mut proj_indices = out_indices.clone();
        proj_indices.push(num_file_cols); // row_base
        let builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(file_groups)
                .with_projection_indices(Some(proj_indices))
                .map_err(|e| DataFusionError::Internal(format!("{e}")))?;
        let scan = DataSourceExec::from_data_source(builder.build());

        Ok(Arc::new(LiveDocsRowNumberFilterExec::new(
            scan,
            output_schema,
            num_out_cols,
            deleted,
        )))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

/// Physical node that drops shard-globally-deleted rows using the `row_base` partition column and
/// the appended virtual row-number column (`global = row_base + row_number`), then strips both
/// helper columns from the output. Order-independent: correctness does not depend on how the input
/// scan is partitioned, so this simply mirrors the input's partitioning.
#[derive(Debug)]
pub struct LiveDocsRowNumberFilterExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    /// Number of requested output columns. Batch layout: real cols [0, num_out_cols), row_base at
    /// `num_out_cols`, virtual row-number at `num_out_cols + 1`.
    num_out_cols: usize,
    /// Shard-global deleted ids (`row_base + local`), strictly ascending; shared across partitions.
    deleted: Arc<Vec<u64>>,
    properties: Arc<PlanProperties>,
}

impl LiveDocsRowNumberFilterExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        num_out_cols: usize,
        deleted: Arc<Vec<u64>>,
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
            deleted,
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

    /// Drop rows whose shard-global id (`row_base + row_number`) is deleted, then emit only the real
    /// columns (strip row_base and row_number). `row_base` is a constant within a batch (the file's
    /// offset); the row-number is file-local. When the row-numbers are the fully-contiguous
    /// [rn0, rn0+n) run (no pruning/reorder gaps) the global ids are also contiguous and the deleted
    /// ids are cleared via a range lookup (O(deletes-in-batch)); otherwise a per-row binary search.
    fn mask_batch(
        batch: &RecordBatch,
        deleted: &[u64],
        num_out_cols: usize,
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let n = batch.num_rows();
        // No deletes, empty batch, or helper columns absent → project real columns through.
        if deleted.is_empty() || n == 0 || batch.num_columns() < num_out_cols + 2 {
            return Self::project_only(batch, num_out_cols, output_schema, n);
        }

        let row_base_arr = batch
            .column(num_out_cols)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("row_base column is not Int64".into()))?;
        let rownum = batch
            .column(num_out_cols + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("virtual row_number column is not Int64".into())
            })?;

        // row_base is a per-file constant literal, so it is identical for every row of a batch.
        let row_base = row_base_arr.value(0);

        let mut keep = vec![true; n];
        let mut cleared = 0usize;
        let rn0 = rownum.value(0);
        let rn_last = rownum.value(n - 1);
        let g0 = row_base + rn0;
        if rn_last - rn0 == n as i64 - 1 {
            // Contiguous: global ids span [g0, g0 + n); clear the deleted ids in that range.
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
                let global = (row_base + rownum.value(i)) as u64;
                if deleted.binary_search(&global).is_ok() {
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
        // Filter only the output columns — never the helper columns (read solely to build the mask).
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
            "LiveDocsRowNumberFilterExec: deleted={}",
            self.deleted.len()
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
        // Recompute properties so partitioning tracks the (possibly repartitioned) input.
        Ok(Arc::new(LiveDocsRowNumberFilterExec::new(
            input,
            self.output_schema.clone(),
            self.num_out_cols,
            Arc::clone(&self.deleted),
        )))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Let EnforceDistribution repartition the underlying parquet scan into the session's slice
        // count (search.concurrent.max_slice_count) so the delete path parallelizes like the plain
        // scan. with_new_children rewraps the repartitioned input and recomputes partitioning; the
        // global-bitmap mask is order-independent, so any slice layout is correct.
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted = Arc::clone(&self.deleted);
        let num_out_cols = self.num_out_cols;
        let output_schema = self.output_schema.clone();
        // TEMP diagnostics: per-stream rows/cleared, emitted from a terminal zero-row batch at
        // stream end (Drop-based logging is unreliable — polled streams outlive the query window).
        let rows = Arc::new(AtomicU64::new(0));
        let cleared = Arc::new(AtomicU64::new(0));
        let (rows_c, cleared_c) = (Arc::clone(&rows), Arc::clone(&cleared));
        let mapped = input_stream.map(move |res| {
            res.and_then(|batch| {
                let rows_in = batch.num_rows();
                let out = Self::mask_batch(&batch, &deleted, num_out_cols, &output_schema)?;
                rows_c.fetch_add(rows_in as u64, Ordering::Relaxed);
                cleared_c.fetch_add((rows_in - out.num_rows()) as u64, Ordering::Relaxed);
                Ok(out)
            })
        });
        let terminal_schema = self.output_schema.clone();
        let terminal = futures::stream::once(async move {
            log_info!(
                "LiveDocsRowNumber summary: partition={} rows_seen={} cleared={}",
                partition,
                rows.load(Ordering::Relaxed),
                cleared.load(Ordering::Relaxed)
            );
            Ok(RecordBatch::new_empty(terminal_schema))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            mapped.chain(terminal),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
