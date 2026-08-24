/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! QTF-like deleted-doc filtering (Approach 3).
//!
//! Wraps a scan that projects the stored `__row_id__` (local, per-segment) plus the `row_base`
//! partition column (per-file cumulative offset). For each row it computes the shard-global id
//! `__row_id__ + row_base`, drops rows present in a global deleted-docs bitmap, and outputs only
//! the original query columns (strips `__row_id__` and `row_base`). This is a value-based filter:
//! O(1) per surviving row, no `RowSelection` runs and no reader run-overhead, so it stays flat as
//! deletions scatter — the pathological case for the RowSelection path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use native_bridge_common::log_info;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::compute::{filter_record_batch, FilterBuilder};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use roaring::RoaringTreemap;

/// Physical node that filters shard-globally-deleted rows using the stored `__row_id__` column
/// and a `row_base` partition column, then strips both helper columns from the output.
#[derive(Debug)]
pub struct LiveDocsRowIdFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Shard-global ids of deleted docs (`local __row_id__ + row_base`).
    deleted: Arc<RoaringTreemap>,
    /// Index of the (global-computed) `__row_id__` column in the input schema.
    rowid_idx: usize,
    /// Index of the `row_base` partition column in the input schema.
    rowbase_idx: usize,
    /// Input-schema column indices to keep in the output (everything except the two helpers).
    output_indices: Arc<Vec<usize>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LiveDocsRowIdFilterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        deleted: Arc<RoaringTreemap>,
        rowid_col: &str,
        rowbase_col: &str,
    ) -> Result<Self> {
        let in_schema = input.schema();
        let rowid_idx = in_schema.index_of(rowid_col)?;
        let rowbase_idx = in_schema.index_of(rowbase_col)?;

        let output_indices: Vec<usize> = (0..in_schema.fields().len())
            .filter(|&i| i != rowid_idx && i != rowbase_idx)
            .collect();
        let out_fields: Vec<_> = output_indices
            .iter()
            .map(|&i| in_schema.field(i).clone())
            .collect();
        let output_schema = Arc::new(Schema::new(out_fields));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            input,
            deleted,
            rowid_idx,
            rowbase_idx,
            output_indices: Arc::new(output_indices),
            output_schema,
            properties,
        })
    }

    /// Filter one batch: drop rows whose global id is deleted, then keep only the output columns.
    fn filter_batch(
        batch: &RecordBatch,
        deleted: &RoaringTreemap,
        rowid_idx: usize,
        rowbase_idx: usize,
        output_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let n = batch.num_rows();
        let rowid = batch
            .column(rowid_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("__row_id__ column is not Int64".into()))?;
        let rowbase = batch
            .column(rowbase_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("row_base column is not Int64".into()))?;

        let mut keep = Vec::with_capacity(n);
        for i in 0..n {
            let global = rowid.value(i) + rowbase.value(i);
            keep.push(!deleted.contains(global as u64));
        }
        let mask = BooleanArray::from(keep);

        // Filter every column, then select only the output columns (drop the two helpers).
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let columns: Vec<_> = output_indices
            .iter()
            .map(|&i| filtered.column(i).clone())
            .collect();
        RecordBatch::try_new_with_options(
            output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(filtered.num_rows())),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for LiveDocsRowIdFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LiveDocsRowIdFilterExec: deleted={}", self.deleted.len())
    }
}

impl ExecutionPlan for LiveDocsRowIdFilterExec {
    fn name(&self) -> &str {
        "LiveDocsRowIdFilterExec"
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
        Ok(Arc::new(LiveDocsRowIdFilterExec {
            input: children[0].clone(),
            deleted: Arc::clone(&self.deleted),
            rowid_idx: self.rowid_idx,
            rowbase_idx: self.rowbase_idx,
            output_indices: Arc::clone(&self.output_indices),
            output_schema: self.output_schema.clone(),
            properties: Arc::clone(&self.properties),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted = Arc::clone(&self.deleted);
        let rowid_idx = self.rowid_idx;
        let rowbase_idx = self.rowbase_idx;
        let output_indices = Arc::clone(&self.output_indices);
        let output_schema = self.output_schema.clone();

        let stream = input_stream.map(move |batch_res| {
            batch_res.and_then(|batch| {
                Self::filter_batch(
                    &batch,
                    &deleted,
                    rowid_idx,
                    rowbase_idx,
                    &output_indices,
                    &output_schema,
                )
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

// ── LiveDocsRowIdTableProvider ───────────────────────────────────────────────

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::Statistics;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::physical_plan::Partitioning;
use futures::{stream, TryStreamExt};

/// Name of the appended virtual row-number column (0-based physical row position within the file,
/// produced by the parquet reader from row-group metadata with zero column I/O).
const ROW_NUMBER_COL: &str = "__row_number__";
/// Arrow extension-type name the parquet reader recognizes as the row-number virtual column.
/// (The `RowNumber` extension type lives in a feature-gated module, so we set the name directly;
/// the reader keys off this exact string — see `parquet::arrow::schema::virtual_type`.)
const VIRTUAL_ROW_NUMBER_EXT: &str = "parquet.virtual.row_number";
/// Arrow metadata key that carries an extension type's name.
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

pub use crate::api::ShardFileInfo;

const ROW_BASE_COL: &str = "row_base";

/// TableProvider for the QTF-like deleted-doc filtering path. Exposes `row_base` as a partition
/// column (like `ShardTableProvider`), projects the stored `__row_id__` alongside the requested
/// columns, and wraps the scan in [`LiveDocsRowIdFilterExec`] so deleted rows are dropped by value
/// and the two helper columns are stripped — the output schema matches the requested projection.
pub struct LiveDocsRowIdTableProvider {
    /// Physical parquet file schema (includes `__row_id__`), WITHOUT the `row_base` partition col.
    file_schema: SchemaRef,
    /// Table schema = file_schema + `row_base` (partition col appended last).
    table_schema: SchemaRef,
    files: Vec<ShardFileInfo>,
    /// Writer generation per file (aligned with `files`), used to resolve liveDocs at scan time.
    writer_generations: Vec<i64>,
    store_url: ObjectStoreUrl,
    /// Query context id for the getLiveDocs FFM callback.
    context_id: i64,
}

impl LiveDocsRowIdTableProvider {
    pub fn new(
        file_schema: SchemaRef,
        files: Vec<ShardFileInfo>,
        writer_generations: Vec<i64>,
        store_url: ObjectStoreUrl,
        context_id: i64,
    ) -> Self {
        let mut fields: Vec<Arc<Field>> = file_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(ROW_BASE_COL, DataType::Int64, true)));
        let table_schema = Arc::new(Schema::new(fields));
        Self {
            file_schema,
            table_schema,
            files,
            writer_generations,
            store_url,
            context_id,
        }
    }

    /// Build the shard-global deleted-docs bitmap by folding each segment's liveDocs
    /// (`local __row_id__ + row_base`). Called at scan time so the getLiveDocs FFM binding
    /// (registered by `configureFilterDelegation`) is available — building it at session-context
    /// creation time is too early (the handle isn't registered yet).
    fn build_deleted(&self) -> Vec<u64> {
        // Global deleted ids, produced strictly ascending: files are ordered by row_base
        // (asserted in scan), locals ascending within a file (word index ascending, and
        // trailing_zeros yields bits ascending). So the result is already sorted — no treemap,
        // no sort — and we use it directly as a sorted slice for the per-batch range lookup.
        let mut deleted: Vec<u64> = Vec::new();
        for (file, &gen) in self.files.iter().zip(self.writer_generations.iter()) {
            if let Ok(Some(alive)) =
                crate::indexed_table::ffm_callbacks::get_live_docs(self.context_id, gen, 0, file.num_rows as i32)
            {
                for (w, &word) in alive.iter().enumerate() {
                    if word == u64::MAX {
                        continue;
                    }
                    let base = (w as u64) * 64;
                    let mut dead = !word;
                    while dead != 0 {
                        let local = base + dead.trailing_zeros() as u64;
                        if local < file.num_rows {
                            deleted.push(file.row_base as u64 + local);
                        }
                        dead &= dead - 1;
                    }
                }
            }
        }
        debug_assert!(deleted.windows(2).all(|w| w[0] < w[1]), "deleted ids must be strictly ascending");
        deleted
    }
}

impl std::fmt::Debug for LiveDocsRowIdTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveDocsRowIdTableProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsRowIdTableProvider {
    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
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
        debug_assert!(
            self.files.windows(2).all(|w| w[0].row_base <= w[1].row_base),
            "LiveDocsRowIdTableProvider: files not ordered by row_base — global ids would be wrong"
        );
        let num_file_cols = self.file_schema.fields().len();

        // Requested columns as indices into `file_schema`. `row_base` (the trailing partition
        // column in `table_schema`) is never read from parquet — it is folded into the global id
        // per file — so drop it if referenced. `__row_id__` is also NOT read: the row's physical
        // position now comes from the zero-I/O virtual row-number column instead of a stored column.
        let proj: Vec<usize> = match projection {
            Some(p) => p.iter().copied().filter(|&i| i < num_file_cols).collect(),
            None => (0..num_file_cols).collect(),
        };

        // Output schema = requested file columns, in the requested order.
        let out_fields: Vec<Arc<Field>> =
            proj.iter().map(|&i| self.file_schema.fields()[i].clone()).collect();
        let output_schema = Arc::new(Schema::new(out_fields));

        // The parquet reader returns projected leaves in ascending file order. Read = sorted-unique
        // proj; `out_col_positions[k]` maps the k-th requested column to its slot in the read batch
        // so we can re-emit in the caller's requested order. (Assumes a flat schema — arrow field
        // index == parquet leaf index — which holds for these analytics tables.)
        let mut read_leaf_indices = proj.clone();
        read_leaf_indices.sort_unstable();
        read_leaf_indices.dedup();
        let out_col_positions: Vec<usize> = proj
            .iter()
            .map(|i| read_leaf_indices.binary_search(i).expect("proj is a subset of read_leaf_indices"))
            .collect();

        // Build the deleted-docs bitmap now (scan time — the getLiveDocs binding exists).
        let build_start = Instant::now();
        let deleted = Arc::new(self.build_deleted());
        log_info!(
            "[virtual-rowid] build_deleted: deleted={} took {:.1}ms",
            deleted.len(),
            build_start.elapsed().as_secs_f64() * 1000.0
        );

        // Start as a single partition (all row groups). DataFusion's EnforceDistribution rule
        // repartitions us up to the session's target_partitions via `repartitioned()` — exactly
        // like DataSourceExec — so parallelism is driven by the standard knob, not a hardcoded one.
        let files = Arc::new(self.files.clone());
        let parts = assign_row_groups(&files, 1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(parts.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Arc::new(VirtualRowIdDeleteExec {
            files,
            store_url: self.store_url.clone(),
            read_leaf_indices,
            out_col_positions: Arc::new(out_col_positions),
            output_schema,
            deleted,
            partitions: Arc::new(parts),
            properties,
        }))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

// ── VirtualRowIdDeleteExec ───────────────────────────────────────────────────

/// Balance all `(file_idx, row_group_idx)` pairs across `num_partitions` buckets by row count,
/// mirroring DataFusion's file-group partitioner. Used both for the initial single-partition plan
/// and by `repartitioned()` when EnforceDistribution asks for more partitions.
fn assign_row_groups(files: &[ShardFileInfo], num_partitions: usize) -> Vec<Vec<(usize, usize)>> {
    let mut all_rgs: Vec<(usize, usize, u64)> = Vec::new();
    let mut total_rows: u64 = 0;
    for (fi, file) in files.iter().enumerate() {
        for (ri, &rows) in file.row_group_row_counts.iter().enumerate() {
            all_rgs.push((fi, ri, rows));
            total_rows += rows;
        }
    }
    let p = num_partitions.max(1).min(all_rgs.len().max(1));
    let per_partition_rows = ((total_rows as f64) / (p as f64)).ceil().max(1.0) as u64;
    let mut parts: Vec<Vec<(usize, usize)>> = vec![Vec::new(); p];
    let mut bucket = 0usize;
    let mut acc: u64 = 0;
    for (fi, ri, rows) in all_rgs {
        if acc >= per_partition_rows && bucket + 1 < p {
            bucket += 1;
            acc = 0;
        }
        parts[bucket].push((fi, ri));
        acc += rows;
    }
    parts.retain(|v| !v.is_empty());
    if parts.is_empty() {
        parts.push(Vec::new());
    }
    parts
}

/// INSTRUMENTATION: per-partition mask accounting. Logs a summary on Drop (i.e. when the
/// partition's stream is exhausted/dropped) so we can attribute the delete-scaling cost:
/// how many batches ran `filter_record_batch` vs. took the project-only fast path, and how long
/// the filter kernel took in aggregate.
struct MaskStats {
    partition: usize,
    batches: AtomicU64,
    filtered: AtomicU64,
    skipped: AtomicU64,
    rows_in: AtomicU64,
    deletes_cleared: AtomicU64,
    filter_nanos: AtomicU64,
}

impl MaskStats {
    fn new(partition: usize) -> Self {
        Self {
            partition,
            batches: AtomicU64::new(0),
            filtered: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            rows_in: AtomicU64::new(0),
            deletes_cleared: AtomicU64::new(0),
            filter_nanos: AtomicU64::new(0),
        }
    }
}

impl Drop for MaskStats {
    fn drop(&mut self) {
        log_info!(
            "[virtual-rowid] partition={} batches={} filtered={} skipped(fast-path)={} rows_in={} deletes_cleared={} filter_kernel_ms={:.1}",
            self.partition,
            self.batches.load(Ordering::Relaxed),
            self.filtered.load(Ordering::Relaxed),
            self.skipped.load(Ordering::Relaxed),
            self.rows_in.load(Ordering::Relaxed),
            self.deletes_cleared.load(Ordering::Relaxed),
            self.filter_nanos.load(Ordering::Relaxed) as f64 / 1.0e6,
        );
    }
}

/// Leaf scan that reads the requested columns from each parquet file PLUS a zero-I/O virtual
/// row-number column (physical 0-based row position within the file, produced by the parquet reader
/// from row-group metadata — no column bytes read), drops shard-globally-deleted rows by value
/// (`row_number + row_base` ∈ deleted), and emits only the requested columns.
///
/// This replaces reading the stored `__row_id__` column (see [`LiveDocsRowIdFilterExec`]): the
/// position previously read from a ~6 B/row on-disk column is now free, so `bytes_scanned` falls
/// back to the query columns only.
#[derive(Debug)]
pub struct VirtualRowIdDeleteExec {
    files: Arc<Vec<ShardFileInfo>>,
    store_url: ObjectStoreUrl,
    /// Sorted, de-duplicated file-column indices to read (parquet leaf indices for a flat schema).
    read_leaf_indices: Vec<usize>,
    /// For each requested output column, its slot within the read batch (excludes row_number).
    out_col_positions: Arc<Vec<usize>>,
    output_schema: SchemaRef,
    /// Sorted (ascending) global deleted ids; used as a slice for per-batch range lookups.
    deleted: Arc<Vec<u64>>,
    /// One entry per output partition: the `(file_idx, row_group_idx)` pairs it reads. Row groups
    /// are balanced across partitions by row count so the scan parallelizes like `DataSourceExec`.
    partitions: Arc<Vec<Vec<(usize, usize)>>>,
    properties: Arc<PlanProperties>,
}

impl VirtualRowIdDeleteExec {
    /// Clone this exec with a new partition assignment (used by `repartitioned()`).
    fn with_partitions(&self, parts: Vec<Vec<(usize, usize)>>) -> Arc<dyn ExecutionPlan> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.output_schema.clone()),
            Partitioning::UnknownPartitioning(parts.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(VirtualRowIdDeleteExec {
            files: Arc::clone(&self.files),
            store_url: self.store_url.clone(),
            read_leaf_indices: self.read_leaf_indices.clone(),
            out_col_positions: Arc::clone(&self.out_col_positions),
            output_schema: self.output_schema.clone(),
            deleted: Arc::clone(&self.deleted),
            partitions: Arc::new(parts),
            properties,
        })
    }

    /// Emit only the requested columns for `batch`, cheaply (no filtering).
    fn project_only(
        batch: &RecordBatch,
        out_col_positions: &[usize],
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> =
            out_col_positions.iter().map(|&p| batch.column(p).clone()).collect();
        RecordBatch::try_new_with_options(
            output_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Drop deleted rows and re-emit the requested columns in order. `deleted` is the sorted
    /// (ascending) list of global deleted ids. The virtual `row_number` is contiguous within a
    /// batch (whole-row-group reads, no RowSelection), so we clear only the deleted ids that fall
    /// in the batch's `[first, first+n)` global range — O(deletes-in-batch), not O(rows). Batches
    /// with no deleted rows skip `filter_record_batch` entirely.
    fn mask_and_project(
        batch: &RecordBatch,
        deleted: &[u64],
        row_base: i64,
        rownum_idx: usize,
        out_col_positions: &[usize],
        output_schema: &SchemaRef,
        stats: &MaskStats,
    ) -> Result<RecordBatch> {
        let n = batch.num_rows();
        stats.batches.fetch_add(1, Ordering::Relaxed);
        stats.rows_in.fetch_add(n as u64, Ordering::Relaxed);
        if deleted.is_empty() || n == 0 {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
            return Self::project_only(batch, out_col_positions, output_schema);
        }

        let rownum = batch
            .column(rownum_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("virtual row_number column is not Int64".into()))?;

        let g0 = rownum.value(0) + row_base;
        let g_last = rownum.value(n - 1) + row_base;
        let contiguous = g_last - g0 == (n as i64 - 1);

        let mut keep = vec![true; n];
        let mut cleared: u64 = 0;
        if contiguous {
            // Clear the deleted ids in [g0, g0 + n) via a range lookup on the sorted slice.
            let gn = g0 + n as i64;
            let start = deleted.partition_point(|&x| (x as i64) < g0);
            let mut idx = start;
            while idx < deleted.len() && (deleted[idx] as i64) < gn {
                keep[(deleted[idx] as i64 - g0) as usize] = false;
                cleared += 1;
                idx += 1;
            }
        } else {
            // Defensive fallback (should not happen with whole-RG reads): per-row binary search.
            for i in 0..n {
                let g = (rownum.value(i) + row_base) as u64;
                if deleted.binary_search(&g).is_ok() {
                    keep[i] = false;
                    cleared += 1;
                }
            }
        }

        if cleared == 0 {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
            return Self::project_only(batch, out_col_positions, output_schema);
        }

        stats.filtered.fetch_add(1, Ordering::Relaxed);
        stats.deletes_cleared.fetch_add(cleared, Ordering::Relaxed);

        let filter_start = Instant::now();
        let kept = n - cleared as usize;
        // Build the filter predicate once, then apply it ONLY to the output columns — never to the
        // virtual row_number (an Int64 we read solely to build the mask and then discard). Filtering
        // the whole batch would copy ~kept row_number values for nothing (the largest column here).
        let mask = BooleanArray::from(keep);
        let predicate = FilterBuilder::new(&mask).optimize().build();
        let cols: Vec<ArrayRef> = out_col_positions
            .iter()
            .map(|&p| predicate.filter(batch.column(p)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let out = RecordBatch::try_new_with_options(
            output_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(kept)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        stats
            .filter_nanos
            .fetch_add(filter_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        out
    }
}

impl DisplayAs for VirtualRowIdDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VirtualRowIdDeleteExec: files={}, deleted={}, position=virtual_row_number",
            self.files.len(),
            self.deleted.len()
        )
    }
}

impl ExecutionPlan for VirtualRowIdDeleteExec {
    fn name(&self) -> &str {
        "VirtualRowIdDeleteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Split the row groups across `target_partitions` so EnforceDistribution can parallelize the
    /// scan — same mechanism DataSourceExec uses. Returns `None` when it wouldn't add partitions.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if target_partitions <= self.partitions.len() {
            return Ok(None);
        }
        let parts = assign_row_groups(&self.files, target_partitions);
        if parts.len() <= self.partitions.len() {
            return Ok(None);
        }
        Ok(Some(self.with_partitions(parts)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let store = context.runtime_env().object_store(&self.store_url)?;
        let files = Arc::clone(&self.files);
        // Sorted (ascending) global deleted ids — built directly as a Vec (no RoaringTreemap),
        // used per batch with a range lookup instead of a per-row contains() over all scanned rows.
        let deleted = Arc::clone(&self.deleted);
        // Per-partition mask accounting; logs a summary when the stream is dropped.
        let stats = Arc::new(MaskStats::new(partition));
        let read_leaf_indices = self.read_leaf_indices.clone();
        let rownum_idx = read_leaf_indices.len(); // virtual row_number is appended last
        let out_col_positions = Arc::clone(&self.out_col_positions);
        let output_schema = self.output_schema.clone();

        // Group this partition's (file_idx, row_group_idx) assignment by file, preserving order,
        // so each file is opened once and read via `with_row_groups`.
        let mut per_file: Vec<(usize, Vec<usize>)> = Vec::new();
        for &(fi, ri) in self.partitions.get(partition).map(|v| v.as_slice()).unwrap_or(&[]) {
            match per_file.last_mut() {
                Some(last) if last.0 == fi => last.1.push(ri),
                _ => per_file.push((fi, vec![ri])),
            }
        }

        // One record-batch stream per file in this partition, concatenated. The virtual row_number
        // is 0-based within its file (correct even for a row-group subset), so we add the file's
        // row_base to form the shard-global id.
        let batches = stream::iter(per_file.into_iter())
            .map(move |(fi, row_groups)| {
                let store = Arc::clone(&store);
                let deleted = Arc::clone(&deleted);
                let stats = Arc::clone(&stats);
                let read_leaf_indices = read_leaf_indices.clone();
                let out_col_positions = Arc::clone(&out_col_positions);
                let output_schema = output_schema.clone();
                let file_location = files[fi].object_meta.location.clone();
                let file_size = files[fi].object_meta.size;
                let row_base = files[fi].row_base;
                async move {
                    let mut ext_md = std::collections::HashMap::new();
                    ext_md.insert(
                        ARROW_EXTENSION_NAME_KEY.to_string(),
                        VIRTUAL_ROW_NUMBER_EXT.to_string(),
                    );
                    let row_number_field = Arc::new(
                        Field::new(ROW_NUMBER_COL, DataType::Int64, false).with_metadata(ext_md),
                    );
                    let reader = ParquetObjectReader::new(store, file_location)
                        .with_file_size(file_size);
                    let options = ArrowReaderOptions::new()
                        .with_virtual_columns(vec![row_number_field])
                        .map_err(|e| DataFusionError::Execution(format!("virtual column: {e}")))?;
                    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("open parquet: {e}")))?;
                    let mask =
                        ProjectionMask::leaves(builder.parquet_schema(), read_leaf_indices.iter().copied());
                    let rb_stream = builder
                        .with_projection(mask)
                        .with_row_groups(row_groups)
                        .build()
                        .map_err(|e| DataFusionError::Execution(format!("build parquet stream: {e}")))?;
                    let mapped = rb_stream.map(move |res| {
                        res.map_err(|e| DataFusionError::Execution(format!("read parquet: {e}")))
                            .and_then(|batch| {
                                Self::mask_and_project(
                                    &batch,
                                    &deleted,
                                    row_base,
                                    rownum_idx,
                                    &out_col_positions,
                                    &output_schema,
                                    &stats,
                                )
                            })
                    });
                    Ok::<_, DataFusionError>(mapped)
                }
            })
            .buffered(1)
            .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            batches,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
