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

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::compute::filter_record_batch;
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
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, ScalarValue, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;

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
    fn build_deleted(&self) -> RoaringTreemap {
        let mut deleted = RoaringTreemap::new();
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
                            deleted.insert(file.row_base as u64 + local);
                        }
                        dead &= dead - 1;
                    }
                }
            }
        }
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug_assert!(
            self.files.windows(2).all(|w| w[0].row_base <= w[1].row_base),
            "LiveDocsRowIdTableProvider: files not ordered by row_base — global ids would be wrong"
        );
        let num_file_cols = self.file_schema.fields().len();
        let row_id_file_idx = self.file_schema.index_of(crate::ROW_ID_COLUMN_NAME)?;
        let row_base_idx = num_file_cols;

        let partitioned_files: Vec<PartitionedFile> = self
            .files
            .iter()
            .map(|file_info| {
                let mut pf = PartitionedFile::from(file_info.object_meta.clone());
                pf.partition_values = vec![ScalarValue::Int64(Some(file_info.row_base))];
                let file_stats = Arc::new(Statistics {
                    num_rows: Precision::Exact(file_info.num_rows as usize),
                    total_byte_size: Precision::Inexact(file_info.object_meta.size as usize),
                    column_statistics: vec![
                        datafusion::common::ColumnStatistics::new_unknown();
                        num_file_cols
                    ],
                });
                pf.with_statistics(file_stats)
            })
            .collect();

        // Read projection = requested columns, then the two helpers (__row_id__, row_base) so the
        // filter can compute the global id. The filter strips both, leaving the requested columns.
        let mut read_proj: Vec<usize> = match projection {
            Some(proj) => proj.clone(),
            None => (0..num_file_cols).collect(),
        };
        if !read_proj.contains(&row_id_file_idx) {
            read_proj.push(row_id_file_idx);
        }
        if !read_proj.contains(&row_base_idx) {
            read_proj.push(row_base_idx);
        }

        let table_schema =
            TableSchema::new(self.file_schema.clone(), vec![Arc::new(Field::new(ROW_BASE_COL, DataType::Int64, true))]);
        let mut parquet_source = ParquetSource::new(table_schema);

        // Push the query predicate into the parquet reader as a RowFilter so it filters rows
        // during decode and late-materializes __row_id__ only for survivors — otherwise the scan
        // reads __row_id__ for every row before the predicate (a FilterExec above) trims them.
        // The predicate is over the file schema; strip qualifiers so it binds unqualified. Any
        // filter we can't lower is left to the (Inexact) FilterExec above — correctness holds.
        if let Some(pred_expr) = conjunction(filters.iter().cloned()) {
            let stripped = pred_expr
                .transform(|node| match node {
                    Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))),
                    other => Ok(Transformed::no(other)),
                })
                .map(|t| t.data);
            if let Ok(stripped) = stripped {
                if let Ok(df_schema) = DFSchema::try_from(self.file_schema.as_ref().clone()) {
                    if let Ok(pred) = state.create_physical_expr(stripped, &df_schema) {
                        let pred: Arc<dyn PhysicalExpr> = pred;
                        parquet_source = parquet_source
                            .with_predicate(pred)
                            .with_pushdown_filters(true)
                            .with_reorder_filters(true);
                    }
                }
            }
        }

        let file_scan_config =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(vec![FileGroup::new(partitioned_files)])
                .with_projection_indices(Some(read_proj))
                .map_err(|e| DataFusionError::Internal(format!("{}", e)))?
                .build();

        let scan = DataSourceExec::from_data_source(file_scan_config);
        // Build the deleted bitmap now (scan time — the getLiveDocs binding exists).
        let deleted = Arc::new(self.build_deleted());
        let filter = LiveDocsRowIdFilterExec::try_new(
            scan,
            deleted,
            crate::ROW_ID_COLUMN_NAME,
            ROW_BASE_COL,
        )?;
        Ok(Arc::new(filter))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
