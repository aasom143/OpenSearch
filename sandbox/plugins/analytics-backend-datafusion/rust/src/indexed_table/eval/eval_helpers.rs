/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for evaluators (SingleCollector, PredicateOnly, Tree).

use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNode;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use roaring::RoaringBitmap;

use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner};
use crate::indexed_table::stream::RowGroupInfo;

/// Compute page-pruned ranges for a row group.
/// Returns `None` if no pruning predicate is available (all rows pass).
/// Returns `Some(vec![])` if all pages are pruned (RG can be skipped).
pub fn compute_page_ranges(
    pruning_predicate: Option<&Arc<PruningPredicate>>,
    page_pruner: &PagePruner,
    rg: &RowGroupInfo,
    min_doc: i32,
    page_prune_metrics: Option<&PagePruneMetrics>,
) -> Option<Vec<(i32, i32)>> {
    pruning_predicate.and_then(|pp| {
        page_pruner
            .prune_rg(pp, rg.index, page_prune_metrics)
            .map(|sel| {
                let mut ranges = Vec::new();
                let mut rg_pos: i64 = 0;
                for s in sel.iter() {
                    if s.skip {
                        rg_pos += s.row_count as i64;
                    } else {
                        let abs_min = min_doc + rg_pos as i32;
                        let abs_max = min_doc + rg_pos as i32 + s.row_count as i32;
                        ranges.push((abs_min, abs_max));
                        rg_pos += s.row_count as i64;
                    }
                }
                ranges
            })
    })
}

/// Build a candidate bitmap from page-pruned ranges (universe — all surviving pages).
/// Returns `None` if all pages were pruned.
pub fn universe_bitmap_from_page_ranges(
    page_ranges: &Option<Vec<(i32, i32)>>,
    rg: &RowGroupInfo,
) -> Option<RoaringBitmap> {
    match page_ranges {
        Some(r) if r.is_empty() => None,
        Some(r) => {
            let mut bm = RoaringBitmap::new();
            for (r_min, r_max) in r {
                let lo = (*r_min as i64 - rg.first_row) as u32;
                let hi = (*r_max as i64 - rg.first_row) as u32;
                bm.insert_range(lo..hi);
            }
            Some(bm)
        }
        None => {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0..rg.num_rows as u32);
            Some(bm)
        }
    }
}

/// Evaluate a residual predicate against a batch, returning a BooleanArray mask.
///
/// `remapped` must already be reseated to the delivered batch's schema (see
/// [`CachedResidual`] / [`remap_expr_to_batch`]).
fn eval_remapped(
    remapped: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    let value = remapped
        .evaluate(batch)
        .map_err(|e| format!("evaluate_residual: {}", e))?;
    let array = value
        .into_array(batch_len)
        .map_err(|e| format!("evaluate_residual into_array: {}", e))?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "evaluate_residual: did not produce BooleanArray".to_string())
        .cloned()
}

/// Evaluate a residual predicate against a batch, returning a BooleanArray mask.
///
/// Remaps the expression to the batch schema on every call — kept for callers
/// (e.g. the bitmap-tree path) that don't hold a per-query evaluator instance to
/// cache on. Hot per-batch evaluators should use [`CachedResidual`] instead,
/// which remaps once.
pub fn evaluate_residual(
    residual: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    let remapped = remap_expr_to_batch(residual, batch)?;
    eval_remapped(&remapped, batch, batch_len)
}

/// A residual `PhysicalExpr` that remaps its column indices to the delivered
/// batch schema exactly once, then reuses the reseated expression for every
/// subsequent batch.
///
/// Substrait-decoded predicates carry column indices into the full table
/// schema, but the delivered batch is projected (only predicate-referenced
/// columns), so the indices must be reseated by name via
/// [`remap_expr_to_batch`]. The projection is fixed for the whole query, so the
/// reseated expression is identical for every batch — recomputing it per batch
/// (a full `PhysicalExpr` tree clone + rebuild) was pure repeated work on the
/// hot path (e.g. ~7K batches for a non-selective ClickBench aggregation).
///
/// The first `eval` remaps and caches; later `eval`s skip straight to
/// `evaluate`. Thread-safe via `OnceLock` (the stream may evaluate batches from
/// multiple RGs); racing initializers produce identical expressions, so the
/// `OnceLock` winner is immaterial.
pub struct CachedResidual {
    residual: Arc<dyn PhysicalExpr>,
    remapped: OnceLock<Arc<dyn PhysicalExpr>>,
}

impl CachedResidual {
    pub fn new(residual: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            residual,
            remapped: OnceLock::new(),
        }
    }

    /// Evaluate the residual against `batch`, remapping to the batch schema on
    /// the first call only.
    pub fn eval(&self, batch: &RecordBatch, batch_len: usize) -> Result<BooleanArray, String> {
        // `get_or_init` can't return a `Result`, so remap fallibly first and
        // only cache on success. On the (cached) hot path this is a single
        // relaxed load.
        if let Some(remapped) = self.remapped.get() {
            return eval_remapped(remapped, batch, batch_len);
        }
        let remapped = remap_expr_to_batch(&self.residual, batch)?;
        // If another thread won the race, `set` returns Err with our value; we
        // just use whichever is now stored (they're equivalent).
        let _ = self.remapped.set(remapped);
        let remapped = self
            .remapped
            .get()
            .expect("remapped was just set or won by another thread");
        eval_remapped(remapped, batch, batch_len)
    }
}

/// Remap column references in a PhysicalExpr to match the batch schema.
/// The expression may reference columns by index in the full table schema,
/// but the batch only contains projected columns. This rewrites Column
/// expressions to use the batch's field positions by name lookup.
pub fn remap_expr_to_batch(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Arc<dyn PhysicalExpr>, String> {
    let batch_schema = batch.schema();
    expr.clone()
        .transform(|node| {
            use datafusion::common::tree_node::Transformed;
            if let Some(col) = node.downcast_ref::<Column>() {
                if let Ok(idx) = batch_schema.index_of(col.name()) {
                    let new_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(col.name(), idx));
                    return Ok(Transformed::yes(new_col));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .map_err(|e| format!("remap_expr_to_batch: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr};

    /// Build `col(name, idx) != 0` as a residual `PhysicalExpr`.
    fn neq_zero(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(name, idx)),
            Operator::NotEq,
            lit(0i32),
        ))
    }

    fn batch_one_col(name: &str, vals: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals))]).unwrap()
    }

    #[test]
    fn cached_residual_remaps_and_matches_uncached() {
        // Residual references column index 5 in the FULL table schema, but the
        // delivered batch is projected to a single column at index 0 — so the
        // remap must reseat the index by name.
        let residual = neq_zero("AdvEngineID", 5);
        let cached = CachedResidual::new(residual.clone());

        let batch = batch_one_col("AdvEngineID", vec![0, 1, 0, 2]);
        let expected = BooleanArray::from(vec![false, true, false, true]);

        // Cached path matches the uncached helper.
        assert_eq!(cached.eval(&batch, 4).unwrap(), expected);
        assert_eq!(evaluate_residual(&residual, &batch, 4).unwrap(), expected);
    }

    #[test]
    fn cached_residual_reuses_across_batches() {
        // Second call hits the cached (already-remapped) expression and must
        // still produce the correct, batch-specific mask.
        let cached = CachedResidual::new(neq_zero("AdvEngineID", 5));

        let b1 = batch_one_col("AdvEngineID", vec![0, 7]);
        assert_eq!(
            cached.eval(&b1, 2).unwrap(),
            BooleanArray::from(vec![false, true])
        );

        let b2 = batch_one_col("AdvEngineID", vec![3, 0, 0]);
        assert_eq!(
            cached.eval(&b2, 3).unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }
}
