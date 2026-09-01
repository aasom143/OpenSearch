/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Predicate-only evaluator — no collector, pure parquet-native filtering.
//!
//! Used for `FilterClass::None` with `emit_row_ids=true`: the query has no
//! `index_filter(...)` call (no Lucene collector), only DataFusion predicates.
//! Candidates default to the page-pruned universe; `on_batch_mask` evaluates
//! only the residual predicate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use roaring::RoaringBitmap;

use super::eval_helpers::{
    compute_page_ranges, evaluate_residual, universe_bitmap_from_page_ranges,
};
use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{bitmap_to_packed_bits, PositionMap};
use crate::indexed_table::stream::RowGroupInfo;

/// Evaluator for predicate-only queries (no Collector).
///
/// Candidates = page-pruned universe. Residual predicate applied in `on_batch_mask`.
/// When `deleted_doc_filtering_required` is set, the segment's liveDocs bitset is ANDed into the
/// candidate bitmap per row group (same mechanism as `SingleCollectorEvaluator`) so deleted rows
/// are excluded before refinement — this is how the pure-DF indexed path filters deletes.
pub struct PredicateOnlyEvaluator {
    page_pruner: Arc<PagePruner>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    page_prune_metrics: Option<PagePruneMetrics>,
    stats_prune_tree: Option<Arc<StatsPruneTree>>,
    /// Reverse map: absolute RG index → position in `rg_can_match` vectors.
    rg_index_to_pos: HashMap<usize, usize>,
    /// When true, AND the segment's liveDocs into candidates (drop deleted rows).
    deleted_doc_filtering_required: bool,
    /// Per-query id, routes the getLiveDocs FFM upcall to the right Java handle.
    context_id: i64,
    /// Stable per-segment id; identifies which segment's liveDocs to fetch.
    writer_generation: i64,
}

impl PredicateOnlyEvaluator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
        stats_prune_tree: Option<Arc<StatsPruneTree>>,
        rg_index_to_pos: HashMap<usize, usize>,
        deleted_doc_filtering_required: bool,
        context_id: i64,
        writer_generation: i64,
    ) -> Self {
        Self {
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            stats_prune_tree,
            rg_index_to_pos,
            deleted_doc_filtering_required,
            context_id,
            writer_generation,
        }
    }
}

impl RowGroupBitsetSource for PredicateOnlyEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        // RG-level early-exit: precomputed from column stats at construction.
        if let Some(ref spt) = self.stats_prune_tree {
            if let Some(&pos) = self.rg_index_to_pos.get(&rg.index) {
                if let Some(&false) = spt.rg_can_match.get(pos) {
                    native_bridge_common::log_debug!(
                        "PredicateOnly: skipping RG {} — pruned by RG-level stats",
                        rg.index
                    );
                    return Ok(None);
                }
            }
        }

        let page_ranges = compute_page_ranges(
            self.pruning_predicate.as_ref(),
            &self.page_pruner,
            rg,
            min_doc,
            self.page_prune_metrics.as_ref(),
        );

        let mut candidates = match universe_bitmap_from_page_ranges(&page_ranges, rg) {
            Some(bm) if bm.is_empty() => return Ok(None),
            Some(bm) => bm,
            None => return Ok(None),
        };

        // LiveDocs filtering: AND with the segment's live-docs bitset to exclude deleted rows.
        // Same mechanism as SingleCollectorEvaluator — the liveDocs words are LSB-first packed bits
        // in the RG-relative `[min_doc, max_doc)` range, converted to a RoaringBitmap at the
        // RG-relative offset and intersected with the candidate universe.
        if self.deleted_doc_filtering_required {
            if let Ok(Some(live_bits)) =
                get_live_docs(self.context_id, self.writer_generation, min_doc, max_doc)
            {
                let offset = (min_doc as i64 - rg.first_row) as u32;
                let bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(live_bits.as_ptr() as *const u8, live_bits.len() * 8)
                };
                let live_bm = RoaringBitmap::from_lsb0_bytes(offset, bytes);
                candidates &= live_bm;
            }
            if candidates.is_empty() {
                return Ok(None);
            }
        }

        let mask_len = rg.num_rows as usize;
        let packed_bits = bitmap_to_packed_bits(&candidates, mask_len as u32);
        let mask_buffer = datafusion::arrow::buffer::Buffer::from_vec(packed_bits);
        // With delete filtering, carry the candidate bitmap (page-universe ∩ liveDocs, RG-relative)
        // into on_batch_mask. Block-granular RowSelection can't skip scattered deletes (no whole
        // block is all-deleted), so deleted rows are delivered and must be masked post-decode. The
        // Some(residual) refinement mask is authoritative (the stream ignores the candidate-derived
        // current_mask when on_batch_mask returns Some), so we AND the liveDocs in there ourselves.
        let context: Box<dyn std::any::Any + Send + Sync> = if self.deleted_doc_filtering_required {
            Box::new(candidates.clone())
        } else {
            Box::new(())
        };
        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context,
            mask_buffer: Some(mask_buffer),
        }))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        position_map: &PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // Residual predicate (e.g. RegionID = 229), if any.
        let residual_mask = match self.residual_expr {
            Some(ref residual) => Some(evaluate_residual(residual, batch, batch_len)?),
            None => None,
        };
        // Deleted-doc mask: candidate carried from prefetch_rg is the RG-relative universe already
        // ANDed with liveDocs. Map this batch's delivered rows to their RG positions and keep only
        // the alive (candidate) ones — this is what actually excludes deleted rows from the output.
        let live_mask: Option<BooleanArray> = if self.deleted_doc_filtering_required {
            rg_state.downcast_ref::<RoaringBitmap>().map(|cand| {
                (0..batch_len)
                    .map(|i| {
                        position_map
                            .rg_position(batch_offset + i)
                            .map(|pos| cand.contains(pos as u32))
                            .unwrap_or(false)
                    })
                    .collect::<BooleanArray>()
            })
        } else {
            None
        };
        match (residual_mask, live_mask) {
            (Some(r), Some(l)) => Ok(Some(
                datafusion::arrow::compute::kernels::boolean::and_kleene(&r, &l)
                    .map_err(|e| format!("delete AND residual: {e}"))?,
            )),
            (Some(r), None) => Ok(Some(r)),
            (None, Some(l)) => Ok(Some(l)),
            (None, None) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::page_pruner::PagePruner;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn minimal_page_pruner() -> Arc<PagePruner> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 8]))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = tmp.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load(&file, options).unwrap();
        Arc::new(PagePruner::new(
            meta.schema(),
            meta.metadata().clone(),
            meta.schema().clone(),
        ))
    }

    #[test]
    fn stats_prune_tree_skips_rg_when_false() {
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![false],
            children: vec![],
        };
        let eval = PredicateOnlyEvaluator::new(
            pruner,
            None,
            None,
            None,
            Some(Arc::new(spt)),
            HashMap::from([(0, 0)]),
            false,
            0,
            0,
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn stats_prune_tree_allows_rg_when_true() {
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![true],
            children: vec![],
        };
        let eval = PredicateOnlyEvaluator::new(
            pruner,
            None,
            None,
            None,
            Some(Arc::new(spt)),
            HashMap::from([(0, 0)]),
            false,
            0,
            0,
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("should have candidates");
        assert_eq!(prefetched.candidates.len(), 8);
    }

    #[test]
    fn stats_prune_tree_none_does_not_prune() {
        let pruner = minimal_page_pruner();
        let eval = PredicateOnlyEvaluator::new(pruner, None, None, None, None, HashMap::new(), false, 0, 0);
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("should have candidates");
        assert_eq!(prefetched.candidates.len(), 8);
    }
}
