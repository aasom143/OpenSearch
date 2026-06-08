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

use super::eval_helpers::{compute_page_ranges, universe_bitmap_from_page_ranges, CachedResidual};
use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{
    bitmap_to_packed_bits, build_mask, packed_bits_to_boolean_array, PositionMap,
};
use crate::indexed_table::stream::RowGroupInfo;

/// Per-RG state carried from `prefetch_rg` to `on_batch_mask` for delete filtering. Holds the
/// candidate bitmap (page-universe ∩ liveDocs) and a pre-built full-RG delivered liveDocs mask.
/// `build_mask` needs the stream's `position_map` (only known at decode time), so the identity
/// case slices the pre-built `live_arr` per batch (zero-copy) and the rare non-identity case
/// maps the candidate bitmap through the position map — the same efficient path the stream uses
/// for `current_mask`, instead of a per-row `contains()`.
struct DeleteRgState {
    /// Candidate bitmap (RG-relative) — used only for the rare non-identity position_map fallback.
    candidates: RoaringBitmap,
    /// Full-RG liveDocs mask as a packed BooleanArray, pre-built in prefetch_rg from the packed bits
    /// already computed there (no recompute). For the common identity position_map, on_batch_mask
    /// just slices this per batch (zero-copy).
    live_arr: BooleanArray,
}

/// Evaluator for predicate-only queries (no Collector).
///
/// Candidates = page-pruned universe. Residual predicate applied in `on_batch_mask`.
/// When `deleted_doc_filtering_required` is set, the segment's liveDocs bitset is ANDed into the
/// candidate bitmap per row group (same mechanism as `SingleCollectorEvaluator`) so deleted rows
/// are excluded before refinement — this is how the pure-DF indexed path filters deletes.
pub struct PredicateOnlyEvaluator {
    page_pruner: Arc<PagePruner>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Residual predicate, remapped to the batch schema once and reused across
    /// batches. `None` when there is no residual (no page pruning either, so the
    /// candidate universe is gap-free and no per-batch filtering is needed).
    residual: Option<CachedResidual>,
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
            residual: residual_expr.map(CachedResidual::new),
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

        // Delete path: AND the segment's liveDocs into candidates so deleted rows are excluded.
        // Same mechanism as SingleCollectorEvaluator — the liveDocs words are LSB-first packed bits
        // in the RG-relative `[min_doc, max_doc)` range, converted to a RoaringBitmap at the
        // RG-relative offset and intersected with the candidate universe. Scattered deletes can't be
        // skipped by a coarse RowSelection (no whole block is all-deleted), so we deliver the full
        // row group and mask deleted rows post-decode in `on_batch_mask` — `forbid_parquet_pushdown`
        // forces pushdown OFF so parquet never drops rows mid-decode and misaligns the mask.
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
            // Pre-build the full-RG liveDocs BooleanArray from the packed candidate bits (no
            // recompute in on_batch_mask). `mask_buffer` stays None: `needs_row_mask()` is false, so
            // the stream never builds `current_mask` — the pre-built `live_arr` lives in
            // DeleteRgState and is applied by `on_batch_mask` instead. `selection_runs` stays None so
            // the stream delivers the full row group (candidate ∩ liveDocs) for post-decode masking.
            let mask_len = rg.num_rows as usize;
            let packed_bits = bitmap_to_packed_bits(&candidates, mask_len as u32);
            let live_arr = packed_bits_to_boolean_array(packed_bits, mask_len);
            return Ok(Some(PrefetchedRg {
                candidates: candidates.clone(),
                eval_nanos: t.elapsed().as_nanos() as u64,
                context: Box::new(DeleteRgState { candidates, live_arr }),
                mask_buffer: None,
                selection_runs: None,
            }));
        }

        // Fast-path select (no deletes) runs straight from the page ranges (RG-relative
        // `(start, len)`), so `IndexedStream` can build the parquet `RowSelection` without
        // re-walking the full candidate bitmap bit-by-bit in
        // `build_row_selection_with_min_skip_run` (the dominant cost on non-selective full
        // scans). Mirrors `universe_bitmap_from_page_ranges`' RG-relative offset math. `None`
        // page_ranges = whole RG = one run.
        let selection_runs: Vec<(usize, usize)> = match &page_ranges {
            Some(ranges) => ranges
                .iter()
                .map(|(r_min, r_max)| {
                    let lo = (*r_min as i64 - rg.first_row) as usize;
                    let len = (*r_max - *r_min) as usize;
                    (lo, len)
                })
                .collect(),
            None => vec![(0, rg.num_rows as usize)],
        };

        // No `mask_buffer`: with `needs_row_mask() == false` the stream never builds
        // `current_mask`, so the packed-bits buffer this evaluator used to pre-materialize would be
        // dead work. The residual in `on_batch_mask` (or parquet pushdown when row-granular) does
        // the filtering. Skipping `bitmap_to_packed_bits` here removes a full per-RG bit-iteration.
        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(()),
            mask_buffer: None,
            selection_runs: Some(selection_runs),
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
        // Residual predicate (e.g. RegionID = 229), if any. `CachedResidual` remaps the expression
        // to the delivered batch schema once and reuses it across batches.
        let residual_mask = match self.residual {
            Some(ref residual) => Some(residual.eval(batch, batch_len)?),
            None => None,
        };
        // Deleted-doc mask: the pre-built full-RG liveDocs BooleanArray (page-universe ∩ liveDocs),
        // sliced to this batch (zero-copy for the common identity position_map). The non-identity
        // (block-granular) fallback maps the candidate bitmap through the position map via
        // build_mask. This is the stream's own current_mask path — not a per-row contains().
        let live_slice: Option<BooleanArray> = if self.deleted_doc_filtering_required {
            match rg_state.downcast_ref::<DeleteRgState>() {
                Some(st) => match position_map {
                    // Identity (delivered idx == RG position): slice the pre-built RG mask (zero-copy).
                    PositionMap::Identity { .. } => Some(st.live_arr.slice(batch_offset, batch_len)),
                    // Non-identity (block-granular RowSelection): map via position_map, then slice.
                    _ => Some(build_mask(&st.candidates, position_map).slice(batch_offset, batch_len)),
                },
                None => None,
            }
        } else {
            None
        };
        match (residual_mask, live_slice) {
            (Some(r), Some(l)) => Ok(Some(
                datafusion::arrow::compute::kernels::boolean::and_kleene(&r, &l)
                    .map_err(|e| format!("delete AND residual: {e}"))?,
            )),
            (Some(r), None) => Ok(Some(r)),
            (None, Some(l)) => Ok(Some(l)),
            (None, None) => Ok(None),
        }
    }

    /// The candidate-stage `current_mask` is never consumed for this evaluator: `on_batch_mask`
    /// returns the exact mask (residual and/or liveDocs) and `finalize_batch` applies it
    /// EXCLUSIVELY (ignoring `current_mask`); when it returns `None` there is no page pruning, so
    /// the candidate universe has no gaps and no mask is needed. Returning `false` skips the per-RG
    /// `build_mask` over the full row group — pure waste here, since `on_batch_mask` already does
    /// the filtering.
    fn needs_row_mask(&self) -> bool {
        false
    }

    /// The delete path applies liveDocs by RG-relative position in `on_batch_mask`, so parquet
    /// pushdown must be OFF — it would drop rows mid-decode and misalign the pre-built liveDocs
    /// mask. The no-delete fast path leaves pushdown enabled (trait default `false`) so the
    /// residual is applied in lockstep with the decode.
    fn forbid_parquet_pushdown(&self) -> bool {
        self.deleted_doc_filtering_required
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
