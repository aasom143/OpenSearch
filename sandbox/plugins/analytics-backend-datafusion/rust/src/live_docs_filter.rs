/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Post-read liveDocs filter for the ListingTable path.
//!
//! Wraps a DataSourceExec and filters deleted rows from output batches.
//! DataFusion's native predicate pushdown works underneath — this only
//! removes the few deleted rows that survive. Zero overhead when no
//! deletions exist (getLiveDocs returns -2 for all segments).

use std::sync::Arc;
use std::task::{Context, Poll};
use std::pin::Pin;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;

use crate::indexed_table::ffm_callbacks::get_live_docs;

/// Holds per-segment liveDocs info for the filter.
#[derive(Debug)]
pub struct SegmentLiveDocs {
    pub writer_generation: i64,
    /// Total rows in this segment (from parquet metadata).
    pub num_rows: u64,
    /// The liveDocs bitset (None = all alive, Some = has deletions).
    pub bitset: Option<Vec<u64>>,
}

/// Physical plan node that filters deleted rows from a parquet scan.
/// Inserted by `LiveDocsFilterOptimizer` above `DataSourceExec` when
/// the shard has deletions.
#[derive(Debug)]
pub struct LiveDocsFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Per-file liveDocs, ordered to match the file_groups ordering
    /// of the underlying DataSourceExec.
    segments: Arc<Vec<SegmentLiveDocs>>,
    properties: Arc<PlanProperties>,
}

impl LiveDocsFilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, segments: Arc<Vec<SegmentLiveDocs>>) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            segments,
            properties,
        }
    }
}

impl DisplayAs for LiveDocsFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LiveDocsFilterExec: segments={}", self.segments.len())
    }
}

impl ExecutionPlan for LiveDocsFilterExec {
    fn name(&self) -> &str {
        "LiveDocsFilterExec"
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
        Ok(Arc::new(Self::new(
            children[0].clone(),
            Arc::clone(&self.segments),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = input_stream.schema();
        let segments = Arc::clone(&self.segments);

        // Build a combined liveDocs bitset for all segments (concatenated).
        // Row positions in the stream are absolute across all files in the partition.
        let combined_bitset = build_combined_bitset(&segments);

        let filtered_stream = LiveDocsFilterStream {
            input: input_stream,
            combined_bitset,
            row_offset: 0,
            schema: schema.clone(),
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::unfold(filtered_stream, |mut state| async move {
                match state.next_batch().await {
                    Some(Ok(batch)) => Some((Ok(batch), state)),
                    Some(Err(e)) => Some((Err(e), state)),
                    None => None,
                }
            }),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Combined liveDocs bitset across all segments in file order.
/// None = all alive (no filtering needed).
struct CombinedLiveDocs {
    bitset: Vec<u64>,
    total_rows: u64,
}

fn build_combined_bitset(segments: &[SegmentLiveDocs]) -> Option<CombinedLiveDocs> {
    let has_any_deletions = segments.iter().any(|s| s.bitset.is_some());
    if !has_any_deletions {
        return None;
    }

    let total_rows: u64 = segments.iter().map(|s| s.num_rows).sum();
    let total_words = ((total_rows + 63) / 64) as usize;
    let mut combined = vec![u64::MAX; total_words]; // start with all-ones

    let mut row_offset: u64 = 0;
    for seg in segments {
        if let Some(ref bitset) = seg.bitset {
            // Copy segment's liveDocs into the combined bitset at the correct offset
            for (i, &word) in bitset.iter().enumerate() {
                let abs_bit = row_offset + (i as u64 * 64);
                let combined_word_idx = (abs_bit / 64) as usize;
                let bit_offset = (abs_bit % 64) as u32;

                if bit_offset == 0 && combined_word_idx < combined.len() {
                    combined[combined_word_idx] = word;
                } else if combined_word_idx < combined.len() {
                    // Unaligned: need to split across two combined words
                    combined[combined_word_idx] &= !(u64::MAX << bit_offset) | (word << bit_offset);
                    if combined_word_idx + 1 < combined.len() {
                        combined[combined_word_idx + 1] &=
                            (u64::MAX << (64 - bit_offset).min(63)) | (word >> (64 - bit_offset));
                    }
                }
            }
        }
        // For segments without deletions, the combined bitset already has all-ones
        row_offset += seg.num_rows;
    }

    Some(CombinedLiveDocs {
        bitset: combined,
        total_rows,
    })
}

struct LiveDocsFilterStream {
    input: SendableRecordBatchStream,
    combined_bitset: Option<CombinedLiveDocs>,
    row_offset: u64,
    schema: SchemaRef,
}

impl LiveDocsFilterStream {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            let batch = match futures::StreamExt::next(&mut self.input).await {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };

            let num_rows = batch.num_rows();
            if num_rows == 0 {
                self.row_offset += num_rows as u64;
                continue;
            }

            let filtered = match &self.combined_bitset {
                None => {
                    // No deletions anywhere — pass through
                    self.row_offset += num_rows as u64;
                    batch
                }
                Some(live_docs) => {
                    // Build a boolean mask from the combined bitset for this batch's row range
                    let mut mask_values = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let abs_pos = self.row_offset + i as u64;
                        let word_idx = (abs_pos / 64) as usize;
                        let bit_idx = abs_pos % 64;
                        let alive = word_idx < live_docs.bitset.len()
                            && (live_docs.bitset[word_idx] >> bit_idx) & 1 == 1;
                        mask_values.push(alive);
                    }
                    self.row_offset += num_rows as u64;

                    let mask = BooleanArray::from(mask_values);
                    // If all alive in this batch, skip the filter
                    if mask.true_count() == num_rows {
                        batch
                    } else if mask.true_count() == 0 {
                        continue; // All deleted — skip batch
                    } else {
                        match filter_record_batch(&batch, &mask) {
                            Ok(filtered) => filtered,
                            Err(e) => {
                                return Some(Err(datafusion::error::DataFusionError::ArrowError(
                                    Box::new(e),
                                    None,
                                )))
                            }
                        }
                    }
                }
            };

            if filtered.num_rows() > 0 {
                return Some(Ok(filtered));
            }
        }
    }
}
