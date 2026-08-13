/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule that inserts LiveDocsFilterExec above DataSourceExec
//! when the shard has deleted documents. Preserves DataFusion's native predicate
//! pushdown — deleted rows are removed post-read.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfig;

use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::live_docs_filter::{LiveDocsFilterExec, SegmentLiveDocs};

/// Inserts `LiveDocsFilterExec` above parquet scans when deletions exist.
/// Carries per-file writer_generation mapping and context_id for getLiveDocs calls.
#[derive(Debug)]
pub struct LiveDocsFilterOptimizer {
    context_id: i64,
    /// file path (string) → writer_generation
    path_to_generation: HashMap<String, i64>,
}

impl LiveDocsFilterOptimizer {
    pub fn new(context_id: i64, path_to_generation: HashMap<String, i64>) -> Self {
        Self {
            context_id,
            path_to_generation,
        }
    }
}

impl PhysicalOptimizerRule for LiveDocsFilterOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let context_id = self.context_id;
        let path_map = &self.path_to_generation;

        let rewritten = plan.transform_up(|node| {
            let Some(dse) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(config) = dse.data_source().as_ref().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(node));
            };
            // Only handle parquet scans
            let is_parquet = (config.file_source().as_ref() as &dyn std::any::Any)
                .downcast_ref::<ParquetSource>()
                .is_some();
            if !is_parquet {
                return Ok(Transformed::no(node));
            }

            // Collect per-file liveDocs
            let mut segments: Vec<SegmentLiveDocs> = Vec::new();
            let mut has_any_deletions = false;

            for file_group in &config.file_groups {
                for pf in file_group.iter() {
                    let path_str = pf.object_meta.location.to_string();
                    let writer_gen = path_map.get(&path_str).copied().unwrap_or(0);

                    // Call getLiveDocs with i32::MAX — Java clamps to leaf.reader().maxDoc()
                    let bitset = match get_live_docs(context_id, writer_gen, 0, i32::MAX) {
                        Ok(Some(b)) => {
                            has_any_deletions = true;
                            let num_rows = (b.len() as u64) * 64; // approximate
                            segments.push(SegmentLiveDocs {
                                writer_generation: writer_gen,
                                num_rows,
                                bitset: Some(b),
                            });
                            continue;
                        }
                        Ok(None) => {
                            // All alive — use file size as approximate row count
                            segments.push(SegmentLiveDocs {
                                writer_generation: writer_gen,
                                num_rows: pf.object_meta.size as u64,
                                bitset: None,
                            });
                            continue;
                        }
                        Err(_) => {
                            // Error — treat as all alive
                            segments.push(SegmentLiveDocs {
                                writer_generation: writer_gen,
                                num_rows: pf.object_meta.size as u64,
                                bitset: None,
                            });
                            continue;
                        }
                    };
                }
            }

            if !has_any_deletions {
                return Ok(Transformed::no(node));
            }

            // Wrap in LiveDocsFilterExec
            let filtered = Arc::new(LiveDocsFilterExec::new(
                node,
                Arc::new(segments),
            ));
            Ok(Transformed::yes(filtered as Arc<dyn ExecutionPlan>))
        })?;

        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "LiveDocsFilterOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
