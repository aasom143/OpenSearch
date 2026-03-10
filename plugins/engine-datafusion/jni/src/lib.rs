use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use jni::objects::{JByteArray, JClass, JMap, JObject};
use jni::objects::JLongArray;
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
use jni::{JNIEnv, JavaVM};
use std::sync::{Arc, OnceLock};
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::{
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    execution::context::SessionContext,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    execution::RecordBatchStream,
    prelude::*,
    DATAFUSION_VERSION,
};


use std::default::Default;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod util;
mod absolute_row_id_optimizer;
mod listing_table;
mod cache;
mod custom_cache_manager;
mod memory;
mod cross_rt_stream;
mod executor;
mod io;
mod runtime_manager;
mod cache_jni;
mod partial_agg_optimizer;
mod query_executor;
mod project_row_id_analyzer;
pub mod logger;
pub mod iceberg_integration;
pub mod s3_partition_downloader;

// Import logger macros from shared crate
use vectorized_exec_spi::{log_info, log_error, log_debug};

// Import query executor types
use crate::query_executor::{execute_query_with_cross_rt_stream, TableSource};

use crate::custom_cache_manager::CustomCacheManager;
use crate::util::{create_file_meta_from_filenames, parse_string_arr, set_action_listener_error, set_action_listener_error_global, set_action_listener_ok, set_action_listener_ok_global, set_action_listener_ok_global_with_map};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};

use crate::statistics_cache::CustomStatisticsCache;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use std::result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

// NativeBridge JNI implementations
use jni::objects::{JObjectArray, JString};
use log::error;
use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;
use crate::cross_rt_stream::CrossRtStream;
use crate::memory::{Monitor, MonitoredMemoryPool};
use crate::runtime_manager::RuntimeManager;

mod statistics_cache;
mod eviction_policy;

struct DataFusionRuntime {
    runtime_env: RuntimeEnv,
    custom_cache_manager: Option<CustomCacheManager>,
    monitor: Arc<Monitor>,
}

// TASK monitorint metrics
static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(50)).clone()
});

// Global runtime manager
static TOKIO_RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();

// Global JavaVM reference
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

// Helper function to get or attach JNI env
fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let env = jvm.attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");
            *opt = Some(env);
        }

        // Safe because we're the only one with access to this thread-local
        let env_ref = opt.as_mut().unwrap();
        f(env_ref)
    })
}

/// Initialize the logger for Rust->Java logging bridge.
/// This should be called once when the native library is loaded.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger with the JVM for Rust->Java logging bridge
    // This uses the shared logger from vectorized_exec_spi
    // The logger stores its own JVM reference internally
    vectorized_exec_spi::logger::init_logger_from_env(&env);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    // Initialize JavaVM for async callbacks from Tokio worker threads
    // This is needed so worker threads can attach to JVM and call ActionListener methods
    JAVA_VM.get_or_init(|| {
        env.get_java_vm().expect("Failed to get JavaVM")
    });

    TOKIO_RUNTIME_MANAGER.get_or_init(|| {
        log_info!("[FLOW] Runtime manager initialized with {} CPU threads", cpu_threads);
        Arc::new(RuntimeManager::new(cpu_threads as usize))
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
) {
    log_info!("Runtime manager shut down started");
    if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
        mgr.shutdown();
        log_info!("Runtime manager shut down successfully");
    }
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_startTokioRuntimeMonitoring(
    _env: JNIEnv,
    _class: JClass,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Tokio runtime manager not initialized");
            return;
        }
    };

    // Uncomment this to monitor tokio metrics

    // let io_runtime = manager.io_runtime.clone();
    // io_runtime.spawn(async move {
    //     let handle = tokio::runtime::Handle::current();
    //     let runtime_monitor = RuntimeMonitor::new(&handle);
    //
    //     // Monitor at 120-second intervals
    //     for metrics in runtime_monitor.intervals() {
    //         log_runtime_metrics(&metrics);
    //         tokio::time::sleep(Duration::from_secs(120)).await;
    //     }
    // });
    //
    // println!("Runtime monitoring started");
}

/// Log runtime metrics with performance analysis
#[allow(dead_code)]
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    log_info!("=== Runtime Metrics ===");
    log_info!("  Workers: {}", metrics.workers_count);
    log_info!("  Global queue depth: {}", metrics.global_queue_depth);
    /*
    //unstable tokio causes build failures, uncomment this when monitoring

    log_info!("  Worker overflow: {}", metrics.total_overflow_count);
    log_info!("  Remote schedule: {}", metrics.max_local_schedule_count);
    log_info!("  Worker steal ops: {}", metrics.total_steal_operations);
    log_info!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
    log_info!("  Max local queue depth: {}", metrics.max_local_queue_depth);
    log_info!("  Min local queue depth: {}", metrics.min_local_queue_depth);
    log_info!("  Max local schedule count: {}", metrics.max_local_schedule_count);
    log_info!("  Min local schedule count: {}", metrics.min_local_schedule_count);
    log_info!("  Queue depth: {}", metrics.total_local_queue_depth);
    log_info!("  Total schedule count: {}", metrics.total_local_schedule_count);
    */
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    log_info!("======================");
}

/// Log task metrics with performance analysis
#[allow(dead_code)]
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    log_info!("=== Task Metrics: {} ===", operation);
    log_info!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    log_info!("  Poll duration: {:?}", metrics.total_poll_duration);
    log_info!("  Idle duration: {:?}", metrics.total_idle_duration);
    log_info!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    log_info!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    log_info!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    log_info!("  Total slow polls: {}", metrics.total_slow_poll_count);
    log_info!("  Total long delays: {}", metrics.total_long_delay_count);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong
) -> jlong {
    log_info!("[FLOW] createGlobalRuntime: memoryLimit={}, spillLimit={}", memory_pool_limit, spill_limit);
    let spill_dir: String = match env.get_string(&spill_dir) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let mut builder = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64);
    log_info!("Spill Limit is being set to : {}", spill_limit);
    let builder = builder.with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    let monitor = Arc::new(Monitor::default());
    let memory_pool = Arc::new(MonitoredMemoryPool::new(
        Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        )),
        monitor.clone(),
    ));

    let (cache_manager_config, custom_cache_manager) = match cache_manager_ptr {
        0 => {
            (CacheManagerConfig::default(), None)
        }
        _ => {
            let custom_cache_manager = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
            (custom_cache_manager.build_cache_manager_config(), Some(custom_cache_manager))
        }
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_manager_config)
        .with_memory_pool(memory_pool.clone())
        .with_disk_manager_builder(builder)
        .build().unwrap();

    let runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager,
        monitor,
    };

    let ptr = Box::into_raw(Box::new(runtime)) as jlong;
    log_info!("[FLOW] Global runtime created: ptr={}", ptr);
    ptr
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut DataFusionRuntime) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    if runtime_id == 0 {
        return 0;
    }
    let runtime_env = unsafe { &*(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtime_env.clone()));
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(
        r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#,
        DATAFUSION_VERSION
    );
    env.new_string(version_info)
        .expect("Couldn't create Java string")
        .as_raw()
}



#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    log_info!("[FLOW] createDatafusionReader called");
    let table_path: String = match env.get_string(&table_path) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let mut files: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(files) => files,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid file list: {}", e),
            );
            return 0;
        }
    };

    log_info!("[FLOW] Creating reader: tablePath={}, fileCount={}, files={:?}", table_path, files.len(), files);
    // TODO: This works since files are named similarly ending with incremental generation count, preferably move this up to DatafusionReaderManager to keep file order
    files.sort();
    let files_metadata = match create_file_meta_from_filenames(&table_path, files.clone()) {
        Ok(metadata) => metadata,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create metadata: {}", err),
            );
            return 0;
        }
    };

    let table_url = match ListingTableUrl::parse(&table_path) {
        Ok(url) => url,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Invalid table path: {}", err),
            );
            return 0;
        }
    };

    log_info!("[FLOW] File metadata created: count={}", files_metadata.len());
    for (idx, meta) in files_metadata.iter().enumerate() {
        let total_rows: i64 = meta.row_group_row_counts().iter().sum();
        log_info!("[FLOW]   File[{}]: path={}, rowBase={}, totalRows={}, rowGroups={:?}",
            idx, meta.object_meta().location, *meta.row_base(), total_rows, *meta.row_group_row_counts());
    }

    let shard_view = ShardView::new(table_url, files_metadata);

    let ptr = Box::into_raw(Box::new(shard_view)) as jlong;
    log_info!("[FLOW] DatafusionReader created: ptr={}, fileCount={}", ptr, files.len());
    ptr
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeDatafusionReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_destroyTokioRuntime(
    mut env: JNIEnv,
    _class: JClass,
    tokio_runtime_ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(tokio_runtime_ptr as *mut Runtime) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_metadata: Vec<CustomFileMeta>) -> Self {
        let files_metadata = Arc::new(files_metadata);
        ShardView {
            table_path,
            files_metadata,
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_metadata(&self) -> Arc<Vec<CustomFileMeta>> {
        self.files_metadata.clone()
    }
}

#[derive(Debug, Clone)]
struct CustomFileMeta {
    row_group_row_counts: Arc<Vec<i64>>,
    row_base: Arc<i64>,
    object_meta: Arc<ObjectMeta>,
}

impl CustomFileMeta {
    pub fn new(row_group_row_counts: Vec<i64>, row_base: i64, object_meta: ObjectMeta) -> Self {
        let row_group_row_counts = Arc::new(row_group_row_counts);
        let row_base = Arc::new(row_base);
        let object_meta = Arc::new(object_meta);
        CustomFileMeta {
            row_group_row_counts,
            row_base,
            object_meta,
        }
    }

    pub fn row_group_row_counts(&self) -> Arc<Vec<i64>> {
        self.row_group_row_counts.clone()
    }

    pub fn row_base(&self) -> Arc<i64> {
        self.row_base.clone()
    }

    pub fn object_meta(&self) -> Arc<ObjectMeta> {
        self.object_meta.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    /// Total file size in bytes
    pub size: u64,

    /// Total number of rows in the file
    pub num_rows: i64,
}

impl FileStats {
    pub fn new(size: u64, num_rows: i64) -> Self {
        Self { size, num_rows }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhaseAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    is_query_plan_explain_enabled: jboolean,
    runtime_ptr: jlong,
    listener: JObject,
) {
    log_info!("[FLOW] executeQueryPhaseAsync: shardViewPtr={}, runtimePtr={}", shard_view_ptr, runtime_ptr);
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // ===== EXTRACT ALL JAVA DATA BEFORE ASYNC BLOCK =====
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get table name: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    log_info!("[FLOW] Query phase: tableName={}, explainEnabled={}", table_name, is_query_plan_explain_enabled != 0);
    let is_query_plan_explain_enabled: bool = is_query_plan_explain_enabled !=0;

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            log_error!("Failed to convert plan bytes: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };
    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    // Note: shard_view_ptr is still passed from Java but will be ignored
    // We're using hardcoded Iceberg S3 path instead
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    io_runtime.block_on(async move {

        // HARDCODED: Using Iceberg metadata from S3 instead of local file listings
        // TODO: Make this configurable via Java parameters
        // let hardcoded_s3_metadata_path = "s3://guptasom-iceberg-test/os-warehouse/opensearch/index-guptasom-1/metadata/custom_metadata.json".to_string();

        // HARDCODED: S3 options with region
        // NOTE: Due to iceberg-datafusion limitation, these options are NOT actually used
        // We're setting environment variables programmatically as a workaround
        let mut s3_options = HashMap::new();
        s3_options.insert("aws.region".to_string(), "us-west-2".to_string());

        // WORKAROUND: Set environment variables programmatically
        // This ensures the region is available even if not set externally
        std::env::set_var("AWS_REGION", "us-west-2");

        // log_info!("[FLOW] Using hardcoded Iceberg S3 path: {}", hardcoded_s3_metadata_path);
        log_info!("[FLOW] Set AWS_REGION environment variable programmatically: us-west-2");
        log_info!("[FLOW] Note: ShardView parameters are being ignored for now");

//         let result = query_executor::execute_query_with_iceberg_from_s3(
//             hardcoded_s3_metadata_path,
//             table_name,
//             plan_bytes_vec,
//             is_query_plan_explain_enabled,
//             runtime,
//             cpu_executor,
//             Some(s3_options),  // Pass region configuration (currently not used by iceberg-datafusion)
//         ).await;

//         // Local files (existing approach)
//         execute_query_with_cross_rt_stream(
//             TableSource::LocalFiles { table_path, files_meta },
//             table_name,
//             plan_bytes,
//             false,
//             runtime,
//             executor,
//         ).await?;
        // Iceberg S3 metadata file execution
//         let result = execute_query_with_cross_rt_stream(
//                 TableSource::IcebergS3 {
//                     s3_metadata_path: hardcoded_s3_metadata_path,
//                     s3_options: Some(s3_options),
//                 },
//                 table_name,
//                 plan_bytes_vec,
//                 is_query_plan_explain_enabled,
//                 runtime,
//                 cpu_executor,
//             ).await;

        // Iceberg from Glue (your use case!)
//         let result = execute_query_with_cross_rt_stream(
//             TableSource::IcebergGlue {
//                 database_name: "opensearch".to_string(),
//                 warehouse_location: "s3://guptasom-iceberg-test/os-warehouse".to_string(),
//                 glue_options: None,
//             },
//             table_name,
//             plan_bytes_vec,
//             false,
//             runtime,
//             cpu_executor,
//         ).await;

        // Extract shard_id from table_name
        // Expected format: {index_name}_{shard_id} where shard_id is 0, 1, 2, etc.
        let shard_id = table_name.rsplit('_')
            .next()
            .and_then(|s| s.parse::<u32>().ok())
            .map(|id| id.to_string());

        log_info!("[FLOW] Extracted shard_id from table_name '{}': {:?}", table_name, shard_id);

        let result = execute_query_with_cross_rt_stream(
            TableSource::IcebergS3Tables {
                table_bucket_arn: "arn:aws:s3tables:us-west-2:339712837375:bucket/srirasac-test".to_string(),
                database_name: "opensearch".to_string(),
                s3tables_options: Some(s3_options),
                shard_id,
            },
            table_name,
            plan_bytes_vec,
            is_query_plan_explain_enabled,
            runtime,
            cpu_executor,
        ).await;

        match result {
            Ok(stream_ptr) => {
                log_info!("[FLOW] Query execution completed: streamPtr={}", stream_ptr);
                with_jni_env(|env| {
                    set_action_listener_ok_global(env, &listener_ref, stream_ptr);
                });
            }
            Err(e) => {
                with_jni_env(|env| {
                    log_error!("Query execution failed: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                });
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_fetchSegmentStats(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };
    let io_runtime = manager.io_runtime.clone();

    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let files_meta = shard_view.files_metadata();
    log_info!("[FLOW] fetchSegmentStats: fileCount={}", files_meta.len());

    io_runtime.block_on(async move {
        let file_stats = util::fetch_segment_statistics(files_meta).await;
        match file_stats {
            Ok(map) => {
                with_jni_env(|env| {
                    set_action_listener_ok_global_with_map(env, &listener_ref, &map);
                });
            }
            Err(e) => {
                with_jni_env(|env| {
                    log_error!("Collecting file stats failed: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                });
            }
        }
    });
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    listener: JObject,
) {
    log_info!("[FLOW] streamNext called: streamPtr={}", stream);
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            set_action_listener_error(
                &mut env,
                listener,
                &DataFusionError::Execution("Runtime manager not initialized".to_string())
            );
            return;
        }
    };

    // Convert listener to GlobalRef
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let stream_ptr = stream;
    let io_runtime = manager.io_runtime.clone();

    // TODO : this can be 'io_runtime.block_on' if we see rust workers getting overloaded
    // benchmarks so far are good with spawn
    // TODO : Thread leaks in tests if its spawn
    io_runtime.block_on(async move {

        let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
        // Poll the stream with monitoring
        let result = stream.try_next().await;

        // Uncomment for monitoring stream next
        // let result = STREAM_NEXT_MONITOR.instrument(async {
        //         stream.try_next().await
        // }).await;

        // Use thread-local JNI env - auto-attaches!
        with_jni_env(|env| {
            match result {
                Ok(Some(batch)) => {
                    // Convert to FFI
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    let ffi_array_ptr = Box::into_raw(Box::new(ffi_array));
                    set_action_listener_ok_global(env, &listener_ref, ffi_array_ptr as jlong);
                }
                Ok(None) => {
                    // End of stream
                    set_action_listener_ok_global(env, &listener_ref, 0);
                }
                Err(err) => {
                    log_error!("Stream next failed: {}", err);
                    set_action_listener_error_global(env, &listener_ref, &err);
                }
            }
        });
    });
    // Function returns immediately to java - async rust work continues in background
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    listener: JObject,
) {
    if stream_ptr == 0 {
        set_action_listener_error(
            &mut env,
            listener,
            &DataFusionError::Execution("Invalid stream pointer".to_string())
        );
        return;
    }
    // Schema access is synchronous and fast - no need for runtime
    let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
    //let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };

    let schema = stream.schema();
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(mut ffi_schema) => {
            set_action_listener_ok(&mut env, listener, addr_of_mut!(ffi_schema) as jlong);
        }
        Err(err) => {
            set_action_listener_error(&mut env, listener, &DataFusionError::Execution(
                format!("Schema conversion failed: {}", err)
            ));
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeFetchPhase(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    values: JLongArray,
    include_fields: JObjectArray,
    exclude_fields: JObjectArray,
    runtime_ptr: jlong,
    callback: JObject,
) -> jlong {
    log_info!("[FLOW] executeFetchPhase: shardViewPtr={}, runtimePtr={}", shard_view_ptr, runtime_ptr);
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_metadata = shard_view.files_metadata();

    let include_fields: Vec<String> =
        parse_string_arr(&mut env, include_fields).expect("Expected list of files");
    let exclude_fields: Vec<String> =
        parse_string_arr(&mut env, exclude_fields).expect("Expected list of files");

    // Safety checks first
    if values.is_null() {
        let _ = env.throw_new("java/lang/NullPointerException", "values array is null");
        return 0;
    }

    // Get array length
    let array_length = match env.get_array_length(&values) {
        Ok(len) => len,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array length: {:?}", e),
            );
            return 0;
        }
    };

    // Allocate Rust buffer
    let mut row_ids: Vec<jlong> = vec![0; array_length as usize];

    // Copy Java array into Rust buffer
    match env.get_long_array_region(values, 0, &mut row_ids[..]) {
        Ok(_) => {
            log_info!("[FLOW] Fetch phase: rowIdCount={}", row_ids.len());
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array data: {:?}", e),
            );
            return 0;
        }
    }

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return 0;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    io_runtime.block_on(async {
        match query_executor::execute_fetch_phase(
            table_path,
            files_metadata,
            row_ids,
            include_fields,
            exclude_fields,
            runtime,
            cpu_executor,
        ).await {
            Ok(stream_ptr) => stream_ptr,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to execute fetch phase: {}", e),
                );
                0 // return 0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
) {
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}

/// JNI function to execute query with downloaded S3 partition files.
/// Execute a query with S3 partition direct access (no download).
///
/// This function creates a ListingTable that reads directly from S3 partition files.
/// DataFusion's object store integration handles S3 access transparently.
///
/// # Parameters
/// * `local_dir` - UNUSED (kept for API compatibility)
/// * `table_bucket_arn` - S3 Tables bucket ARN
/// * `database_name` - Database/namespace name
/// * `table_name` - Table name (also used for registration in DataFusion context)
/// * `partition_column` - Partition column name (e.g., "shard_id")
/// * `partition_value` - Partition value to filter (e.g., "0")
/// * `substrait_bytes` - Serialized Substrait query plan
/// * `is_query_plan_explain_enabled` - Enable query plan explanation
/// * `runtime_ptr` - Pointer to DataFusion runtime
/// * `listener` - Java ActionListener for async result callback
#[no_mangle]
/// JNI function to execute query with downloaded S3 partition files.
/// Downloads partition files from S3 Tables to local directory, then queries using ListingTable.
///
/// # Parameters
/// * `local_dir` - Local directory to download files to
/// * `table_bucket_arn` - S3 Tables bucket ARN
/// * `database_name` - Database/namespace name
/// * `table_name` - Table name (also used for registration in DataFusion context)
/// * `partition_column` - Partition column name (e.g., "shard_id")
/// * `partition_value` - Partition value to filter (e.g., "0")
/// * `s3_options` - Java Map containing S3 credentials (access key, secret key, session token, region)
/// * `substrait_bytes` - Serialized Substrait query plan
/// * `is_query_plan_explain_enabled` - Enable query plan explanation
/// * `runtime_ptr` - Pointer to DataFusion runtime
/// * `listener` - Java ActionListener for async result callback
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryWithDownloadedPartitionAsync(
    mut env: JNIEnv,
    _class: JClass,
    local_dir: JString,
    table_bucket_arn: JString,
    database_name: JString,
    table_name: JString,
    partition_column: JString,
    partition_value: JString,
    s3_options_map: JObject,
    substrait_bytes: jbyteArray,
    is_query_plan_explain_enabled: jboolean,
    runtime_ptr: jlong,
    listener: JObject,
) {
    log_info!("[FLOW] executeQueryWithDownloadedPartitionAsync: runtimePtr={}", runtime_ptr);

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Extract all Java parameters before async block
    let local_dir: String = match env.get_string(&local_dir) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get local_dir: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get local_dir: {}", e)));
            return;
        }
    };

    let table_bucket_arn: String = match env.get_string(&table_bucket_arn) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get table_bucket_arn: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get table_bucket_arn: {}", e)));
            return;
        }
    };

    let database_name: String = match env.get_string(&database_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get database_name: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get database_name: {}", e)));
            return;
        }
    };

    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get table_name: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get table_name: {}", e)));
            return;
        }
    };

    let partition_column: String = match env.get_string(&partition_column) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get partition_column: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get partition_column: {}", e)));
            return;
        }
    };

    let partition_value: String = match env.get_string(&partition_value) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get partition_value: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get partition_value: {}", e)));
            return;
        }
    };

    log_info!(
        "[FLOW] Downloaded partition query: table={}.{}, partition={}={}, local_dir={}",
        database_name, table_name, partition_column, partition_value, local_dir
    );

    // Extract s3Options from Java Map if provided
    let mut s3_options = HashMap::new();
    if !s3_options_map.is_null() {
        let map = JMap::from_env(&mut env, &s3_options_map).expect("Failed to convert to JMap");
        let mut iter = map.iter(&mut env).expect("Failed to get map iterator");

        while let Some((key, value)) = iter.next(&mut env).expect("Failed to iterate map") {
            let key_str: String = env.get_string(&JString::from(key)).expect("Failed to get key").into();
            let value_str: String = env.get_string(&JString::from(value)).expect("Failed to get value").into();
            s3_options.insert(key_str, value_str);
        }

        log_info!("[FLOW] Received {} s3Options from Java", s3_options.len());
    }

    let is_query_plan_explain_enabled: bool = is_query_plan_explain_enabled != 0;

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            log_error!("Failed to convert plan bytes: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    io_runtime.block_on(async move {
        // First, download the partition files
        use crate::s3_partition_downloader::{S3PartitionDownloadConfig, download_s3_partition_files};

        let download_config = S3PartitionDownloadConfig::new(
            local_dir.clone(),
            table_bucket_arn,
            database_name,
            table_name.clone(),
            partition_column,
            partition_value,
        ).with_s3_options(s3_options);

        // Download files
        let download_result = download_s3_partition_files(&download_config).await;

        match download_result {
            Ok(files) => {
                log_info!("[FLOW] Downloaded {} files to {}", files.len(), local_dir);

                // Now execute query with downloaded files
                let result = execute_query_with_cross_rt_stream(
                    TableSource::DownloadedPartition {
                        local_dir,
                    },
                    table_name,
                    plan_bytes_vec,
                    is_query_plan_explain_enabled,
                    runtime,
                    cpu_executor,
                ).await;

                match result {
                    Ok(stream_ptr) => {
                        log_info!("[FLOW] Downloaded partition query execution completed: streamPtr={}", stream_ptr);
                        with_jni_env(|env| {
                            set_action_listener_ok_global(env, &listener_ref, stream_ptr);
                        });
                    }
                    Err(e) => {
                        with_jni_env(|env| {
                            log_error!("Downloaded partition query execution failed: {}", e);
                            set_action_listener_error_global(env, &listener_ref, &e);
                        });
                    }
                }
            }
            Err(e) => {
                with_jni_env(|env| {
                    log_error!("Failed to download partition files: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                });
            }
        }
    });
}

