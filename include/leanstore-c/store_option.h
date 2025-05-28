#ifndef LEANSTORE_C_STORE_OPTION_H
#define LEANSTORE_C_STORE_OPTION_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// The log level
typedef enum LogLevel {
  kDebug = 0,
  kInfo,
  kWarn,
  kError,
} LogLevel;

/// The options for creating a new store.
typedef struct StoreOption {
  // ---------------------------------------------------------------------------
  // Store related options
  // ---------------------------------------------------------------------------

  /// Whether to create store from scratch.
  bool create_from_scratch_;

  /// The directory for all the database files.
  const char* store_dir_;

  // ---------------------------------------------------------------------------
  // log related options
  // ---------------------------------------------------------------------------

  /// The log level
  LogLevel log_level_;

  // ---------------------------------------------------------------------------
  // Worker thread related options
  // ---------------------------------------------------------------------------

  /// The number of worker threads.
  uint64_t worker_threads_;

  /// The WAL buffer size for each worker (bytes).
  uint64_t wal_buffer_size_;

  // ---------------------------------------------------------------------------
  // Buffer pool related options
  // ---------------------------------------------------------------------------

  /// The page size (bytes). For buffer manager.
  uint64_t page_size_;

  uint64_t buffer_frame_size_;

  /// The number of partitions. For buffer manager.
  uint32_t num_partitions_;

  /// The buffer pool size (bytes). For buffer manager.
  uint64_t buffer_pool_size_;

  /// The free percentage of the buffer pool. In the range of [0, 100].
  uint32_t free_pct_;

  /// The number of page evictor threads.
  uint32_t num_buffer_providers_;

  /// The async buffer
  uint32_t buffer_write_batch_size_;

  /// Whether to perform crc check for buffer frames.
  bool enable_buffer_crc_check_;

  /// BufferFrame recycle batch size. Everytime a batch of buffer frames is randomly picked and
  /// verified by page evictors, some of them are COOLed, some of them are EVICted.
  uint64_t buffer_frame_recycle_batch_size_;

  /// Whether to reclaim unused free page ids
  bool enable_reclaim_page_ids_;

  // ---------------------------------------------------------------------------
  // Logging and recovery related options
  // ---------------------------------------------------------------------------

  /// Whether to enable write-ahead log.
  bool enable_wal_;

  /// Whether to execute fsync after each WAL write.
  bool enable_wal_fsync_;

  // ---------------------------------------------------------------------------
  // Generic BTree related options
  // ---------------------------------------------------------------------------

  /// Whether to enable bulk insert.
  bool enable_bulk_insert_;

  /// Whether to enable X-Merge
  bool enable_xmerge_;

  /// The number of children to merge in X-Merge
  uint64_t xmerge_k_;

  /// The target percentage of the number of children to merge in X-Merge
  double xmerge_target_pct_;

  /// Whether to enable contention split.
  bool enable_contention_split_;

  /// Contention split probability, as exponent of 2
  uint64_t contention_split_probility_;

  /// Contention stats sample probability, as exponent of 2
  uint64_t contention_split_sample_probability_;

  /// Contention percentage to trigger the split, in the range of [0, 100].
  uint64_t contention_split_threshold_pct_;

  /// Whether to enable btree hints optimization. Available options:
  /// 0: disabled
  /// 1: serial
  /// 2: AVX512
  int64_t btree_hints_;

  /// Whether to enable heads optimization in LowerBound search.
  bool enable_head_optimization_;

  /// Whether to enable optimistic scan. Jump to next leaf directly if the pointer in the parent has
  /// not changed
  bool enable_optimistic_scan_;

  // ---------------------------------------------------------------------------
  // Transaction related options
  // ---------------------------------------------------------------------------

  /// Whether to enable long running transaction.
  bool enable_long_running_tx_;

  /// Whether to enable fat tuple.
  bool enable_fat_tuple_;

  /// Whether to enable garbage collection.
  bool enable_gc_;

  /// Whether to enable eager garbage collection. To enable eager garbage collection, the garbage
  /// collection must be enabled first. Once enabled, the garbage collection will be triggered after
  /// each transaction commit and abort.
  bool enable_eager_gc_;

  // ---------------------------------------------------------------------------
  // Metrics related options
  // ---------------------------------------------------------------------------

  /// Whether to enable cpu counters.
  bool enable_cpu_counters_;

  /// Whether to enable time measure.
  bool enable_time_measure_;

  /// Whether to enable perf events.
  bool enable_perf_events_;

} StoreOption;

/// Create a new store option.
/// @param storeDir the directory for all the database files. The string content is deep copied to
///                 the created store option.
StoreOption* CreateStoreOption(const char* store_dir);

/// Create a new store option from an existing store option.
/// @param storeDir the existing store option.
StoreOption* CreateStoreOptionFrom(const StoreOption* store_dir);

/// Destroy a store option.
/// @param option the store option to destroy.
void DestroyStoreOption(const StoreOption* option);

/// The options for creating a new BTree.
typedef struct BTreeConfig {
  /// Whether to enable write-ahead log.
  bool enable_wal_;

  /// Whether to enable bulk insert.
  bool use_bulk_insert_;

} BTreeConfig;

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_STORE_OPTION_H
