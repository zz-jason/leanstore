#ifndef LEANSTORE_COMMON_TYPES_H
#define LEANSTORE_COMMON_TYPES_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

//------------------------------------------------------------------------------
// Integer type aliases
//------------------------------------------------------------------------------

typedef uint64_t lean_treeid_t; // Tree ID
typedef uint64_t lean_pid_t;    // Page ID
typedef uint64_t lean_lid_t;    // Log ID
typedef uint64_t lean_txid_t;   // Transaction ID
typedef uint32_t lean_cmdid_t;  // Command ID
typedef uint16_t lean_wid_t;    // Worker ID

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------

struct lean_btree_config;
struct lean_store_option;

//------------------------------------------------------------------------------
// Enums
//------------------------------------------------------------------------------

/// BTree type.
typedef enum lean_btree_type {
  LEAN_BTREE_TYPE_ATOMIC = 0,
  LEAN_BTREE_TYPE_MVCC,
} lean_btree_type;

/// Log level.
typedef enum lean_log_level {
  LEAN_LOG_LEVEL_DEBUG = 0,
  LEAN_LOG_LEVEL_INFO,
  LEAN_LOG_LEVEL_WARN,
  LEAN_LOG_LEVEL_ERROR,
} lean_log_level;

//------------------------------------------------------------------------------
// BTree config
//------------------------------------------------------------------------------

/// The options for creating a new BTree.
typedef struct lean_btree_config {
  /// Whether to enable write-ahead log.
  bool enable_wal_;

  /// Whether to enable bulk insert.
  bool use_bulk_insert_;

} lean_btree_config;

//------------------------------------------------------------------------------
// Store option
//------------------------------------------------------------------------------

/// The options for creating a new store.
typedef struct lean_store_option {
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
  lean_log_level log_level_;

  // ---------------------------------------------------------------------------
  // Worker thread related options
  // ---------------------------------------------------------------------------

  /// The number of worker threads.
  uint64_t worker_threads_;

  /// Maximum number of concurrent transactions per worker thread, a hard limit.
  /// Also limits the maximum concurrent sessions in a leanstore worker thread.
  uint64_t max_concurrent_transaction_per_worker_;

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

  /// Size of the autonomous commit group.
  uint64_t commit_group_size_;

  /// The WAL buffer size for each worker (bytes).
  uint64_t wal_buffer_bytes_;

  /// The threshold (bytes) for flushing WAL entries to disk. Log buffer is flushed when
  /// the pending flush wal size reaches this threshold.
  uint32_t wal_flush_unit_bytes_;

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

} lean_store_option;

/// Create store option with the given store directory.
struct lean_store_option* lean_store_option_create(const char* store_dir);

/// Create store option by copying from another option.
struct lean_store_option* lean_store_option_create_from(const struct lean_store_option* other);

/// Destroy the store option.
void lean_store_option_destroy(const struct lean_store_option* option);

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif
