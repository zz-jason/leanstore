#include "leanstore-c/store_option.h"

#include <string.h>

/// The default store option.
static constexpr StoreOption kDefaultStoreOption = {
    // Store related options
    .create_from_scratch_ = true,
    .store_dir_ = "leanstore",

    // log related options
    .log_level_ = LogLevel::kInfo,

    // Worker thread related options
    .worker_threads_ = 4,
    .max_concurrent_transaction_per_worker_ = 1,

    // Buffer pool related options
    .page_size_ = 4 << 10,                // 4KB
    .buffer_frame_size_ = 512 + 4 * 1024, // 4KB + 512B
    .num_partitions_ = 64,
    .buffer_pool_size_ = 1ull << 30, // 1 GB
    .free_pct_ = 1,
    .num_buffer_providers_ = 1,
    .buffer_write_batch_size_ = 1024,
    .enable_buffer_crc_check_ = false,
    .buffer_frame_recycle_batch_size_ = 64,
    .enable_reclaim_page_ids_ = true,

    // Logging and recovery related options
    .enable_wal_ = true,
    .enable_wal_fsync_ = false,
    .commit_group_size_ = 4,
    .wal_buffer_bytes_ = 10u << 20,    // 10 MB
    .wal_flush_unit_bytes_ = 4u << 10, // 4KB

    // Generic BTree related options
    .enable_bulk_insert_ = false,
    .enable_xmerge_ = true,
    .xmerge_k_ = 5,
    .xmerge_target_pct_ = 80,
    .enable_contention_split_ = true,
    .contention_split_probility_ = 14,
    .contention_split_sample_probability_ = 7,
    .contention_split_threshold_pct_ = 1,
    .btree_hints_ = 1,
    .enable_head_optimization_ = true,
    .enable_optimistic_scan_ = true,

    // Transaction related options
    .enable_long_running_tx_ = true,
    .enable_fat_tuple_ = false,
    .enable_gc_ = true,
    .enable_eager_gc_ = false,

    // Metrics related options
    .enable_cpu_counters_ = true,
    .enable_time_measure_ = false,
    .enable_perf_events_ = false,
};

StoreOption* CreateStoreOption(const char* store_dir) {
  // create a new store option with default values
  StoreOption* option = new StoreOption();
  *option = kDefaultStoreOption;

  if (store_dir == nullptr) {
    store_dir = kDefaultStoreOption.store_dir_;
  }

  // override the default store directory
  char* store_dir_copy = new char[strlen(store_dir) + 1];
  memcpy(store_dir_copy, store_dir, strlen(store_dir));
  store_dir_copy[strlen(store_dir)] = '\0';
  option->store_dir_ = store_dir_copy;

  return option;
}

StoreOption* CreateStoreOptionFrom(const StoreOption* store_dir) {
  StoreOption* option = new StoreOption();
  *option = *store_dir;

  // deep copy the store directory
  char* store_dir_copy = new char[strlen(store_dir->store_dir_) + 1];
  memcpy(store_dir_copy, store_dir->store_dir_, strlen(store_dir->store_dir_));
  store_dir_copy[strlen(store_dir->store_dir_)] = '\0';
  option->store_dir_ = store_dir_copy;

  return option;
}

void DestroyStoreOption(const StoreOption* option) {
  if (option != nullptr) {
    if (option->store_dir_ != nullptr) {
      delete[] option->store_dir_;
    }
    delete option;
  }
}
