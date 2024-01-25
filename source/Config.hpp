#pragma once

#include <gflags/gflags.h>

#include <string>

// Buffer management
DECLARE_uint32(page_size);
DECLARE_uint64(buffer_pool_size);
DECLARE_string(data_dir);

// Config for TransactionKV
DECLARE_bool(enable_fat_tuple);
DECLARE_bool(enable_long_running_transaction);

DECLARE_uint32(worker_threads);
DECLARE_bool(cpu_counters);
DECLARE_bool(enable_pin_worker_threads);
DECLARE_bool(smt);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(write_buffer_size);
DECLARE_uint32(pp_threads);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_bool(print_tx_console);
DECLARE_bool(profiling);
DECLARE_bool(profile_latency);
DECLARE_bool(crc_check);
DECLARE_uint32(print_debug_interval_s);

// -------------------------------------------------------------------------------------
DECLARE_bool(contention_split);
DECLARE_uint64(contention_split_sample_probability);
DECLARE_uint64(cm_period);
DECLARE_uint64(contention_split_threshold_pct);
// -------------------------------------------------------------------------------------
DECLARE_bool(xmerge);
DECLARE_uint64(xmerge_k);
DECLARE_double(xmerge_target_pct);
// -------------------------------------------------------------------------------------
DECLARE_bool(optimistic_scan);
DECLARE_bool(measure_time);
// -------------------------------------------------------------------------------------
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
DECLARE_uint64(warmup_for_seconds);

DECLARE_bool(btree_prefix_compression);
DECLARE_int64(btree_hints);
DECLARE_bool(btree_heads);
DECLARE_bool(bulk_insert);
// -------------------------------------------------------------------------------------
DECLARE_int64(trace_dt_id);
DECLARE_int64(trace_trigger_probability);
DECLARE_bool(pid_tracing);
DECLARE_string(tag);
// -------------------------------------------------------------------------------------
DECLARE_uint64(buffer_frame_recycle_batch_size);
DECLARE_bool(reclaim_page_ids);

// logging && recovery
DECLARE_bool(wal);
DECLARE_bool(wal_fsync);
DECLARE_uint64(wal_buffer_size);
DECLARE_bool(init);

// MVCC && GC
DECLARE_string(isolation_level);
DECLARE_bool(enable_garbage_collection);
DECLARE_bool(enable_eager_garbage_collection);

namespace leanstore {

extern std::string GetMetaFilePath();

/// DB file for pages and WALs
extern std::string GetDBFilePath();

extern std::string GetWALFilePath();

extern std::string GetLogDir();

} // namespace leanstore
