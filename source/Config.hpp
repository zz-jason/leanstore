#pragma once

#include <gflags/gflags.h>

#include <string>

// Buffer management
DECLARE_uint32(page_size);
DECLARE_uint64(buffer_pool_size);
DECLARE_string(data_dir);
DECLARE_uint64(db_file_capacity);

DECLARE_uint32(worker_threads);
DECLARE_bool(cpu_counters);
DECLARE_bool(enable_pin_worker_threads);
DECLARE_bool(smt);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(write_buffer_size);
DECLARE_uint32(db_file_prealloc_gib);
DECLARE_uint32(pp_threads);
DECLARE_bool(worker_page_eviction);
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
DECLARE_string(zipf_path);
DECLARE_double(zipf_factor);
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
DECLARE_uint64(warmup_for_seconds);
// -------------------------------------------------------------------------------------
DECLARE_double(tmp1);
DECLARE_double(tmp2);
DECLARE_double(tmp3);
DECLARE_double(tmp4);
DECLARE_double(tmp5);
DECLARE_double(tmp6);
DECLARE_double(tmp7);
// -------------------------------------------------------------------------------------
DECLARE_bool(enable_print_btree_stats_on_exit);
DECLARE_bool(btree_prefix_compression);
DECLARE_int64(btree_hints);
DECLARE_bool(btree_heads);
DECLARE_bool(nc_reallocation);
DECLARE_bool(bulk_insert);
// -------------------------------------------------------------------------------------
DECLARE_int64(trace_dt_id);
DECLARE_int64(trace_trigger_probability);
DECLARE_bool(pid_tracing);
DECLARE_string(tag);
// -------------------------------------------------------------------------------------
DECLARE_uint64(buffer_frame_recycle_batch_size);
DECLARE_bool(reclaim_page_ids);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal);
DECLARE_bool(wal_rfa);
DECLARE_bool(wal_tuple_rfa);
DECLARE_bool(wal_fsync);
DECLARE_int64(wal_variant);
DECLARE_uint64(wal_log_writers);
DECLARE_uint64(wal_buffer_size);
// -------------------------------------------------------------------------------------
DECLARE_string(isolation_level);
DECLARE_bool(mv);
DECLARE_uint64(si_refresh_rate);
DECLARE_bool(todo);
// -------------------------------------------------------------------------------------
DECLARE_bool(vi);
DECLARE_bool(vi_delta);
DECLARE_bool(vi_utodo);
DECLARE_bool(vi_rtodo);
DECLARE_bool(vi_flookup);
DECLARE_bool(vi_fremove);
DECLARE_bool(vi_update_version_elision);
DECLARE_bool(vi_fupdate_chained);
DECLARE_bool(vi_fupdate_fat_tuple);
DECLARE_uint64(vi_fat_tuple_trigger);
DECLARE_bool(vi_fat_tuple_alternative);
DECLARE_uint64(vi_pgc_batch_size);
DECLARE_bool(vi_fat_tuple);
DECLARE_string(vi_fat_tuple_dts);
DECLARE_bool(vi_dangling_pointer);
DECLARE_bool(vi_fat_tuple_decompose);
// -------------------------------------------------------------------------------------
DECLARE_bool(olap_mode);
DECLARE_bool(graveyard);
// -------------------------------------------------------------------------------------
DECLARE_bool(pgc);
DECLARE_uint64(pgc_variant);
DECLARE_double(garbage_in_page_pct);
DECLARE_uint64(vi_max_chain_length);
DECLARE_uint64(todo_batch_size);
DECLARE_bool(history_tree_inserts);
// -------------------------------------------------------------------------------------
DECLARE_bool(recover);

namespace leanstore {

extern std::string GetMetaFilePath();

/// DB file for pages and WALs
extern std::string GetDBFilePath();

extern std::string GetWALFilePath();

extern std::string GetLogDir();

} // namespace leanstore
