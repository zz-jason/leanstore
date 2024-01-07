#include <gflags/gflags.h>

// Buffer management
DEFINE_uint32(page_size, 4096, "The page size (bytes)"); // 4 KiB
DEFINE_uint64(buffer_pool_size, 1073741824,
              "The buffer pool size (bytes)"); // 1 GiB
DEFINE_string(data_dir, "~/.leanstore",
              "Where to put all the database files, meta file, and log files");
DEFINE_uint64(db_file_capacity, 1825361100800,
              "DB file capacity (bytes)"); // 1700 GB

// Config for multi-version, OLAP-isolated BTree
DEFINE_bool(enable_fat_tuple, false, "");
DEFINE_bool(enable_olap_mode, true,
            "Use OLAP mode for long running transactions");

DEFINE_uint32(db_file_prealloc_gib, 0, "Disk size to pre-allocate on DB file");
DEFINE_bool(recover, false, "");

DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 6, "bits per partition");
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
DEFINE_uint32(write_buffer_size, 1024, "");

DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, false, "");

// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_bool(print_tx_console, true, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
DEFINE_bool(profiling, false, "");
DEFINE_bool(profile_latency, false, "");
DEFINE_bool(crc_check, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
DEFINE_bool(cpu_counters, true,
            "Disable if HW does not have enough counters for all threads");
DEFINE_bool(enable_pin_worker_threads, false,
            "Whether to ping each worker thread to a specified CPU thread to "
            "get better CPU affinity and memory locality");
DEFINE_bool(smt, true, "Simultaneous multithreading");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(
    target_gib, 0.0,
    "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
DEFINE_uint64(warmup_for_seconds, 10, "Warmup for x seconds");

// -------------------------------------------------------------------------------------
DEFINE_bool(contention_split, true, "Whether contention split is enabled");
DEFINE_uint64(contention_split_sample_probability, 7,
              "Contention stats sample probability, as exponent of 2");
DEFINE_uint64(cm_period, 14, "Contention split probability, as exponent of 2");
DEFINE_uint64(contention_split_threshold_pct, 1,
              "Contention percentage to trigger the contention split");
// -------------------------------------------------------------------------------------
DEFINE_bool(xmerge, false, "");
DEFINE_uint64(xmerge_k, 5, "");
DEFINE_double(xmerge_target_pct, 80, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(
    optimistic_scan, true,
    "Jump to next leaf directly if the pointer in the parent has not changed");
DEFINE_bool(measure_time, false, "");
// -------------------------------------------------------------------------------------
DEFINE_double(tmp1, 0.0, "for ad-hoc experiments");
DEFINE_double(tmp2, 0.0, "");
DEFINE_double(tmp3, 0.0, "");
DEFINE_double(tmp4, 0.0, "");
DEFINE_double(tmp5, 0.0, "");
DEFINE_double(tmp6, 0.0, "");
DEFINE_double(tmp7, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(
    enable_print_btree_stats_on_exit, true,
    "Print BTree stats including name, hight, and num slots, etc. on exit");
DEFINE_bool(btree_prefix_compression, true, "");
DEFINE_bool(btree_heads, true,
            "Enable heads optimization in lowerBound search");
DEFINE_int64(btree_hints, 1, "0: disabled, 1: serial, 2: AVX512");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1,
             "Print a stack trace for page reads for this DT ID");
DEFINE_int64(trace_trigger_probability, 100, "");
DEFINE_bool(pid_tracing, false, "");
// -------------------------------------------------------------------------------------
DEFINE_string(tag, "",
              "Unique identifier for this, will be appended to each line csv");

// -----------------------------------------------------------------------------
// buffer manager, buffer frame provider
// -----------------------------------------------------------------------------
DEFINE_uint64(buffer_frame_recycle_batch_size, 64,
              "BufferFrame recycle batch size. Everytime a batch of buffer "
              "frames is randomly picked and verified by the buffer frame "
              "provider, some of them are COOLed, some of them are EVICted.");

DEFINE_bool(reclaim_page_ids, true, "Whether to reclaim unused free page ids");

// -------------------------------------------------------------------------------------
DEFINE_bool(wal, true, "Whether wal is enabled");
DEFINE_bool(wal_fsync, true, "Whether to explicitly flush wal to disk");

// WAL variant 0:
//   All workers submit their WAL entries to the the local ring buffer, the
//   group committer flushes the buffer periodically with RFA.
// WAL variant 1: ?
// WAL variant 2: ?
DEFINE_int64(wal_variant, 0, "Different WAL and group commit strategies");
DEFINE_uint64(wal_log_writers, 1, "");
DEFINE_uint64(wal_buffer_size, 1024 * 1024 * 10,
              "WAL buffer size for each worker (Bytes)");
// -------------------------------------------------------------------------------------
DEFINE_string(isolation_level, "si",
              "options: ru (READ_UNCOMMITTED), rc (READ_COMMITTED), si "
              "(SNAPSHOT_ISOLATION), ser (SERIALIZABLE)");
DEFINE_uint64(si_refresh_rate, 0, "");
DEFINE_bool(todo, true, "");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_bool(pgc, true, "Precise garbage collection/recycling");
DEFINE_uint64(pgc_variant, 0, "0 naive, 1 bit faster, 2 ...");
DEFINE_double(garbage_in_page_pct, 15,
              "Threshold to trigger page-wise garbage collection (%)");
DEFINE_uint64(todo_batch_size, 1024, "");
DEFINE_bool(history_tree_inserts, true, "");
// -------------------------------------------------------------------------------------

namespace leanstore {

std::string GetMetaFilePath() {
  return FLAGS_data_dir + "/meta.json";
}

std::string GetDBFilePath() {
  return FLAGS_data_dir + "/db.pages";
}

std::string GetWALFilePath() {
  return FLAGS_data_dir + "/db.wals";
}

std::string GetLogDir() {
  return FLAGS_data_dir + "/logs";
}

} // namespace leanstore
