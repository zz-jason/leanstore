#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

static bool PageSizeValidator(const char* flagname, google::uint32 value) {
  google::uint32 kMaxPageSize = 4096;
  if (value > kMaxPageSize) {
    LOG(FATAL) << "Invalid value for --" << flagname << ": " << value
               << ". Must be <= " << kMaxPageSize << " Bytes";
    return false;
  }
  return true;
}

// Buffer management
DEFINE_uint32(page_size, 4096, "The page size (bytes)"); // 4 KiB
DEFINE_validator(page_size, &PageSizeValidator);
DEFINE_uint64(buffer_pool_size, 1073741824,
              "The buffer pool size (bytes)"); // 1 GiB
DEFINE_string(data_dir, "~/.leanstore",
              "Where to put all the database files, meta file, and log files");

// Config for TransactionKV
DEFINE_bool(enable_fat_tuple, false, "");
DEFINE_bool(enable_long_running_transaction, true,
            "For long running transactions");

DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 6, "bits per partition");
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
DEFINE_uint32(write_buffer_size, 1024, "");

DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, false, "");

// -------------------------------------------------------------------------------------
DEFINE_bool(profiling, false, "");
DEFINE_bool(profile_latency, false, "");
DEFINE_bool(crc_check, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
DEFINE_bool(cpu_counters, true,
            "Disable if HW does not have enough counters for all threads");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");

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

DEFINE_bool(btree_heads, true,
            "Enable heads optimization in lowerBound search");
DEFINE_int64(btree_hints, 1, "0: disabled, 1: serial, 2: AVX512");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");

// -----------------------------------------------------------------------------
// buffer manager, buffer frame provider
// -----------------------------------------------------------------------------
DEFINE_uint64(buffer_frame_recycle_batch_size, 64,
              "BufferFrame recycle batch size. Everytime a batch of buffer "
              "frames is randomly picked and verified by the buffer frame "
              "provider, some of them are COOLed, some of them are EVICted.");

DEFINE_bool(reclaim_page_ids, true, "Whether to reclaim unused free page ids");

// logging && recovery
DEFINE_bool(wal, true, "Whether wal is enabled");
DEFINE_bool(wal_fsync, true, "Whether to explicitly flush wal to disk");
DEFINE_uint64(wal_buffer_size, 1024 * 1024 * 10,
              "WAL buffer size for each worker (Bytes)");

DEFINE_bool(init, true, "When enabled, the store is initialized from scratch");

// MVCC && GC
DEFINE_string(isolation_level, "si",
              "options: si (Snapshot Isolation), ser (Serializable)");
DEFINE_bool(enable_garbage_collection, true,
            "Whether to enable garbage collection");
DEFINE_bool(enable_eager_garbage_collection, false,
            "When enabled, the global watermarks are updated after each "
            "transaction commit. Used for tests");

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
