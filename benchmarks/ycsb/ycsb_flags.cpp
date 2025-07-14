#include <gflags/gflags.h>

// For the benchmark driver
DEFINE_string(ycsb_target, "leanstore",
              "Ycsb target, available: unordered_map, leanstore, rocksdb, leveldb");
DEFINE_string(ycsb_cmd, "run", "Ycsb command, available: run, load");
DEFINE_string(ycsb_workload, "a", "Ycsb workload, available: a, b, c, d, e, f");
DEFINE_uint32(ycsb_threads, 4, "Worker threads");
DEFINE_uint32(ycsb_clients, 0, "YCSB clients");
DEFINE_uint64(ycsb_mem_gb, 1, "Max memory in GB to use");
DEFINE_uint64(ycsb_run_for_seconds, 300, "Run the benchmark for x seconds");

// For the data preparation
DEFINE_string(ycsb_data_dir, "/tmp/ycsb", "Ycsb data dir");
DEFINE_uint64(ycsb_key_size, 16, "Key size in bytes");
DEFINE_uint64(ycsb_val_size, 120, "Value size in bytes");
DEFINE_uint64(ycsb_record_count, 10000, "The number of records to insert");
DEFINE_double(ycsb_zipf_factor, 0.99, "Zipf factor, 0 means uniform distribution");
