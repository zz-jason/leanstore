#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/parallelize.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"
#include "ycsb.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <rocksdb/db.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

namespace leanstore::ycsb {

class YcsbRocksDb : public YcsbExecutor {

private:
  rocksdb::DB* db_ = nullptr;

public:
  YcsbRocksDb() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.arena_block_size = FLAGS_ycsb_mem_gb << 30;

    auto status =
        rocksdb::DB::Open(options, FLAGS_ycsb_data_dir + "/rocksdb/" + FLAGS_ycsb_workload, &db_);
    if (!status.ok()) {
      Log::Fatal("Failed to open rocksdb: {}", status.ToString());
    }
  }

  void HandleCmdLoad() override {
    // load data with FLAGS_ycsb_threads
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values" << std::endl;
    SCOPED_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      std::cout << "Done inserting" << ", time elapsed: " << duration / 1000000.0 << " seconds"
                << ", throughput: " << CalculateTps(start, end, FLAGS_ycsb_record_count) << " tps"
                << std::endl;
    });

    utils::Parallelize::Range(
        FLAGS_ycsb_threads, FLAGS_ycsb_record_count,
        [&](uint64_t thread_id [[maybe_unused]], uint64_t begin, uint64_t end) {
          uint8_t key[FLAGS_ycsb_key_size];
          uint8_t val[FLAGS_ycsb_val_size];
          rocksdb::WriteOptions write_option;

          for (uint64_t i = begin; i < end; i++) {
            // generate key-value for insert
            GenKey(i, key);
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            auto status = db_->Put(write_option, rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                                   rocksdb::Slice((char*)val, FLAGS_ycsb_val_size));
            if (!status.ok()) {
              Log::Fatal("Failed to insert: {}", status.ToString());
            }
          }
        });
  }

  void HandleCmdRun() override {
    // Run the benchmark in FLAGS_ycsb_threads
    auto workload_type = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workload_type);
    auto zipf_random =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keep_running = true;
    std::vector<std::atomic<uint64_t>> thread_committed(FLAGS_ycsb_threads);
    std::vector<std::atomic<uint64_t>> thread_aborted(FLAGS_ycsb_threads);

    std::vector<std::thread> threads;
    for (uint64_t worker_id = 0; worker_id < FLAGS_ycsb_threads; worker_id++) {
      threads.emplace_back(
          [&](uint64_t thread_id) {
            uint8_t key[FLAGS_ycsb_key_size];
            std::string val_read;
            while (keep_running) {
              switch (workload_type) {
              case Workload::kA:
              case Workload::kB:
              case Workload::kC: {
                auto read_probability = utils::RandomGenerator::Rand(0, 100);
                if (read_probability <= workload.read_proportion_ * 100) {
                  // generate key for read
                  GenYcsbKey(zipf_random, key);
                  auto status =
                      db_->Get(rocksdb::ReadOptions(),
                               rocksdb::Slice((char*)key, FLAGS_ycsb_key_size), &val_read);
                  if (!status.ok()) {
                    Log::Fatal("Failed to read: {}", status.ToString());
                  }
                } else {
                  // generate key for update
                  GenYcsbKey(zipf_random, key);
                  // generate val for update
                  auto status = db_->Put(rocksdb::WriteOptions(),
                                         rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                                         rocksdb::Slice((char*)key, FLAGS_ycsb_val_size));
                  if (!status.ok()) {
                    thread_aborted[thread_id]++;
                  }
                }
                break;
              }
              default: {
                Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workload_type));
              }
              }
              thread_committed[thread_id]++;
            }
          },
          worker_id);
    }

    // init counters
    for (auto& c : thread_committed) {
      c = 0;
    }
    for (auto& a : thread_aborted) {
      a = 0;
    }

    PrintTpsSummary(1, FLAGS_ycsb_run_for_seconds, FLAGS_ycsb_threads, thread_committed,
                    thread_aborted);

    keep_running.store(false);
    for (auto& thread : threads) {
      thread.join();
    }
  }
};

} // namespace leanstore::ycsb