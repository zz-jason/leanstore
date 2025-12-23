#include "leanstore/cpp/base/defer.hpp"
#include "leanstore/cpp/base/log.hpp"
#include "leanstore/cpp/base/small_vector.hpp"
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
  explicit YcsbRocksDb(const YcsbOptions& ycsb_options) : YcsbExecutor(ycsb_options) {
    rocksdb::Options rocksdb_options;
    rocksdb_options.create_if_missing = true;
    rocksdb_options.error_if_exists = false;
    rocksdb_options.arena_block_size = options_.mem_gb_ << 30;

    auto status = rocksdb::DB::Open(rocksdb_options,
                                    options_.data_dir_ + "/rocksdb/" + options_.workload_, &db_);
    if (!status.ok()) {
      Log::Fatal("Failed to open rocksdb: {}", status.ToString());
    }
  }

  void HandleCmdLoad() override {
    // load data with options_.threads_
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << options_.record_count_ << " values" << std::endl;
    LEAN_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      std::cout << "Done inserting" << ", time elapsed: " << duration / 1000000.0 << " seconds"
                << ", throughput: " << CalculateTps(start, end, options_.record_count_) << " tps"
                << std::endl;
    });

    utils::Parallelize::Range(
        options_.threads_, options_.record_count_,
        [&](uint64_t thread_id [[maybe_unused]], uint64_t begin, uint64_t end) {
          SmallBuffer256 key_buffer(options_.key_size_);
          SmallBuffer256 val_buffer(options_.val_size_);
          auto* key = key_buffer.Data();
          auto* val = val_buffer.Data();
          rocksdb::WriteOptions write_options;

          for (uint64_t i = begin; i < end; i++) {
            // generate key-value for insert
            GenKey(i, key);
            utils::RandomGenerator::RandString(val, options_.val_size_);

            auto status = db_->Put(write_options, rocksdb::Slice((char*)key, options_.key_size_),
                                   rocksdb::Slice((char*)val, options_.val_size_));
            if (!status.ok()) {
              Log::Fatal("Failed to insert: {}", status.ToString());
            }
          }
        });
  }

  void HandleCmdRun() override {
    auto workload_type = static_cast<Workload>(options_.workload_[0] - 'a');
    auto workload = GetWorkloadSpec(workload_type);
    auto zipf_random =
        utils::ScrambledZipfGenerator(0, options_.record_count_, options_.zipf_factor_);
    std::atomic<bool> keep_running = true;
    std::vector<std::atomic<uint64_t>> thread_committed(options_.threads_);
    std::vector<std::atomic<uint64_t>> thread_aborted(options_.threads_);

    std::vector<std::thread> threads;
    for (uint64_t worker_id = 0; worker_id < options_.threads_; worker_id++) {
      threads.emplace_back(
          [&](uint64_t thread_id) {
            SmallBuffer256 key_buffer(options_.key_size_);
            auto* key = key_buffer.Data();
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
                  auto status = db_->Get(rocksdb::ReadOptions(),
                                         rocksdb::Slice((char*)key, options_.key_size_), &val_read);
                  if (!status.ok()) {
                    Log::Fatal("Failed to read: {}", status.ToString());
                  }
                } else {
                  // generate key for update
                  GenYcsbKey(zipf_random, key);
                  // generate val for update
                  auto status = db_->Put(rocksdb::WriteOptions(),
                                         rocksdb::Slice((char*)key, options_.key_size_),
                                         rocksdb::Slice((char*)key, options_.val_size_));
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

    PrintTpsSummary(1, options_.run_for_seconds_, options_.threads_, thread_committed,
                    thread_aborted);

    keep_running.store(false);
    for (auto& thread : threads) {
      thread.join();
    }
  }
};

} // namespace leanstore::ycsb