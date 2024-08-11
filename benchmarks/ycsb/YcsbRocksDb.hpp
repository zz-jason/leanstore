#include "Ycsb.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#ifdef ENABLE_ROCKSDB
#include <rocksdb/db.h>
#endif

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

namespace leanstore::ycsb {

class YcsbRocksDb : public YcsbExecutor {

private:
#ifdef ENABLE_ROCKSDB
  rocksdb::DB* mDb = nullptr;
#endif

public:
  YcsbRocksDb() {
#ifdef ENABLE_ROCKSDB
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.arena_block_size = FLAGS_ycsb_mem_kb * 1024;

    auto status =
        rocksdb::DB::Open(options, FLAGS_ycsb_data_dir + "/rocksdb/" + FLAGS_ycsb_workload, &mDb);
    if (!status.ok()) {
      Log::Fatal("Failed to open rocksdb: {}", status.ToString());
    }
#endif
  }

  void HandleCmdLoad() override {
#ifdef ENABLE_ROCKSDB
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
        [&](uint64_t threadId [[maybe_unused]], uint64_t begin, uint64_t end) {
          uint8_t key[FLAGS_ycsb_key_size];
          uint8_t val[FLAGS_ycsb_val_size];
          rocksdb::WriteOptions writeOption;

          for (uint64_t i = begin; i < end; i++) {
            // generate key-value for insert
            GenKey(i, key);
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            auto status = mDb->Put(writeOption, rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                                   rocksdb::Slice((char*)val, FLAGS_ycsb_val_size));
            if (!status.ok()) {
              Log::Fatal("Failed to insert: {}", status.ToString());
            }
          }
        });
#endif
  }

  void HandleCmdRun() override {
#ifdef ENABLE_ROCKSDB
    // Run the benchmark in FLAGS_ycsb_threads
    auto workloadType = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workloadType);
    auto zipfRandom =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keepRunning = true;
    std::vector<std::atomic<uint64_t>> threadCommitted(FLAGS_ycsb_threads);
    std::vector<std::atomic<uint64_t>> threadAborted(FLAGS_ycsb_threads);

    std::vector<std::thread> threads;
    for (uint64_t workerId = 0; workerId < FLAGS_ycsb_threads; workerId++) {
      threads.emplace_back(
          [&](uint64_t threadId) {
            uint8_t key[FLAGS_ycsb_key_size];
            std::string valRead;
            while (keepRunning) {
              switch (workloadType) {
              case Workload::kA:
              case Workload::kB:
              case Workload::kC: {
                auto readProbability = utils::RandomGenerator::Rand(0, 100);
                if (readProbability <= workload.mReadProportion * 100) {
                  // generate key for read
                  GenYcsbKey(zipfRandom, key);
                  auto status = mDb->Get(rocksdb::ReadOptions(),
                                         rocksdb::Slice((char*)key, FLAGS_ycsb_key_size), &valRead);
                  if (!status.ok()) {
                    Log::Fatal("Failed to read: {}", status.ToString());
                  }
                } else {
                  // generate key for update
                  GenYcsbKey(zipfRandom, key);
                  // generate val for update
                  auto status = mDb->Put(rocksdb::WriteOptions(),
                                         rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                                         rocksdb::Slice((char*)key, FLAGS_ycsb_val_size));
                  if (!status.ok()) {
                    threadAborted[threadId]++;
                  }
                }
                break;
              }
              default: {
                Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workloadType));
              }
              }
              threadCommitted[threadId]++;
            }
          },
          workerId);
    }

    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    printTpsSummary(1, FLAGS_ycsb_run_for_seconds, FLAGS_ycsb_threads, threadCommitted,
                    threadAborted);

    keepRunning.store(false);
    for (auto& thread : threads) {
      thread.join();
    }

#endif
  }
};

} // namespace leanstore::ycsb