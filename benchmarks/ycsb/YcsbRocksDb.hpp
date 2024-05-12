#include "Ycsb.hpp"
#include "utils/Defer.hpp"
#include "utils/Log.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/ScrambledZipfGenerator.hpp"

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

    auto status = rocksdb::DB::Open(
        options, FLAGS_ycsb_data_dir + "/rocksdb/" + FLAGS_ycsb_workload, &mDb);
    if (!status.ok()) {
      Log::Fatal("Failed to open rocksdb: {}", status.ToString());
    }
#endif
  }

  void HandleCmdLoad() override {
#ifdef ENABLE_ROCKSDB
    // load data with FLAGS_ycsb_threads
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values"
              << std::endl;
    SCOPED_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
      std::cout << "Done inserting"
                << ", time elapsed: " << duration / 1000000.0 << " seconds"
                << ", throughput: "
                << CalculateTps(start, end, FLAGS_ycsb_record_count) << " tps"
                << std::endl;
    });

    utils::Parallelize::Range(
        FLAGS_ycsb_threads, FLAGS_ycsb_record_count,
        [&](uint64_t, uint64_t begin, uint64_t end) {
          for (uint64_t i = begin; i < end; i++) {
            // generate key
            uint8_t key[FLAGS_ycsb_key_size];
            GenKey(i, key);

            // generate value
            uint8_t val[FLAGS_ycsb_val_size];
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            auto status =
                mDb->Put(rocksdb::WriteOptions(),
                         rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
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
    auto zipfRandom = utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count,
                                                    FLAGS_ycsb_zipf_factor);
    std::atomic<bool> keepRunning = true;
    std::vector<std::atomic<uint64_t>> threadCommitted(FLAGS_ycsb_threads);
    std::vector<std::atomic<uint64_t>> threadAborted(FLAGS_ycsb_threads);
    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    for (uint64_t workerId = 0; workerId < FLAGS_ycsb_threads; workerId++) {
      std::thread([&, workerId]() {
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
              auto status = mDb->Get(
                  rocksdb::ReadOptions(),
                  rocksdb::Slice((char*)key, FLAGS_ycsb_key_size), &valRead);
              if (!status.ok()) {
                Log::Fatal("Failed to read: {}", status.ToString());
              }
            } else {
              // generate key for update
              GenYcsbKey(zipfRandom, key);
              // generate val for update
              auto status =
                  mDb->Put(rocksdb::WriteOptions(),
                           rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                           rocksdb::Slice((char*)key, FLAGS_ycsb_val_size));
              if (!status.ok()) {
                threadAborted[workerId]++;
              }
            }
            break;
          }
          default: {
            Log::Fatal("Unsupported workload type: {}",
                       static_cast<uint8_t>(workloadType));
          }
          }
          threadCommitted[workerId]++;
        }
      }).detach();
    }

    auto reportPeriod = 1;
    for (uint64_t i = 0; i < FLAGS_ycsb_run_for_seconds; i += reportPeriod) {
      sleep(reportPeriod);
      auto committed = 0;
      auto aborted = 0;
      for (auto& c : threadCommitted) {
        committed += c.exchange(0);
      }
      for (auto& a : threadAborted) {
        aborted += a.exchange(0);
      }
      auto abortRate = (aborted)*1.0 / (committed + aborted);
      auto summary = std::format("[{} thds] [{}s] [tps={:.2f}] [committed={}] "
                                 "[conflicted={}] [conflict rate={:.2f}]",
                                 FLAGS_ycsb_threads, i,
                                 (committed + aborted) * 1.0 / reportPeriod,
                                 committed, aborted, abortRate);
      std::cout << summary << std::endl;
    }
#endif
  }
};

} // namespace leanstore::ycsb