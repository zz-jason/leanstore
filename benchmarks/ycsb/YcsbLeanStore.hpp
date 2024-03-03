#include "Config.hpp"
#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "Ycsb.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/TransactionKV.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "utils/Defer.hpp"
#include "utils/Parallelize.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/ScrambledZipfGenerator.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace leanstore::ycsb {

class YcsbLeanStore : public YcsbExecutor {
private:
  std::unique_ptr<LeanStore> mStore = nullptr;

public:
  YcsbLeanStore() {
    FLAGS_init = true;
    FLAGS_data_dir = "/tmp/ycsb/" + FLAGS_ycsb_workload;

    auto res = LeanStore::Open();
    if (!res) {
      std::cerr << "Failed to open leanstore: " << res.error().ToString()
                << std::endl;
      exit(res.error().Code());
    }

    mStore = std::move(res.value());
  }

  inline KVInterface* CreateTable() {
    auto tableName = "ycsb_" + FLAGS_ycsb_workload;
    auto config = btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };
    btree::TransactionKV* table;
    mStore->ExecSync(0, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      mStore->CreateTransactionKV(tableName, config, &table);
    });
    return table;
  }

  inline KVInterface* GetTable() {
    auto tableName = "ycsb_" + FLAGS_ycsb_workload;
    btree::TransactionKV* table;
    mStore->GetTransactionKV(tableName, &table);
    return table;
  }

public:
  void HandleCmdLoad() override {
    auto* table = CreateTable();
    auto zipfRandom = utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count,
                                                    FLAGS_zipf_factor);

    // record the start and end time, calculating throughput in the end
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

    utils::Parallelize::range(
        FLAGS_worker_threads, FLAGS_ycsb_record_count,
        [&](uint64_t workerId, uint64_t begin, uint64_t end) {
          mStore->ExecAsync(workerId, [&, begin, end]() {
            for (uint64_t i = begin; i < end; i++) {
              // generate key
              uint8_t key[FLAGS_ycsb_key_size];
              GenYcsbKey(zipfRandom, key);

              // generate value
              uint8_t val[FLAGS_ycsb_val_size];
              utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

              cr::Worker::My().StartTx();
              table->Insert(Slice(key, FLAGS_ycsb_key_size),
                            Slice(val, FLAGS_ycsb_val_size));
              cr::Worker::My().CommitTx();
            }
          });
        });
    mStore->WaitAll();
  }

  void HandleCmdRun() override {
    auto* table = GetTable();
    auto workloadType = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workloadType);
    auto zipfRandom = utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count,
                                                    FLAGS_zipf_factor);
    atomic<bool> keepRunning = true;
    std::vector<std::atomic<uint64_t>> threadCommitted(FLAGS_worker_threads);
    std::vector<std::atomic<uint64_t>> threadAborted(FLAGS_worker_threads);
    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    for (uint64_t workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
      mStore->ExecAsync(workerId, [&]() {
        uint8_t key[FLAGS_ycsb_key_size];
        std::string valRead;
        auto copyValue = [&](Slice val) {
          valRead.resize(val.size(), 0);
          std::memcpy(valRead.data(), val.data(), val.size());
        };

        auto updateDescBufSize = UpdateDesc::Size(1);
        uint8_t updateDescBuf[updateDescBufSize];
        auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
        updateDesc->mNumSlots = 1;
        updateDesc->mUpdateSlots[0].mOffset = 0;
        updateDesc->mUpdateSlots[0].mSize = FLAGS_ycsb_val_size;

        auto updateCallBack = [&](MutableSlice toUpdate) {
          auto newValSize = updateDesc->mUpdateSlots[0].mSize;
          auto newVal = utils::RandomGenerator::RandAlphString(newValSize);
          std::memcpy(toUpdate.Data(), newVal.data(), newVal.size());
        };

        while (keepRunning) {
          JUMPMU_TRY() {
            switch (workloadType) {
            case Workload::kA:
            case Workload::kB:
            case Workload::kC: {
              auto readProbability = utils::RandomGenerator::Rand(0, 100);
              if (readProbability <= workload.mReadProportion * 100) {
                // generate key for read
                GenYcsbKey(zipfRandom, key);
                cr::Worker::My().StartTx(TxMode::kShortRunning,
                                         IsolationLevel::kSnapshotIsolation,
                                         true);
                table->Lookup(Slice(key, FLAGS_ycsb_key_size), copyValue);
                cr::Worker::My().CommitTx();
              } else {
                // generate key for update
                GenYcsbKey(zipfRandom, key);
                // generate val for update
                cr::Worker::My().StartTx();
                table->UpdatePartial(Slice(key, FLAGS_ycsb_key_size),
                                     updateCallBack, *updateDesc);
                cr::Worker::My().CommitTx();
              }
              break;
            }
            default: {
              LOG(FATAL) << "Unsupported workload type: "
                         << static_cast<uint8_t>(workloadType);
            }
            }
            threadCommitted[cr::Worker::My().mWorkerId]++;
          }
          JUMPMU_CATCH() {
            threadAborted[cr::Worker::My().mWorkerId]++;
          }
        }
      });
    }

    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
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
      std::cout << "[" << i << "s] "
                << " [tps=" << committed * 1.0 / reportPeriod << "]" // tps
                << " [committed=" << committed << "]"     // committed count
                << " [conflicted=" << aborted << "]"      // aborted count
                << " [conflict rate=" << abortRate << "]" // abort rate
                << std::endl;
    }

    // Shutdown threads
    keepRunning = false;
    mStore->WaitAll();
  }
};

} // namespace leanstore::ycsb
