#include "Ycsb.hpp"
#include "btree/BasicKV.hpp"
#include "btree/TransactionKV.hpp"
#include "concurrency/CRManager.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/StoreOption.hpp"
#include "utils/Defer.hpp"
#include "utils/JumpMU.hpp"
#include "utils/Log.hpp"
#include "utils/RandomGenerator.hpp"
#include "utils/ScrambledZipfGenerator.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <format>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace leanstore::ycsb {

constexpr std::string kTableName = "ycsb_leanstore";

class YcsbLeanStore : public YcsbExecutor {
private:
  std::unique_ptr<LeanStore> mStore;

  bool mBenchTransactionKv;

public:
  YcsbLeanStore(bool benchTransactionKv, bool createFromScratch)
      : mBenchTransactionKv(benchTransactionKv) {
    auto res = LeanStore::Open(StoreOption{
        .mCreateFromScratch = createFromScratch,
        .mStoreDir = FLAGS_ycsb_data_dir + "/leanstore",
        .mWorkerThreads = FLAGS_ycsb_threads,
        .mBufferPoolSize = FLAGS_ycsb_mem_kb * 1024,
        .mEnableMetrics = true,
        .mMetricsPort = 8080,
    });
    if (!res) {
      std::cerr << "Failed to open leanstore: " << res.error().ToString() << std::endl;
      exit(res.error().Code());
    }

    mStore = std::move(res.value());
  }

  ~YcsbLeanStore() override {
    std::cout << "~YcsbLeanStore" << std::endl;
    mStore.reset(nullptr);
  }

  KVInterface* CreateTable() {
    // create table with transaction kv
    if (mBenchTransactionKv) {
      btree::TransactionKV* table;
      mStore->ExecSync(0, [&]() {
        auto res = mStore->CreateTransactionKV(kTableName);
        if (!res) {
          Log::Fatal("Failed to create table: name={}, error={}", kTableName,
                     res.error().ToString());
        }
        table = res.value();
      });
      return table;
    }

    // create table with basic kv
    btree::BasicKV* table;
    mStore->ExecSync(0, [&]() {
      auto res = mStore->CreateBasicKV(kTableName);
      if (!res) {
        Log::Fatal("Failed to create table: name={}, error={}", kTableName, res.error().ToString());
      }
      table = res.value();
    });
    return table;
  }

  KVInterface* GetTable() {
    if (mBenchTransactionKv) {
      btree::TransactionKV* table;
      mStore->GetTransactionKV(kTableName, &table);
      return table;
    }
    btree::BasicKV* table;
    mStore->GetBasicKV(kTableName, &table);
    return table;
  }

  void HandleCmdLoad() override {
    auto* table = CreateTable();

    // record the start and end time, calculating throughput in the end
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Inserting " << FLAGS_ycsb_record_count << " values" << std::endl;
    SCOPED_DEFER({
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      auto summary =
          std::format("Done inserting, time elapsed={:.2f} seconds, throughput={:.2f} tps",
                      duration / 1000000.0, CalculateTps(start, end, FLAGS_ycsb_record_count));
      std::cout << summary << std::endl;
    });

    auto numWorkers = mStore->mStoreOption.mWorkerThreads;
    auto avg = FLAGS_ycsb_record_count / numWorkers;
    auto rem = FLAGS_ycsb_record_count % numWorkers;
    for (auto workerId = 0u, begin = 0u; workerId < numWorkers;) {
      auto end = begin + avg + (rem-- > 0 ? 1 : 0);
      mStore->ExecAsync(workerId, [&, begin, end]() {
        uint8_t key[FLAGS_ycsb_key_size];
        uint8_t val[FLAGS_ycsb_val_size];

        for (uint64_t i = begin; i < end; i++) {
          // generate key-value for insert
          GenKey(i, key);
          utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

          if (mBenchTransactionKv) {
            cr::Worker::My().StartTx();
          }
          auto opCode =
              table->Insert(Slice(key, FLAGS_ycsb_key_size), Slice(val, FLAGS_ycsb_val_size));
          if (opCode != OpCode::kOK) {
            Log::Fatal("Failed to insert, opCode={}", static_cast<uint8_t>(opCode));
          }
          if (mBenchTransactionKv) {
            cr::Worker::My().CommitTx();
          }
        }
      });
      workerId++, begin = end;
    }
    mStore->WaitAll();
  }

  void HandleCmdRun() override {
    auto* table = GetTable();
    auto workloadType = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workloadType);
    auto zipfRandom =
        utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor);
    atomic<bool> keepRunning = true;
    std::vector<std::atomic<uint64_t>> threadCommitted(mStore->mStoreOption.mWorkerThreads);
    std::vector<std::atomic<uint64_t>> threadAborted(mStore->mStoreOption.mWorkerThreads);
    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    for (uint64_t workerId = 0; workerId < mStore->mStoreOption.mWorkerThreads; workerId++) {
      mStore->ExecAsync(workerId, [&]() {
        uint8_t key[FLAGS_ycsb_key_size];
        std::string valRead;
        auto copyValue = [&](Slice val) { val.CopyTo(valRead); };

        auto updateDescBufSize = UpdateDesc::Size(1);
        uint8_t updateDescBuf[updateDescBufSize];
        auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
        updateDesc->mNumSlots = 1;
        updateDesc->mUpdateSlots[0].mOffset = 0;
        updateDesc->mUpdateSlots[0].mSize = FLAGS_ycsb_val_size;

        std::string valGen;
        auto updateCallBack = [&](MutableSlice toUpdate) {
          auto newValSize = updateDesc->mUpdateSlots[0].mSize;
          utils::RandomGenerator::RandAlphString(newValSize, valGen);
          std::memcpy(toUpdate.Data(), valGen.data(), valGen.size());
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
                if (mBenchTransactionKv) {
                  cr::Worker::My().StartTx(TxMode::kShortRunning,
                                           IsolationLevel::kSnapshotIsolation, true);
                  table->Lookup(Slice(key, FLAGS_ycsb_key_size), copyValue);
                  cr::Worker::My().CommitTx();
                } else {
                  table->Lookup(Slice(key, FLAGS_ycsb_key_size), copyValue);
                }
              } else {
                // generate key for update
                GenYcsbKey(zipfRandom, key);
                // generate val for update
                if (mBenchTransactionKv) {
                  cr::Worker::My().StartTx();
                  table->UpdatePartial(Slice(key, FLAGS_ycsb_key_size), updateCallBack,
                                       *updateDesc);
                  cr::Worker::My().CommitTx();
                } else {
                  table->UpdatePartial(Slice(key, FLAGS_ycsb_key_size), updateCallBack,
                                       *updateDesc);
                }
              }
              break;
            }
            default: {
              Log::Fatal("Unsupported workload type: {}", static_cast<uint8_t>(workloadType));
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

    printTpsSummary(1, FLAGS_ycsb_run_for_seconds, mStore->mStoreOption.mWorkerThreads,
                    threadCommitted, threadAborted);

    // Shutdown threads
    keepRunning = false;
    mStore->WaitAll();
  }
};

} // namespace leanstore::ycsb
