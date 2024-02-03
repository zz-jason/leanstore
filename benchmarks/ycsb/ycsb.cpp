#include "Config.hpp"
#include "KVInterface.hpp"
#include "LeanStore.hpp"
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
#include <rocksdb/db.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// For data preparation
static std::string kCmdLoad = "load";
static std::string kCmdRun = "run";
static std::string kTargetLeanStore = "leanstore";
static std::string kTargetRocksDb = "rocksdb";

// For the benchmark driver
DEFINE_string(
    ycsb_target, "leanstore",
    "Ycsb target, available: unordered_map, leanstore, rocksdb, leveldb");
DEFINE_string(ycsb_cmd, "run", "Ycsb command, available: run, load");
DEFINE_string(ycsb_workload, "a", "Ycsb workload, available: a, b, c, d, e, f");
DEFINE_uint64(ycsb_run_for_seconds, 300, "Run the benchmark for x seconds");

// For the data preparation
DEFINE_uint64(ycsb_key_size, 16, "Key size in bytes");
DEFINE_uint64(ycsb_val_size, 120, "Value size in bytes");
DEFINE_uint64(ycsb_record_count, 10000, "The number of records to insert");
DEFINE_double(zipf_factor, 0.99, "Zipf factor, 0 means uniform distribution");

namespace leanstore::ycsb {

enum class Distrubition : u8 {
  kUniform = 0,
  kZipf = 1,
  kLatest = 2,
};

enum class Workload : u8 {
  kA = 0,
  kB = 1,
  kC = 2,
  kD = 3,
  kE = 4,
  kF = 5,
};

struct WorkloadSpec {
  u64 mRecordCount;
  u64 mOperationCount;
  bool mReadAllFields;
  double mReadProportion;
  double mUpdateProportion;
  double mScanProportion;
  double mInsertProportion;
  Distrubition mKeyDistrubition;
};

// Generate workload spec from workload type
WorkloadSpec GetWorkloadSpec(Workload workload) {
  switch (workload) {
  case Workload::kA:
    return {100, 100000, true, 0.5, 0.5, 0.0, 0.0, Distrubition::kZipf};
  case Workload::kB:
    return {100, 100000, true, 0.95, 0.05, 0.0, 0.0, Distrubition::kZipf};
  case Workload::kC:
    return {100, 100000, true, 1.0, 0.0, 0.0, 0.0, Distrubition::kZipf};
  case Workload::kD:
    return {100, 100000, true, 0.95, 0.0, 0.0, 0.05, Distrubition::kLatest};
  case Workload::kE:
    return {100, 100000, true, 0.0, 0.0, 0.95, 0.05, Distrubition::kZipf};
  case Workload::kF:
    return {100, 100000, true, 0.5, 0.0, 0.0, 0.5, Distrubition::kUniform};
  default:
    LOG(FATAL) << "Unknown workload: " << static_cast<u8>(workload);
  }
}

double CalculateTps(chrono::high_resolution_clock::time_point begin,
                    chrono::high_resolution_clock::time_point end,
                    u64 numOperations) {
  // calculate secondas elaspsed
  auto sec = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                 .count() /
             1000.0;
  return numOperations / sec;
}

static void GenYcsbKey(utils::ScrambledZipfGenerator& zipfRandom, u8* keyBuf) {
  auto zipfKey = zipfRandom.rand();
  auto zipfKeyStr = std::to_string(zipfKey);
  auto prefixSize = FLAGS_ycsb_key_size - zipfKeyStr.size() > 0
                        ? FLAGS_ycsb_key_size - zipfKeyStr.size()
                        : 0;
  std::memset(keyBuf, 'k', prefixSize);
  std::memcpy(keyBuf + prefixSize, zipfKeyStr.data(), zipfKeyStr.size());
}

class YcsbExecutor {
public:
  virtual ~YcsbExecutor() = default;

  virtual void HandleCmdLoad() = 0;

  virtual void HandleCmdRun() = 0;
};

class YcsbExecutor4LeanStore;
class YcsbExecutor4RocksDb;

class YcsbExecutor4LeanStore : public YcsbExecutor {
private:
  std::unique_ptr<LeanStore> mStore = nullptr;

public:
  YcsbExecutor4LeanStore() {
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
        [&](u64 workerId, u64 begin, u64 end) {
          mStore->ExecAsync(workerId, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
              // generate key
              u8 key[FLAGS_ycsb_key_size];
              GenYcsbKey(zipfRandom, key);

              // generate value
              u8 val[FLAGS_ycsb_val_size];
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
    std::vector<std::atomic<u64>> threadCommitted(FLAGS_worker_threads);
    std::vector<std::atomic<u64>> threadAborted(FLAGS_worker_threads);
    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
      mStore->ExecAsync(workerId, [&]() {
        u8 key[FLAGS_ycsb_key_size];
        std::string valRead;
        auto copyValue = [&](Slice val) {
          valRead = std::string((char*)val.data(), val.size());
        };

        auto updateDescBufSize = UpdateDesc::Size(1);
        u8 updateDescBuf[updateDescBufSize];
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
                         << static_cast<u8>(workloadType);
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
    for (u64 i = 0; i < FLAGS_ycsb_run_for_seconds; i += reportPeriod) {
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

class YcsbExecutor4RocksDb : public YcsbExecutor {
private:
  rocksdb::DB* mDb = nullptr;

public:
  YcsbExecutor4RocksDb() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    auto status = rocksdb::DB::Open(
        options, "/tmp/ycsb/rocksdb/" + FLAGS_ycsb_workload, &mDb);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to open rocksdb: " << status.ToString();
    }
  }

  void HandleCmdLoad() override {
    // load data with FLAGS_worker_threads
    auto zipfRandom = utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count,
                                                    FLAGS_zipf_factor);
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
        [&](u64, u64 begin, u64 end) {
          for (u64 i = begin; i < end; i++) {
            // generate key
            u8 key[FLAGS_ycsb_key_size];
            GenYcsbKey(zipfRandom, key);

            // generate value
            u8 val[FLAGS_ycsb_val_size];
            utils::RandomGenerator::RandString(val, FLAGS_ycsb_val_size);

            auto status =
                mDb->Put(rocksdb::WriteOptions(),
                         rocksdb::Slice((char*)key, FLAGS_ycsb_key_size),
                         rocksdb::Slice((char*)val, FLAGS_ycsb_val_size));
            if (!status.ok()) {
              LOG(FATAL) << "Failed to insert: " << status.ToString();
            }
          }
        });
  }

  void HandleCmdRun() override {
    // Run the benchmark in FLAGS_worker_threads
    auto workloadType = static_cast<Workload>(FLAGS_ycsb_workload[0] - 'a');
    auto workload = GetWorkloadSpec(workloadType);
    auto zipfRandom = utils::ScrambledZipfGenerator(0, FLAGS_ycsb_record_count,
                                                    FLAGS_zipf_factor);
    atomic<bool> keepRunning = true;
    std::vector<std::atomic<u64>> threadCommitted(FLAGS_worker_threads);
    std::vector<std::atomic<u64>> threadAborted(FLAGS_worker_threads);
    // init counters
    for (auto& c : threadCommitted) {
      c = 0;
    }
    for (auto& a : threadAborted) {
      a = 0;
    }

    for (u64 workerId = 0; workerId < FLAGS_worker_threads; workerId++) {
      std::thread([&, workerId]() {
        u8 key[FLAGS_ycsb_key_size];
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
                LOG(FATAL) << "Failed to read: " << status.ToString();
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
            LOG(FATAL) << "Unsupported workload type: "
                       << static_cast<u8>(workloadType);
          }
          }
          threadCommitted[workerId]++;
        }
      }).detach();
    }

    auto reportPeriod = 1;
    for (u64 i = 0; i < FLAGS_ycsb_run_for_seconds; i += reportPeriod) {
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
  }
};

} // namespace leanstore::ycsb

using namespace leanstore;

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Ycsb Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Transform ycsb_target to lowercase
  std::transform(FLAGS_ycsb_target.begin(), FLAGS_ycsb_target.end(),
                 FLAGS_ycsb_target.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_cmd to lowercase
  std::transform(FLAGS_ycsb_cmd.begin(), FLAGS_ycsb_cmd.end(),
                 FLAGS_ycsb_cmd.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Transform ycsb_workload to lowercase
  std::transform(FLAGS_ycsb_workload.begin(), FLAGS_ycsb_workload.end(),
                 FLAGS_ycsb_workload.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (FLAGS_ycsb_key_size < 8) {
    LOG(FATAL) << "Key size must be >= 8";
  }

  leanstore::ycsb::YcsbExecutor* executor = nullptr;
  if (FLAGS_ycsb_target == kTargetLeanStore) {
    executor = new leanstore::ycsb::YcsbExecutor4LeanStore();
  } else if (FLAGS_ycsb_target == kTargetRocksDb) {
    executor = new leanstore::ycsb::YcsbExecutor4RocksDb();
  } else {
    LOG(FATAL) << "Unknown target: " << FLAGS_ycsb_target;
  }

  if (FLAGS_ycsb_cmd == kCmdLoad) {
    executor->HandleCmdLoad();
    return 0;
  }

  if (FLAGS_ycsb_cmd == kCmdRun) {
    executor->HandleCmdLoad();
    executor->HandleCmdRun();
    return 0;
  }

  LOG(FATAL) << "Unknown command: " << FLAGS_ycsb_cmd;
  return 0;
}
