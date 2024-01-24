#include "Config.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/RandomGenerator.hpp"

#include <benchmark/benchmark.h>

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <filesystem>
#include <unordered_set>

namespace leanstore::test {

static void BenchUpdateInsert(benchmark::State& state) {
  FLAGS_wal = true;
  FLAGS_bulk_insert = false;
  FLAGS_worker_threads = 4;
  FLAGS_init = true;
  FLAGS_data_dir = "/tmp/InsertUpdateBench";
  FLAGS_wal_fsync = false;

  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  auto sLeanStore = std::make_unique<leanstore::LeanStore>();

  storage::btree::TransactionKV* btree;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
    cr::Worker::My().StartTx();
    sLeanStore->RegisterTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();
  });

  std::unordered_set<std::string> dedup;
  for (auto _ : state) {
    cr::CRManager::sInstance->ScheduleJobAsync(0, [&]() {
      cr::Worker::My().StartTx();
      std::string key;
      std::string val;
      for (size_t i = 0; i < 16; i++) {
        key = utils::RandomGenerator::RandAlphString(24);
        val = utils::RandomGenerator::RandAlphString(128);
        btree->Insert(Slice((const u8*)key.data(), key.size()),
                      Slice((const u8*)val.data(), val.size()));
      }
      cr::Worker::My().CommitTx();
    });
  }

  cr::CRManager::sInstance->ScheduleJobSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    sLeanStore->UnRegisterTransactionKV(btreeName);
  });
}

BENCHMARK(BenchUpdateInsert);

} // namespace leanstore::test

BENCHMARK_MAIN();