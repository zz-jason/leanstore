#include "btree/BasicKV.hpp"
#include "btree/TransactionKV.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/StoreOption.hpp"
#include "utils/RandomGenerator.hpp"

#include <benchmark/benchmark.h>

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <filesystem>
#include <unordered_set>

namespace leanstore::test {

static void BenchUpdateInsert(benchmark::State& state) {
  std::filesystem::path dirPath = "/tmp/InsertUpdateBench";
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  auto sLeanStore = std::make_unique<leanstore::LeanStore>(StoreOption{
      .mCreateFromScratch = true,
      .mStoreDir = "/tmp/InsertUpdateBench",
      .mWorkerThreads = 4,
  });

  storage::btree::TransactionKV* btree;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  sLeanStore->ExecSync(0, [&]() {
    auto res = sLeanStore->CreateTransactionKV(btreeName);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
    btree = res.value();
  });

  std::unordered_set<std::string> dedup;
  for (auto _ : state) {
    sLeanStore->ExecSync(0, [&]() {
      cr::Worker::My().StartTx();
      std::string key;
      std::string val;
      for (size_t i = 0; i < 16; i++) {
        key = utils::RandomGenerator::RandAlphString(24);
        val = utils::RandomGenerator::RandAlphString(128);
        btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                      Slice((const uint8_t*)val.data(), val.size()));
      }
      cr::Worker::My().CommitTx();
    });
  }

  sLeanStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    sLeanStore->DropTransactionKV(btreeName);
  });
}

BENCHMARK(BenchUpdateInsert);

} // namespace leanstore::test

BENCHMARK_MAIN();