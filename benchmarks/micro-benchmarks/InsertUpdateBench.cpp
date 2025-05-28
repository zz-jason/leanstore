#include "leanstore-c/store_option.h"
#include "leanstore/LeanStore.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

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

  StoreOption* option = CreateStoreOption("/tmp/leanstore/InsertUpdateBench");
  option->mCreateFromScratch = true;
  option->mWorkerThreads = 4;
  auto sLeanStore = std::make_unique<leanstore::LeanStore>(option);

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
      cr::WorkerContext::My().StartTx();
      std::string key;
      std::string val;
      for (size_t i = 0; i < 16; i++) {
        key = utils::RandomGenerator::RandAlphString(24);
        val = utils::RandomGenerator::RandAlphString(128);
        btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                      Slice((const uint8_t*)val.data(), val.size()));
      }
      cr::WorkerContext::My().CommitTx();
    });
  }

  sLeanStore->ExecSync(0, [&]() {
    cr::WorkerContext::My().StartTx();
    SCOPED_DEFER(cr::WorkerContext::My().CommitTx());
    sLeanStore->DropTransactionKV(btreeName);
  });
}

BENCHMARK(BenchUpdateInsert);

} // namespace leanstore::test

BENCHMARK_MAIN();