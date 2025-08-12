#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <benchmark/benchmark.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <unordered_set>

namespace leanstore::test {

static void BenchUpdateInsert(benchmark::State& state) {
  std::filesystem::path dir_path = "/tmp/InsertUpdateBench";
  std::filesystem::remove_all(dir_path);
  std::filesystem::create_directories(dir_path);

  StoreOption* option = CreateStoreOption("/tmp/leanstore/InsertUpdateBench");
  option->create_from_scratch_ = true;
  option->worker_threads_ = 4;
  auto leanstore = std::make_unique<leanstore::LeanStore>(option);

  storage::btree::TransactionKV* btree;

  // create leanstore btree for table records
  const auto* btree_name = "testTree1";
  leanstore->ExecSync(0, [&]() {
    auto res = leanstore->CreateTransactionKV(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
    btree = res.value();
  });

  std::unordered_set<std::string> dedup;
  for (auto _ : state) {
    leanstore->ExecSync(0, [&]() {
      cr::TxManager::My().StartTx();
      std::string key;
      std::string val;
      for (size_t i = 0; i < 16; i++) {
        key = utils::RandomGenerator::RandAlphString(24);
        val = utils::RandomGenerator::RandAlphString(128);
        btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                      Slice((const uint8_t*)val.data(), val.size()));
      }
      cr::TxManager::My().CommitTx();
    });
  }

  leanstore->ExecSync(0, [&]() {
    cr::TxManager::My().StartTx();
    SCOPED_DEFER(cr::TxManager::My().CommitTx());
    leanstore->DropTransactionKV(btree_name);
  });
}

BENCHMARK(BenchUpdateInsert);

} // namespace leanstore::test

BENCHMARK_MAIN();