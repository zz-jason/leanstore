#include "LeanStore.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

namespace leanstore::test {

template <typename T = std::mt19937>
auto random_generator() -> T {
  auto constexpr fixed_seed = 123456789; // Fixed seed for deterministic output
  return T{fixed_seed};
}

static std::string generate_random_alphanumeric_string(std::size_t len) {
  static constexpr auto chars = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  thread_local auto rng = random_generator<>();
  auto dist = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[dist(rng)]; });
  return result;
}

static void BenchUpdateInsert(benchmark::State& state) {
  FLAGS_vi = true;
  FLAGS_enable_print_btree_stats_on_exit = true;
  FLAGS_wal = true;
  FLAGS_bulk_insert = false;
  FLAGS_worker_threads = 4;
  FLAGS_recover = false;
  FLAGS_data_dir = "/tmp/InsertUpdateBench";
  FLAGS_wal_fsync = false;

  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  auto sLeanStore = std::make_unique<leanstore::LeanStore>();

  storage::btree::BTreeVI* btree;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    EXPECT_TRUE(
        sLeanStore->RegisterBTreeVI(btreeName, btreeConfig, &btree));
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().commitTX();
  });

  std::unordered_set<std::string> dedup;
  for (auto _ : state) {
    cr::CRManager::sInstance->scheduleJobAsync(0, [&]() {
      cr::Worker::my().startTX();
      std::string key;
      std::string val;
      for (size_t i = 0; i < 16; i++) {
        key = generate_random_alphanumeric_string(24);
        val = generate_random_alphanumeric_string(128);
        btree->insert(Slice((const u8*)key.data(), key.size()),
                      Slice((const u8*)val.data(), val.size()));
      }
      cr::Worker::my().commitTX();
    });
  }

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().startTX();
    SCOPED_DEFER(cr::Worker::my().commitTX());
    sLeanStore->UnRegisterBTreeVI(btreeName);
  });
}

BENCHMARK(BenchUpdateInsert);

} // namespace leanstore::test

BENCHMARK_MAIN();