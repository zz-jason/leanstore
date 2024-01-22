#include "shared-headers/Units.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include <benchmark/benchmark.h>

#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <vector>

using namespace leanstore;
using namespace leanstore::utils;
using namespace leanstore::storage;

static void BenchU8ToPage(benchmark::State& state) {
  AlignedBuffer<512> alignedBuffer(FLAGS_page_size * 4);
  auto* buf = alignedBuffer.Get();
  auto i = 1;
  for (auto _ : state) {
    reinterpret_cast<Page*>(&buf[i * FLAGS_page_size])->mGSN = 1;
  }
}

static void BenchPageDirectly(benchmark::State& state) {
  Page pages[4];
  auto i = 1;
  for (auto _ : state) {
    pages[i].mGSN = 1;
  }
}

static std::array<std::atomic<u64>, 8> tmpArray;
static void BenchStdArray(benchmark::State& state) {
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      tmpArray[i].store(val, std::memory_order_release);
      // rawArray[i].store(val, std::memory_order_release);
    }
  }
}

static std::vector<std::atomic<u64>> tmpArray2(8);
static void BenchVecArray(benchmark::State& state) {
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      tmpArray2[i].store(val, std::memory_order_release);
    }
  }
}

static std::unique_ptr<std::atomic<u64>[]> tmpArray3 =
    std::make_unique<std::atomic<u64>[]>(8);
static void BenchRawArray(benchmark::State& state) {
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      tmpArray3[i].store(val, std::memory_order_release);
    }
  }
}

BENCHMARK(BenchU8ToPage);
BENCHMARK(BenchPageDirectly);
BENCHMARK(BenchStdArray);
BENCHMARK(BenchVecArray);
BENCHMARK(BenchRawArray);
BENCHMARK_MAIN();