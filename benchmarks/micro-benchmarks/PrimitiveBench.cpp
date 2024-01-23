#include "shared-headers/Units.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "sync-primitives/OptimisticGuarded.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include <benchmark/benchmark.h>

#include <array>
#include <atomic>
#include <cstdint>
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

static void BenchStdArray(benchmark::State& state) {
  static std::array<std::atomic<u64>, 8> sTmpArray;
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      sTmpArray[i].store(val, std::memory_order_release);
      // rawArray[i].store(val, std::memory_order_release);
    }
  }
}

static void BenchVecArray(benchmark::State& state) {
  static std::vector<std::atomic<u64>> sTmpArray2(8);
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      sTmpArray2[i].store(val, std::memory_order_release);
    }
  }
}

static void BenchRawArray(benchmark::State& state) {
  static std::unique_ptr<std::atomic<u64>[]> sTmpArray3 =
      std::make_unique<std::atomic<u64>[]>(8);
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((u64)0, std::numeric_limits<u64>::max());
      sTmpArray3[i].store(val, std::memory_order_release);
    }
  }
}

struct TestPayload {
  uint64_t mVal1;
  uint64_t mVal2;

  TestPayload() = default;

  inline static TestPayload New(uint64_t val1 = 0, uint64_t val2 = 0) {
    TestPayload tmp;
    tmp.mVal1 = val1;
    tmp.mVal2 = val2;
    return tmp;
  }
};

static void BenchSwmrOptimisticGuard(benchmark::State& state) {
  OptimisticGuarded<TestPayload> guardedValue(TestPayload::New(0, 0));

  for (auto _ : state) {
    if (state.thread_index() == 0) {
      if (auto i = RandomGenerator::Rand(0, 20); i == 0) {
        guardedValue.Set(TestPayload::New(RandomGenerator::Rand(0, 100),
                                          RandomGenerator::Rand(100, 200)));
      }
    } else {
      [[maybe_unused]] TestPayload copiedValue;
      [[maybe_unused]] auto version = guardedValue.Get(copiedValue);
    }
  }
}

static void BenchSwmrAtomicValue(benchmark::State& state) {
  std::atomic<TestPayload> atomicValue(TestPayload::New(0, 0));

  for (auto _ : state) {
    if (state.thread_index() == 0) {
      if (auto i = RandomGenerator::Rand(0, 20); i == 0) {
        atomicValue.store(TestPayload::New(RandomGenerator::Rand(0, 100),
                                           RandomGenerator::Rand(100, 200)),
                          std::memory_order_release);
      }
    } else {
      atomicValue.load();
    }
  }
}

BENCHMARK(BenchSwmrOptimisticGuard)->Threads(8);
BENCHMARK(BenchSwmrAtomicValue)->Threads(8);
BENCHMARK(BenchU8ToPage);
BENCHMARK(BenchPageDirectly);
BENCHMARK(BenchStdArray);
BENCHMARK(BenchVecArray);
BENCHMARK(BenchRawArray);

BENCHMARK_MAIN();