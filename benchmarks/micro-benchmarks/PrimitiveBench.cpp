#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/sync/OptimisticGuarded.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

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
  auto pageSize = 4 * 1024;
  AlignedBuffer<512> alignedBuffer(pageSize * 4);
  auto* buf = alignedBuffer.Get();
  auto i = 1;
  for (auto _ : state) {
    reinterpret_cast<Page*>(&buf[i * pageSize])->mGSN = 1;
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
  static std::array<std::atomic<uint64_t>, 8> sTmpArray;
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((uint64_t)0, std::numeric_limits<uint64_t>::max());
      sTmpArray[i].store(val, std::memory_order_release);
    }
  }
}

static void BenchVecArray(benchmark::State& state) {
  static std::vector<std::atomic<uint64_t>> sTmpArray2(8);
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((uint64_t)0, std::numeric_limits<uint64_t>::max());
      sTmpArray2[i].store(val, std::memory_order_release);
    }
  }
}

static void BenchRawArray(benchmark::State& state) {
  static std::unique_ptr<std::atomic<uint64_t>[]> sTmpArray3 =
      std::make_unique<std::atomic<uint64_t>[]>(8);
  for (auto _ : state) {
    for (auto counter = 0; counter < 1000; counter++) {
      auto i = RandomGenerator::Rand(0, 8);
      auto val = RandomGenerator::Rand((uint64_t)0, std::numeric_limits<uint64_t>::max());
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
        guardedValue.Set(
            TestPayload::New(RandomGenerator::Rand(0, 100), RandomGenerator::Rand(100, 200)));
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
        atomicValue.store(
            TestPayload::New(RandomGenerator::Rand(0, 100), RandomGenerator::Rand(100, 200)),
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