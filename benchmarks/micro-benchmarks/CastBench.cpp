#include "storage/buffer-manager/BufferFrame.hpp"
#include "utils/Misc.hpp"

#include <benchmark/benchmark.h>

using namespace leanstore;
using namespace leanstore::utils;
using namespace leanstore::storage;

static void BenchU8ToPage(benchmark::State& state) {
  AlignedBuffer<512> alignedBuffer(FLAGS_page_size * 4);
  auto buf = alignedBuffer.Get();
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

BENCHMARK(BenchU8ToPage);
BENCHMARK(BenchPageDirectly);
BENCHMARK_MAIN();