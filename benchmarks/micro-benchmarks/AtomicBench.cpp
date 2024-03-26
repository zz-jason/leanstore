
#include <benchmark/benchmark.h>

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>

namespace leanstore::benchmark {

class UnorderedMapBenchFixture : public ::benchmark::Fixture {
public:
  UnorderedMapBenchFixture() {
    // prepare the map
    for (uint64_t i = 0; i < 1000000; i++) {
      mMap[i] = i;
    }
  }

protected:
  std::shared_mutex mMutex;
  std::unordered_map<uint64_t, uint64_t> mMap;
};

BENCHMARK_F(UnorderedMapBenchFixture, Get)(::benchmark::State& state) {
  for (auto _ : state) {
    std::shared_lock readGuard{mMutex};
    mMap[rand() % 1000000];
  }
}

BENCHMARK_REGISTER_F(UnorderedMapBenchFixture, Get)->Threads(1);
BENCHMARK_REGISTER_F(UnorderedMapBenchFixture, Get)->Threads(2);
BENCHMARK_REGISTER_F(UnorderedMapBenchFixture, Get)->Threads(4);
BENCHMARK_REGISTER_F(UnorderedMapBenchFixture, Get)->Threads(8);
BENCHMARK_REGISTER_F(UnorderedMapBenchFixture, Get)->Threads(12);

} // namespace leanstore::benchmark

BENCHMARK_MAIN();