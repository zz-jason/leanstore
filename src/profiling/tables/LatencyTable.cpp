#include "LatencyTable.hpp"

#include "profiling/counters/CRCounters.hpp"
#include "utils/EnumerableThreadLocal.hpp"

namespace leanstore {
namespace profiling {

std::string LatencyTable::getName() {
  return "latency";
}

void LatencyTable::open() {
  columns.emplace("key", [](Column&) {});
  columns.emplace("tx_i", [](Column&) {});
  columns.emplace("cc_ms_precommit_latency", [](Column&) {});
  columns.emplace("cc_ms_commit_latency", [](Column&) {});
  columns.emplace("cc_flushes_counter", [](Column&) {});
  columns.emplace("cc_rfa_ms_precommit_latency", [](Column&) {});
  columns.emplace("cc_rfa_ms_commit_latency", [](Column&) {});
}

void LatencyTable::next() {
  clear();

  // utils::EnumerableThreadLocal<CRCounters> sCounters;
  utils::ForEach<CRCounters>(CRCounters::sCounters, [&](CRCounters* tls) {
    if (tls->mWorkerId.load() != -1) {
      return;
    }
    for (uint64_t i = 0; i < CRCounters::latency_tx_capacity; i++) {
      columns.at("key") << tls->mWorkerId.load();
      columns.at("i") << i;
      columns.at("cc_ms_precommit_latency")
          << tls->cc_ms_precommit_latency[i].load();
      columns.at("cc_ms_commit_latency") << tls->cc_ms_commit_latency[i].load();
      columns.at("cc_flushes_counter") << tls->cc_flushes_counter[i].load();
      columns.at("cc_rfa_ms_precommit_latency")
          << tls->cc_rfa_ms_precommit_latency[i].load();
      columns.at("cc_rfa_ms_commit_latency")
          << tls->cc_rfa_ms_commit_latency[i].load();
    }
  });
}

} // namespace profiling
} // namespace leanstore
