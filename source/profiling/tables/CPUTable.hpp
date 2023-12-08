#pragma once

#include "ProfilingTable.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

namespace leanstore {
namespace profiling {

using namespace storage;
class CPUTable : public ProfilingTable {
public:
  std::unordered_map<std::string, double> workers_agg_events, pp_agg_events,
      ww_agg_events;
  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace profiling
} // namespace leanstore
