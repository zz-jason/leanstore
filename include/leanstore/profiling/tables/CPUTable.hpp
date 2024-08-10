#pragma once

#include "ProfilingTable.hpp"

namespace leanstore::profiling {

class CPUTable : public ProfilingTable {
public:
  std::unordered_map<std::string, double> workers_agg_events, pp_agg_events, ww_agg_events;
  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace leanstore::profiling
