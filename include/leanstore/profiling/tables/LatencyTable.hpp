#pragma once

#include "ProfilingTable.hpp"

namespace leanstore::profiling {

class LatencyTable : public ProfilingTable {
public:
  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace leanstore::profiling
