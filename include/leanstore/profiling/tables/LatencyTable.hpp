#pragma once
#include "ProfilingTable.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"

namespace leanstore {
namespace profiling {

using namespace storage;

class LatencyTable : public ProfilingTable {
public:
  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace profiling
} // namespace leanstore
