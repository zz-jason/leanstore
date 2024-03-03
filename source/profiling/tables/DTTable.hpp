#pragma once

#include "ProfilingTable.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

namespace leanstore {
namespace profiling {

using namespace storage;

class DTTable : public ProfilingTable {
private:
  std::string dt_name;
  TREEID mTreeId;
  BufferManager& bm;

public:
  DTTable(BufferManager& bm);

  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace profiling
} // namespace leanstore
