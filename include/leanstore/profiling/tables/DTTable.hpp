#pragma once

#include "ProfilingTable.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"

namespace leanstore::profiling {

class DTTable : public ProfilingTable {
private:
  std::string dt_name;
  TREEID mTreeId;
  leanstore::storage::BufferManager& bm;

public:
  DTTable(leanstore::storage::BufferManager& bm);

  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace leanstore::profiling
