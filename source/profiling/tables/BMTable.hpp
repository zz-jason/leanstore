#pragma once

#include "ProfilingTable.hpp"

namespace leanstore {

namespace storage {

class BufferManager;

} // namespace storage
namespace profiling {

class BMTable : public ProfilingTable {
private:
  leanstore::storage::BufferManager& bm;

  int64_t local_phase_1_ms = 0;

  int64_t local_phase_2_ms = 0;

  int64_t local_phase_3_ms = 0;

  int64_t local_poll_ms = 0;

  int64_t total;

  uint64_t local_total_free;

  int64_t local_total_cool;

public:
  BMTable(leanstore::storage::BufferManager& bm);

  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};

} // namespace profiling
} // namespace leanstore
