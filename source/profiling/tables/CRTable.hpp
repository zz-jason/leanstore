#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace profiling {
class CRTable : public ProfilingTable {
private:
  u64 wal_hits, wal_miss;
  double p1, p2, total, write, wal_total, wal_hit_pct, wal_miss_pct;

public:
  virtual std::string getName() override;
  virtual void open() override;
  virtual void next() override;
};
} // namespace profiling
} // namespace leanstore
