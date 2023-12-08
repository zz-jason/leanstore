#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class ConfigsTable : public ProfilingTable
{
  public:
   virtual std::string getName() override;
   virtual void open() override;
   virtual void next() override;
   u64 hash();
   void add(string name, string value);
};
}  // namespace profiling
}  // namespace leanstore
