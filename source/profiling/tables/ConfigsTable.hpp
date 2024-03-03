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
   uint64_t hash();
   void add(std::string name, std::string value);
};
}  // namespace profiling
}  // namespace leanstore
