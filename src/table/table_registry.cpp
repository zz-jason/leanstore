#include "leanstore/table/table_registry.hpp"

#include "leanstore/cpp/base/error.hpp"

namespace leanstore {

Result<Table*> TableRegistry::Register(std::unique_ptr<Table> table) {
  LEAN_UNIQUE_LOCK(mutex_);
  const auto& name = table->definition().name;
  auto [it, inserted] = tables_.emplace(name, std::move(table));
  if (!inserted) {
    return Error::General("table already exists: " + name);
  }
  return it->second.get();
}

Result<std::unique_ptr<Table>> TableRegistry::Drop(const std::string& name) {
  LEAN_UNIQUE_LOCK(mutex_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    return Error::General("table not found: " + name);
  }
  auto table = std::move(it->second);
  tables_.erase(it);
  return table;
}

Table* TableRegistry::Get(const std::string& name) {
  LEAN_SHARED_LOCK(mutex_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

} // namespace leanstore
