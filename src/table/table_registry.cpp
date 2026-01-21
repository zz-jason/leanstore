#include "leanstore/table/table_registry.hpp"

#include "leanstore/base/error.hpp"

namespace leanstore {

Result<Table*> TableRegistry::Register(std::unique_ptr<Table> table) {
  LEAN_UNIQUE_LOCK(mutex_);
  const auto& name = table->Definition().name_;
  auto [it, inserted] = tables_.emplace(name, std::move(table));
  if (!inserted) {
    return Error::General("table already exists: " + name);
  }
  return it->second.get();
}

Result<std::unique_ptr<Table>> TableRegistry::Drop(std::string_view name) {
  LEAN_UNIQUE_LOCK(mutex_);
  auto it = tables_.find(std::string(name));
  if (it == tables_.end()) {
    return Error::General(std::string("table not found: ") + std::string(name));
  }
  auto table = std::move(it->second);
  tables_.erase(it);
  return table;
}

Table* TableRegistry::Get(std::string_view name) {
  LEAN_SHARED_LOCK(mutex_);
  auto it = tables_.find(std::string(name));
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

} // namespace leanstore
