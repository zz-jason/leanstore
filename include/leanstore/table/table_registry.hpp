#pragma once

#include "coroutine/lean_mutex.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/table/table.hpp"

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace leanstore {

class TableRegistry {
public:
  TableRegistry() = default;
  ~TableRegistry() = default;

  Result<Table*> Register(std::unique_ptr<Table> table);
  Result<std::unique_ptr<Table>> Drop(std::string_view name);
  Table* Get(std::string_view name);

  template <typename Visitor>
  void Visit(Visitor&& visitor) {
    LEAN_SHARED_LOCK(mutex_);
    visitor(tables_);
  }

private:
  mutable LeanSharedMutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
};

} // namespace leanstore
