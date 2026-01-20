#include "tools/ycsb/ycsb_backend.hpp"

#include "leanstore/cpp/base/result.hpp"
#include "tools/ycsb/ycsb_backend_leanstore.hpp"
#include "tools/ycsb/ycsb_backend_rocksdb.hpp"
#include "tools/ycsb/ycsb_backend_wiredtiger.hpp"

#include <string_view>
#include <variant>

namespace leanstore::ycsb {

YcsbSession YcsbDb::NewSession() {
  return std::visit([&](auto& db) { return db->NewSession(); }, db_impl_);
}

Result<YcsbKvSpace> YcsbSession::GetKvSpace(std::string_view name) {
  auto future =
      std::visit([name](auto& session) { return session->GetKvSpace(name); }, session_impl_);
  if (!future) {
    return std::move(future.error());
  }
  return std::move(future.value());
}

} // namespace leanstore::ycsb