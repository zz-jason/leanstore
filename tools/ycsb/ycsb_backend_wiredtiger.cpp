#include "tools/ycsb/ycsb_backend_wiredtiger.hpp"

#include "tools/ycsb/console_logger.hpp"
#include "tools/ycsb/ycsb_backend.hpp"

#include <format>

namespace leanstore::ycsb {

//------------------------------------------------------------------------------
// YcsbWtDb
//------------------------------------------------------------------------------

Result<std::unique_ptr<YcsbWtDb>> YcsbWtDb::Create(const YcsbOptions& options) {
  auto data_dir = options.DataDir();
  std::string config_string("create, direct_io=[data, log, checkpoint], "
                            "log=(enabled=true,archive=true), statistics_log=(wait=1), "
                            "statistics=(all, clear), session_max=2000, eviction=(threads_max=" +
                            std::to_string(options.workers_) +
                            "), cache_size=" + std::to_string(options.dram_ * 1024) + "M");

  WT_CONNECTION* conn;
  int ret = wiredtiger_open(data_dir.c_str(), nullptr, config_string.c_str(), &conn);
  if (ret != 0) {
    return Error::General(std::format("Failed to open wiredtiger: {}", wiredtiger_strerror(ret)));
  }
  return std::make_unique<YcsbWtDb>(conn);
}

YcsbSession YcsbWtDb::NewSession() {
  WT_SESSION* session;
  int ret = conn_->open_session(conn_, nullptr, nullptr, &session);
  if (ret != 0) {
    ConsoleFatal(std::format("Failed to open wiredtiger session: {}", wiredtiger_strerror(ret)));
  }
  return YcsbSession(std::make_unique<YcsbWtSession>(session));
}

//------------------------------------------------------------------------------
// YcsbWtSession
//------------------------------------------------------------------------------

Result<void> YcsbWtSession::CreateKvSpace(std::string_view name) {
  const char* config_string = "key_format=S,value_format=S";
  int ret = session_->create(session_, name.data(), config_string);
  if (ret != 0) {
    return Error::General(
        std::format("WiredTiger Create table failed: {}", wiredtiger_strerror(ret)));
  }
  return {};
}

Result<YcsbKvSpace> YcsbWtSession::GetKvSpace(std::string_view name) {
  WT_CURSOR* cursor;
  int ret = session_->open_cursor(session_, name.data(), nullptr, "raw", &cursor);
  if (ret != 0) {
    return Error::General(std::format("Failed to open cursor: {}", wiredtiger_strerror(ret)));
  }
  return YcsbKvSpace(std::make_unique<YcsbWtKvSpace>(cursor));
}

//------------------------------------------------------------------------------
// YcsbWtKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbWtKvSpace::Put(std::string_view key, std::string_view value) {
  ResetItem(key_item_, key);
  ResetItem(val_item_, value);
  cursor_->set_key(cursor_, &key_item_);
  cursor_->set_value(cursor_, &val_item_);
  int ret = cursor_->insert(cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Insert failed: {}", wiredtiger_strerror(ret)));
  }
  return {};
}

Result<void> YcsbWtKvSpace::Update(std::string_view key, std::string_view value) {
  ResetItem(key_item_, key);
  ResetItem(val_item_, value);
  cursor_->set_key(cursor_, &key_item_);
  cursor_->set_value(cursor_, &val_item_);

  int ret = cursor_->update(cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Update failed: {}", wiredtiger_strerror(ret)));
  }
  return {};
}

Result<void> YcsbWtKvSpace::Get(std::string_view key, std::string& value_out) {
  ResetItem(key_item_, key);
  cursor_->set_key(cursor_, &key_item_);
  int ret = cursor_->search(cursor_);
  if (ret != 0) {
    return Error::General(std::format("WiredTiger Lookup failed: {}", wiredtiger_strerror(ret)));
  }

  WT_ITEM val_read;
  cursor_->get_value(cursor_, &val_read);

  value_out.assign(static_cast<const char*>(val_read.data), val_read.size);
  return {};
}

} // namespace leanstore::ycsb