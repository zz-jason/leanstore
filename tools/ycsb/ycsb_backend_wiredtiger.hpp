#pragma once

#include "leanstore/base/result.hpp"
#include "tools/ycsb/console_logger.hpp"
#include "tools/ycsb/ycsb_options.hpp"

#include <wiredtiger.h>

#include <memory>
#include <string>
#include <string_view>

namespace leanstore::ycsb {

class YcsbDb;
class YcsbSession;
class YcsbKvSpace;

class YcsbWtDb {
public:
  static Result<std::unique_ptr<YcsbWtDb>> Create(const YcsbOptions& options);

  explicit YcsbWtDb(WT_CONNECTION* conn) : conn_(conn) {
  }

  ~YcsbWtDb() {
    if (conn_ != nullptr) {
      int ret = conn_->close(conn_, nullptr);
      if (ret != 0) {
        ConsoleFatal(std::format("Failed to close wiredtiger: {}", wiredtiger_strerror(ret)));
      }
      conn_ = nullptr;
    }
  }

  YcsbSession NewSession();

private:
  WT_CONNECTION* conn_;
};

class YcsbWtSession {
public:
  explicit YcsbWtSession(WT_SESSION* session) : session_(session) {
  }

  ~YcsbWtSession() {
    if (session_ != nullptr) {
      int ret = session_->close(session_, nullptr);
      if (ret != 0) {
        ConsoleFatal(
            std::format("Failed to close wiredtiger session: {}", wiredtiger_strerror(ret)));
      }
      session_ = nullptr;
    }
  }

  Result<void> CreateKvSpace(std::string_view name);
  Result<YcsbKvSpace> GetKvSpace(std::string_view name);

private:
  WT_SESSION* session_;
};

class YcsbWtKvSpace {
public:
  explicit YcsbWtKvSpace(WT_CURSOR* cursor) : cursor_(cursor) {
    key_item_.data = nullptr;
    key_item_.size = 0;
    val_item_.data = nullptr;
    val_item_.size = 0;
  }

  ~YcsbWtKvSpace() {
    if (cursor_ != nullptr) {
      cursor_->close(cursor_);
      cursor_ = nullptr;
    }
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  static void ResetItem(WT_ITEM& item, std::string_view new_item) {
    item.data = new_item.data();
    item.size = new_item.size();
  }

  WT_CURSOR* cursor_;
  WT_ITEM key_item_;
  WT_ITEM val_item_;
};

} // namespace leanstore::ycsb