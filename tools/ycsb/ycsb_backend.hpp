#pragma once

#include "leanstore/cpp/base/result.hpp"
#include "tools/ycsb/ycsb_backend_leanstore.hpp"
#include "tools/ycsb/ycsb_backend_rocksdb.hpp"
#include "tools/ycsb/ycsb_backend_wiredtiger.hpp"

#include <memory>
#include <string_view>
#include <variant>

namespace leanstore::ycsb {

// API for YcsbDb, YcsbSession, YcsbKvSpace
class YcsbDb;
class YcsbSession;
class YcsbKvSpace;

// For LeanStore
class YcsbLeanDb;
class YcsbLeanSession;
class YcsbLeanAtomicKvSpace;
class YcsbLeanMvccKvSpace;

// For WiredTiger
class YcsbWtDb;
class YcsbWtSession;
class YcsbWtKvSpace;

// For RocksDB
class YcsbRocksDb;
class YcsbRocksSession;
class YcsbRocksKvSpace;

class YcsbDb {
public:
  using AnyDb = std::variant<std::unique_ptr<YcsbLeanDb>, std::unique_ptr<YcsbWtDb>,
                             std::unique_ptr<YcsbRocksDb>>;

  static Result<std::unique_ptr<YcsbDb>> Create(const YcsbOptions& options) {
    if (options.IsBenchBasicKv() || options.IsBenchTransactionKv()) {
      auto res = YcsbLeanDb::Create(options);
      if (!res) {
        return std::move(res.error());
      }
      return std::make_unique<YcsbDb>(std::move(res.value()));
    }

    if (options.IsBenchWiredTiger()) {
      auto res = YcsbWtDb::Create(options);
      if (!res) {
        return std::move(res.error());
      }
      return std::make_unique<YcsbDb>(std::move(res.value()));
    }

    if (options.IsBenchRocksDb()) {
      auto res = YcsbRocksDb::Create(options);
      if (!res) {
        return std::move(res.error());
      }
      return std::make_unique<YcsbDb>(std::move(res.value()));
    }

    return Error::General("Unknown storage engine option: " + options.backend_);
  }

  explicit YcsbDb(std::unique_ptr<YcsbLeanDb> db_impl) : db_impl_(std::move(db_impl)) {
  }

  explicit YcsbDb(std::unique_ptr<YcsbWtDb> db_impl) : db_impl_(std::move(db_impl)) {
  }

  explicit YcsbDb(std::unique_ptr<YcsbRocksDb> db_impl) : db_impl_(std::move(db_impl)) {
  }

  YcsbSession NewSession();

private:
  AnyDb db_impl_;
};

class YcsbSession {
public:
  using AnySession = std::variant<std::unique_ptr<YcsbLeanSession>, std::unique_ptr<YcsbWtSession>,
                                  std::unique_ptr<YcsbRocksSession>>;

  explicit YcsbSession(std::unique_ptr<YcsbLeanSession> session)
      : session_impl_(std::move(session)) {
  }

  explicit YcsbSession(std::unique_ptr<YcsbWtSession> session) : session_impl_(std::move(session)) {
  }

  explicit YcsbSession(std::unique_ptr<YcsbRocksSession> session)
      : session_impl_(std::move(session)) {
  }

  Result<void> CreateKvSpace(std::string_view name) {
    return std::visit([name](auto& session) { return session->CreateKvSpace(name); },
                      session_impl_);
  }

  Result<YcsbKvSpace> GetKvSpace(std::string_view name);

private:
  AnySession session_impl_;
};

class YcsbKvSpace {
public:
  using AnyKvSpace =
      std::variant<std::unique_ptr<YcsbLeanAtomicKvSpace>, std::unique_ptr<YcsbLeanMvccKvSpace>,
                   std::unique_ptr<YcsbWtKvSpace>, std::unique_ptr<YcsbRocksKvSpace>>;

  explicit YcsbKvSpace(std::unique_ptr<YcsbLeanAtomicKvSpace> kv_space)
      : kv_space_(std::move(kv_space)) {
  }

  explicit YcsbKvSpace(std::unique_ptr<YcsbLeanMvccKvSpace> kv_space)
      : kv_space_(std::move(kv_space)) {
  }

  explicit YcsbKvSpace(std::unique_ptr<YcsbWtKvSpace> kv_space) : kv_space_(std::move(kv_space)) {
  }

  explicit YcsbKvSpace(std::unique_ptr<YcsbRocksKvSpace> kv_space)
      : kv_space_(std::move(kv_space)) {
  }

  Result<void> Put(std::string_view key, std::string_view value) {
    return std::visit([&](auto&& kv_space) { return kv_space->Put(key, value); }, kv_space_);
  }

  Result<void> Update(std::string_view key, std::string_view value) {
    return std::visit([&](auto&& kv_space) { return kv_space->Update(key, value); }, kv_space_);
  }

  Result<void> Get(std::string_view key, std::string& value_out) {
    return std::visit([&](auto&& kv_space) { return kv_space->Get(key, value_out); }, kv_space_);
  }

private:
  AnyKvSpace kv_space_;
};

} // namespace leanstore::ycsb