#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"
#include "tools/ycsb/ycsb_options.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace leanstore::ycsb {

class YcsbDb;
class YcsbSession;
class YcsbKvSpace;

class YcsbLeanDb {
public:
  static Result<std::unique_ptr<YcsbLeanDb>> Create(const YcsbOptions& options);

  explicit YcsbLeanDb(std::unique_ptr<LeanStore> store, bool bench_transaction_kv)
      : store_(std::move(store)),
        bench_transaction_kv_(bench_transaction_kv) {
  }

  YcsbSession NewSession();

private:
  std::unique_ptr<LeanStore> store_;
  bool bench_transaction_kv_;
};

class YcsbLeanSession {
public:
  YcsbLeanSession(LeanStore& store, bool bench_transaction_kv)
      : session_(store.Connect()),
        bench_transaction_kv_(bench_transaction_kv) {
  }

  ~YcsbLeanSession() = default;

  Result<void> CreateKvSpace(std::string_view name);
  Result<YcsbKvSpace> GetKvSpace(std::string_view name);

private:
  LeanSession session_;
  bool bench_transaction_kv_;
};

class YcsbLeanAtomicKvSpace {
public:
  explicit YcsbLeanAtomicKvSpace(LeanBTree kv_space) : kv_space_(std::move(kv_space)) {
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  LeanBTree kv_space_;
};

class YcsbLeanMvccKvSpace {
public:
  YcsbLeanMvccKvSpace(LeanSession& session, LeanBTree kv_space)
      : session_(session),
        kv_space_(std::move(kv_space)) {
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  LeanSession& session_;
  LeanBTree kv_space_;
};

} // namespace leanstore::ycsb
