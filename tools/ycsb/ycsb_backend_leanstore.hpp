#pragma once

#include "coroutine/coro_scheduler.hpp"
#include "coroutine/coro_session.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/cpp/base/result.hpp"
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
  uint64_t round_robin_counter_ = 0;
};

class YcsbLeanSession {
public:
  YcsbLeanSession(LeanStore& store, uint64_t runs_on, bool bench_transaction_kv)
      : store_(store),
        coro_session_(store_.GetCoroScheduler().TryReserveCoroSession(runs_on)),
        bench_transaction_kv_(bench_transaction_kv) {
    assert(coro_session_ != nullptr && "Failed to reserve CoroSession for YCSB session");
  }

  ~YcsbLeanSession() {
    store_.GetCoroScheduler().ReleaseCoroSession(coro_session_);
  }

  Result<void> CreateKvSpace(std::string_view name);
  Result<YcsbKvSpace> GetKvSpace(std::string_view name);

private:
  LeanStore& store_;
  CoroSession* coro_session_;
  bool bench_transaction_kv_;
};

class YcsbLeanAtomicKvSpace {
public:
  YcsbLeanAtomicKvSpace(CoroScheduler& scheduler, CoroSession& session, BasicKV& kv_space)
      : scheduler_(scheduler),
        session_(session),
        kv_space_(kv_space) {
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  CoroScheduler& scheduler_;
  CoroSession& session_;
  BasicKV& kv_space_;
  std::vector<uint8_t> temp_buffer_;
};

class YcsbLeanMvccKvSpace {
public:
  YcsbLeanMvccKvSpace(CoroScheduler& scheduler, CoroSession& session, TransactionKV& kv_space)
      : scheduler_(scheduler),
        session_(session),
        kv_space_(kv_space) {
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  CoroScheduler& scheduler_;
  CoroSession& session_;
  TransactionKV& kv_space_;
  std::vector<uint8_t> temp_buffer_;
};

} // namespace leanstore::ycsb