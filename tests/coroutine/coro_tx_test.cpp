#include "coroutine/coro_env.hpp"
#include "coroutine/coro_executor.hpp"
#include "coroutine/coro_session.hpp"
#include "lean_test_suite.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>

namespace leanstore::test {

class CoroTxTest : public LeanTestSuite {
protected:
  static constexpr auto kTestDirPattern = "/tmp/leanstore/{}/{}";
  static constexpr auto kBtreeName = "coro_txn_test";
  static constexpr auto kNumKeys = 100;
  static constexpr auto kKeyPattern = "key_btree_LL_xxxxxxxxxxxx_{}";
  static constexpr auto kValPattern = "VAL_BTREE_LL_YYYYYYYYYYYY_{}";
  static constexpr auto kEnableWal = true;
  static constexpr auto kBtreeConfig = lean_btree_config{
      .enable_wal_ = kEnableWal,
      .use_bulk_insert_ = false,
  };
};

TEST_F(CoroTxTest, BasicCommit) {
  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = kEnableWal;
  option->worker_threads_ = 2;
  auto res = LeanStore::Open(option);
  ASSERT_TRUE(res);
  auto store = std::move(res.value());
  ASSERT_NE(store, nullptr);

  // prepare key-value pairs to insert
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string key(std::format(kKeyPattern, i));
    std::string val(std::format(kValPattern, i));
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  // create btree for table records
  auto* coro_session_0 = store->GetCoroScheduler().ReserveCoroSession(0);
  TransactionKV* btree = nullptr;
  auto job_create_btree = [&]() {
    auto res = store->CreateTransactionKV(kBtreeName, kBtreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
    btree = res.value();
  };
  store->GetCoroScheduler().Submit(coro_session_0, std::move(job_create_btree))->Wait();

  // insert key-value pairs in worker 0
  auto job_insert = [&]() {
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }
  };
  store->GetCoroScheduler().Submit(coro_session_0, std::move(job_insert))->Wait();

  store->GetCoroScheduler().ReleaseCoroSession(coro_session_0);
}

TEST_F(CoroTxTest, BasicSnapshotIsolation) {
  lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->enable_wal_ = kEnableWal;
  option->worker_threads_ = 2;
  auto res = LeanStore::Open(option);
  ASSERT_TRUE(res);
  auto store = std::move(res.value());
  ASSERT_NE(store, nullptr);

  auto* coro_session_0 = store->GetCoroScheduler().ReserveCoroSession(0);
  auto* coro_session_1 = store->GetCoroScheduler().ReserveCoroSession(1);

  // create btree for table records
  TransactionKV* btree = nullptr;
  auto job_create_btree = [&]() {
    auto res = store->CreateTransactionKV(kBtreeName, kBtreeConfig);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
    btree = res.value();
  };
  store->GetCoroScheduler().Submit(coro_session_0, std::move(job_create_btree))->Wait();

  // insert initial values
  auto job_insert = [&]() {
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree->Insert("k1", "v1"), OpCode::kOK);
    EXPECT_EQ(btree->Insert("k2", "v2"), OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
  };
  store->GetCoroScheduler().Submit(coro_session_0, std::move(job_insert))->Wait();

  std::string copied_value{""};
  auto copy_value_out = [&](Slice val) {
    copied_value = std::string((const char*)val.data(), val.size());
  };

  // start two sessions
  store->GetCoroScheduler()
      .Submit(coro_session_0,
              [&]() {
                CoroEnv::CurTxMgr().StartTx();
                EXPECT_EQ(btree->Lookup("k1", copy_value_out), OpCode::kOK);
                EXPECT_EQ(copied_value, "v1");
                EXPECT_EQ(btree->Lookup("k2", copy_value_out), OpCode::kOK);
                EXPECT_EQ(copied_value, "v2");
              })
      ->Wait();

  store->GetCoroScheduler()
      .Submit(coro_session_1,
              [&]() {
                CoroEnv::CurTxMgr().StartTx();
                EXPECT_EQ(btree->Insert("k3", "v3"), OpCode::kOK);
                EXPECT_EQ(btree->Lookup("k3", copy_value_out), OpCode::kOK);
                EXPECT_EQ(copied_value, "v3");
                CoroEnv::CurTxMgr().CommitTx();

                CoroEnv::CurTxMgr().StartTx();
                EXPECT_EQ(btree->Lookup("k3", copy_value_out), OpCode::kOK);
                EXPECT_EQ(copied_value, "v3");
                CoroEnv::CurTxMgr().CommitTx();
              })
      ->Wait();

  store->GetCoroScheduler()
      .Submit(coro_session_0,
              [&]() {
                EXPECT_EQ(btree->Lookup("k3", copy_value_out), OpCode::kNotFound);
                CoroEnv::CurTxMgr().CommitTx();

                CoroEnv::CurTxMgr().StartTx();
                EXPECT_EQ(btree->Lookup("k3", copy_value_out), OpCode::kOK);
                EXPECT_EQ(copied_value, "v3");
                CoroEnv::CurTxMgr().CommitTx();
              })
      ->Wait();

  store->GetCoroScheduler().ReleaseCoroSession(coro_session_0);
  store->GetCoroScheduler().ReleaseCoroSession(coro_session_1);
}

} // namespace leanstore::test