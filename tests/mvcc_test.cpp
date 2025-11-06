#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <memory>

using namespace leanstore::utils;
using namespace leanstore;

namespace leanstore::test {

class MvccTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;
  std::string tree_name_;
  TransactionKV* btree_;

protected:
  // create a leanstore instance for current test case
  MvccTest() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    lean_store_option* option = lean_store_option_create(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~MvccTest() = default;

  void SetUp() override {
    // create a btree name for test
    tree_name_ = RandomGenerator::RandAlphString(10);
    store_->ExecSync(0, [&]() {
      auto res = store_->CreateTransactionKV(tree_name_);
      ASSERT_TRUE(res);
      btree_ = res.value();
      ASSERT_NE(btree_, nullptr);
    });
  }

  void TearDown() override {
    store_->ExecSync(1, [&]() {
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
      store_->DropTransactionKV(tree_name_);
    });
  }
};

TEST_F(MvccTest, LookupWhileInsert) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key0.data(), key0.size()),
                              Slice((const uint8_t*)val0.data(), val0.size()));
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert another record, don't commit
  auto key1 = RandomGenerator::RandAlphString(17);
  auto val1 = RandomGenerator::RandAlphString(131);
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to lookup the base record
  // the lookup should not be blocked
  store_->ExecSync(2, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree_->Lookup(Slice((const uint8_t*)key0.data(), key0.size()), copy_value_out),
              OpCode::kOK);
    EXPECT_EQ(copied_value, val0);
    CoroEnv::CurTxMgr().CommitTx();
  });

  // commit the transaction
  store_->ExecSync(1, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(btree_->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copy_value_out),
              OpCode::kOK);
    EXPECT_EQ(copied_value, val1);
    CoroEnv::CurTxMgr().CommitTx();
  });

  // now we can see the latest record
  store_->ExecSync(2, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree_->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copy_value_out),
              OpCode::kOK);
    EXPECT_EQ(copied_value, val1);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(MvccTest, InsertConflict) {
  // insert a base record
  auto key0 = RandomGenerator::RandAlphString(42);
  auto val0 = RandomGenerator::RandAlphString(151);
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key0.data(), key0.size()),
                              Slice((const uint8_t*)val0.data(), val0.size()));
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start a transaction to insert a bigger key, don't commit
  auto key1 = key0 + "a";
  auto val1 = val0;
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kOK);
  });

  // start another transaction to insert the same key
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    CoroEnv::CurTxMgr().AbortTx();
  });

  // start another transaction to insert a smaller key
  auto key2 = std::string(key0.data(), key0.size() - 1);
  auto val2 = val0;
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    auto res = btree_->Insert(Slice((const uint8_t*)key1.data(), key1.size()),
                              Slice((const uint8_t*)val1.data(), val1.size()));
    EXPECT_EQ(res, OpCode::kAbortTx);
    CoroEnv::CurTxMgr().AbortTx();
  });

  // commit the transaction
  store_->ExecSync(1, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };

    EXPECT_EQ(btree_->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copy_value_out),
              OpCode::kOK);
    EXPECT_EQ(copied_value, val1);
    CoroEnv::CurTxMgr().CommitTx();
  });

  // now we can see the latest record
  store_->ExecSync(2, [&]() {
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree_->Lookup(Slice((const uint8_t*)key1.data(), key1.size()), copy_value_out),
              OpCode::kOK);
    EXPECT_EQ(copied_value, val1);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

} // namespace leanstore::test
