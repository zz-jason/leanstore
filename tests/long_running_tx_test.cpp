#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/history_storage.hpp"
#include "leanstore/concurrency/tx_manager.hpp"
#include "leanstore/cpp/base/defer.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

using namespace leanstore::utils;
using namespace leanstore;

namespace leanstore::test {

class LongRunningTxTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;
  std::string tree_name_;
  TransactionKV* kv_;

protected:
  LongRunningTxTest() = default;

  ~LongRunningTxTest() = default;

  void SetUp() override {
    // Create a leanstore instance for the test case
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = std::string("/tmp/leanstore/") + cur_test_name;

    lean_store_option* option = lean_store_option_create(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    option->enable_eager_gc_ = true;

    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    store_ = std::move(res.value());

    // TxManager 0, create a btree for test
    tree_name_ = RandomGenerator::RandAlphString(10);
    store_->ExecSync(0, [&]() {
      auto res = store_->CreateTransactionKV(tree_name_);
      ASSERT_TRUE(res);
      kv_ = res.value();
      ASSERT_NE(kv_, nullptr);
    });

    // TxManager 0, do extra insert and remove transactions in worker 0 to make it
    // have more than one entries in the commit log, which helps to advance the
    // global lower watermarks for garbage collection
    store_->ExecSync(0, [&]() {
      CoroEnv::CurTxMgr().StartTx();
      ASSERT_EQ(kv_->Insert(Slice("0"), Slice("0")), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();

      CoroEnv::CurTxMgr().StartTx();
      ASSERT_EQ(kv_->Remove(Slice("0")), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    });
  }

  void TearDown() override {
    // TxManager 0, remove the btree
    store_->ExecSync(0, [&]() {
      CoroEnv::CurTxMgr().StartTx();
      LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());
      store_->DropTransactionKV(tree_name_);
    });
  }
};

// TODO(lookup from graveyard)
TEST_F(LongRunningTxTest, LookupFromGraveyard) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copied_val;
  auto copy_value = [&](Slice val) {
    copied_val = std::string((const char*)val.data(), val.size());
  };

  // Insert 2 key-values as the test base.
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(kv_->Insert(key1, Slice(val1)), OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();

    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(kv_->Insert(key2, Slice(val2)), OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
  });

  store_->ExecSync(1, [&]() { CoroEnv::CurTxMgr().StartTx(); });

  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning);

    // get the old value in worker 2
    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);
  });

  // remove the key in worker 1
  store_->ExecSync(1, [&]() {
    EXPECT_EQ(kv_->Remove(key1), OpCode::kOK);
    EXPECT_EQ(kv_->Remove(key2), OpCode::kOK);
  });

  // get the old value in worker 2
  store_->ExecSync(2, [&]() {
    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);
  });

  // commit the transaction in worker 1, after garbage collection when
  // committing the transaction, tombstones should be moved to the graveyard.
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(kv_->graveyard_->CountEntries(), 2u);
  });

  // lookup from graveyard, still get the old value in worker 2
  store_->ExecSync(2, [&]() {
    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);

    // commit the transaction in worker 2
    CoroEnv::CurTxMgr().CommitTx();
  });

  // now worker 2 can not get the old value
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning, IsolationLevel::kSnapshotIsolation);
    LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());

    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kNotFound);
    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kNotFound);
  });
}

TEST_F(LongRunningTxTest, LookupAfterUpdate100Times) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copied_val;
  auto copy_value = [&](Slice val) {
    copied_val = std::string((const char*)val.data(), val.size());
  };

  // Work 1, insert 2 key-values as the test base
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(kv_->Insert(key1, Slice(val1)), OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();

    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(kv_->Insert(key2, Slice(val2)), OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
  });

  // TxManager 1, start a short-running transaction
  store_->ExecSync(1, [&]() { CoroEnv::CurTxMgr().StartTx(); });

  // TxManager 2, start a long-running transaction, lookup, get the old value
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning);

    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);
  });

  // TxManager 1, update key1 100 times with random values
  std::string new_val;
  store_->ExecSync(1, [&]() {
    auto update_desc_buf_size = UpdateDesc::Size(1);
    uint8_t update_desc_buf[update_desc_buf_size];
    auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
    update_desc->num_slots_ = 1;
    update_desc->update_slots_[0].offset_ = 0;
    update_desc->update_slots_[0].size_ = val1.size();

    auto update_call_back = [&](MutableSlice to_update) {
      auto new_val_size = update_desc->update_slots_[0].size_;
      new_val = RandomGenerator::RandAlphString(new_val_size);
      std::memcpy(to_update.data(), new_val.data(), new_val.size());
    };

    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(kv_->UpdatePartial(key1, update_call_back, *update_desc), OpCode::kOK);
    }
  });

  // TxManager 2, lookup, get the old value
  store_->ExecSync(2, [&]() {
    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);
  });

  // TxManager 1, commit the transaction, graveyard should be empty, update history
  // trees should have 100 versions
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().CommitTx();

    EXPECT_EQ(kv_->graveyard_->CountEntries(), 0u);
    auto* update_tree = CoroEnv::CurTxMgr().cc_.history_storage_.GetUpdateIndex();
    auto* remove_tree = CoroEnv::CurTxMgr().cc_.history_storage_.GetRemoveIndex();
    EXPECT_EQ(update_tree->CountEntries(), 100u);
    EXPECT_EQ(remove_tree->CountEntries(), 0u);
  });

  // TxManager 2, lookup, skip the update versions, still get old values, commit
  store_->ExecSync(2, [&]() {
    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val1);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);

    // commit the transaction in worker 2
    CoroEnv::CurTxMgr().CommitTx();
  });

  // TxManager 2, now get the updated new value
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning);
    LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());

    EXPECT_EQ(kv_->Lookup(key1, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, new_val);

    EXPECT_EQ(kv_->Lookup(key2, copy_value), OpCode::kOK);
    EXPECT_EQ(copied_val, val2);
  });
}

TEST_F(LongRunningTxTest, ScanAscFromGraveyard) {
  // randomly generate 100 unique key-values for s1 to insert
  size_t num_kv = 100;
  std::unordered_map<std::string, std::string> kv_to_test;
  std::string smallest_key;
  for (size_t i = 0; i < num_kv; ++i) {
    std::string key = RandomGenerator::RandAlphString(10);
    std::string val = RandomGenerator::RandAlphString(10);
    if (kv_to_test.find(key) != kv_to_test.end()) {
      --i;
      continue;
    }

    // update the smallest key
    kv_to_test[key] = val;
    if (smallest_key.empty() || smallest_key > key) {
      smallest_key = key;
    }
  }

  // insert the key-values in worker 0
  store_->ExecSync(0, [&]() {
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());
      EXPECT_EQ(kv_->Insert(key, Slice(val)), OpCode::kOK);
    }
  });

  // start transaction on worker 2, get the inserted values
  std::string copied_key, copied_val;
  auto copy_key_val = [&](Slice key, Slice val) {
    copied_key = std::string((const char*)key.data(), key.size());
    copied_val = std::string((const char*)val.data(), val.size());
    EXPECT_EQ(copied_val, kv_to_test[copied_key]);
    return true;
  };
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning);
    EXPECT_EQ(kv_->ScanAsc(Slice(smallest_key), copy_key_val), OpCode::kOK);
  });

  // remove the key-values in worker 1
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    for (const auto& [key, val] : kv_to_test) {
      EXPECT_EQ(kv_->Remove(key), OpCode::kOK);
    }
  });

  // get the old values in worker 2
  store_->ExecSync(
      2, [&]() { EXPECT_EQ(kv_->ScanAsc(Slice(smallest_key), copy_key_val), OpCode::kOK); });

  // commit the transaction in worker 1, all the removed key-values should be
  // moved to graveyard
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(kv_->graveyard_->CountEntries(), kv_to_test.size());
  });

  // still get the old values in worker 2
  store_->ExecSync(2, [&]() {
    EXPECT_EQ(kv_->ScanAsc(Slice(smallest_key), copy_key_val), OpCode::kOK);

    // commit the transaction in worker 2
    CoroEnv::CurTxMgr().CommitTx();
  });

  // now worker 2 can not get the old values
  store_->ExecSync(2, [&]() {
    CoroEnv::CurTxMgr().StartTx(TxMode::kLongRunning);
    LEAN_DEFER(CoroEnv::CurTxMgr().CommitTx());
    EXPECT_EQ(kv_->ScanAsc(Slice(smallest_key), copy_key_val), OpCode::kOK);
  });
}

} // namespace leanstore::test
