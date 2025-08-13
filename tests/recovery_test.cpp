#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/debug_flags.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <format>
#include <string>

#include <sys/socket.h>

using namespace leanstore::storage::btree;

namespace leanstore::test {

class RecoveryTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;

  RecoveryTest() {
    // Create a leanstore instance for the test case
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    auto* option = CreateStoreOption(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->enable_eager_gc_ = true;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~RecoveryTest() = default;
};

TEST_F(RecoveryTest, SerializeAndDeserialize) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  size_t num_keys(100);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  // create btree for table records
  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(btree_name);
    ASSERT_TRUE(res);
    btree = res.value();
    EXPECT_NE(btree, nullptr);
  });

  // insert some values
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
    }
  });

  Log::Info("Buffer Pool Before Shutdown:");
  store_->buffer_manager_->DoWithBufferFrameIf(
      [](leanstore::storage::BufferFrame& bf) { return !bf.IsFree(); },
      [](leanstore::storage::BufferFrame& bf [[maybe_unused]]) {
        Log::Info("pageId={}, treeId={}, isDirty={}", bf.header_.page_id_, bf.page_.btree_id_,
                  bf.IsDirty());
      });
  // meta file should be serialized during destructor.
  auto* store_option = CreateStoreOptionFrom(store_->store_option_);
  store_option->create_from_scratch_ = false;
  store_.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult.
  auto res = LeanStore::Open(store_option);
  EXPECT_TRUE(res);

  store_ = std::move(res.value());
  store_->GetTransactionKV(btree_name, &btree);
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      auto op_code = btree->Lookup(key, copy_value_out);
      EXPECT_EQ(op_code, OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });

  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    store_->DropTransactionKV(btree_name);
  });

  store_ = nullptr;
}

TEST_F(RecoveryTest, RecoverAfterInsert) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

  TransactionKV* btree;

  // prepare key-value pairs to insert
  size_t num_keys(100);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_YYYYYYYYYYYY_" + std::to_string(i));
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert some values
    CoroEnv::CurTxMgr().StartTx();
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
    }
    CoroEnv::CurTxMgr().CommitTx();
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE(store_, "skip_CheckpointAllBufferFrames");
  SCOPED_DEFER({ LS_DEBUG_DISABLE(store_, "skip_CheckpointAllBufferFrames"); });
  auto* store_option = CreateStoreOptionFrom(store_->store_option_);
  store_option->create_from_scratch_ = false;
  store_.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  auto res = LeanStore::Open(store_option);
  EXPECT_TRUE(res);

  store_ = std::move(res.value());
  store_->GetTransactionKV(btree_name, &btree);
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(key, copy_value_out), OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });
}

// generate value in the {}-{} format
static std::string GenerateValue(int ordinal_prefix, size_t val_size) {
  auto prefix = std::format("{}-", ordinal_prefix);
  if (prefix.size() >= val_size) {
    return prefix.substr(0, val_size);
  }

  return prefix + utils::RandomGenerator::RandAlphString(val_size - prefix.size());
}

TEST_F(RecoveryTest, RecoverAfterUpdate) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

  TransactionKV* btree;

  // prepare key-value pairs to insert
  auto val_size = 120u;
  size_t num_keys(20);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val = GenerateValue(0, val_size);
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btree_name = "testTree1";

  // update all the values to this newVal
  const uint64_t update_desc_buf_size = UpdateDesc::Size(1);
  uint8_t update_desc_buf[update_desc_buf_size];
  auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
  update_desc->num_slots_ = 1;
  update_desc->update_slots_[0].offset_ = 0;
  update_desc->update_slots_[0].size_ = val_size;

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert some values
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    // update all the values
    for (size_t i = 0; i < num_keys; ++i) {
      auto& [key, val] = kv_to_test[i];
      auto update_call_back = [&](MutableSlice mut_raw_val) {
        std::memcpy(mut_raw_val.Data(), val.data(), mut_raw_val.Size());
      };
      // update each key 3 times
      for (auto j = 1u; j <= 3; j++) {
        val = GenerateValue(j, val_size);
        CoroEnv::CurTxMgr().StartTx();
        EXPECT_EQ(btree->UpdatePartial(key, update_call_back, *update_desc), OpCode::kOK);
        CoroEnv::CurTxMgr().CommitTx();
      }
    }
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE(store_, "skip_CheckpointAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE(store_, "skip_CheckpointAllBufferFrames"));
  auto* store_option = CreateStoreOptionFrom(store_->store_option_);
  store_option->create_from_scratch_ = false;
  store_.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  auto res = LeanStore::Open(store_option);
  EXPECT_TRUE(res);

  store_ = std::move(res.value());
  store_->GetTransactionKV(btree_name, &btree);
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) { copied_value = val.ToString(); };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      auto ret_code = btree->Lookup(key, copy_value_out);
      EXPECT_EQ(ret_code, OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });
}

TEST_F(RecoveryTest, RecoverAfterRemove) {
#ifndef DEBUG
  GTEST_SKIP() << "This test only works in debug mode";
#endif

  TransactionKV* btree;
  const auto* btree_name = "testTree1";

  // prepare key-value pairs to insert
  auto val_size = 120u;
  size_t num_keys(10);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val = utils::RandomGenerator::RandAlphString(val_size);
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert some values
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(key, val), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    // remove all the values
    for (size_t i = 0; i < num_keys; ++i) {
      auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Remove(key), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }
  });

  // skip dumpping buffer frames on exit
  LS_DEBUG_ENABLE(store_, "skip_CheckpointAllBufferFrames");
  SCOPED_DEFER(LS_DEBUG_DISABLE(store_, "skip_CheckpointAllBufferFrames"));
  auto* store_option = CreateStoreOptionFrom(store_->store_option_);
  store_.reset(nullptr);

  // recreate the store, it's expected that all the meta and pages are rebult
  // based on the WAL entries
  store_option->create_from_scratch_ = false;
  auto res = LeanStore::Open(std::move(store_option));
  EXPECT_TRUE(res);

  store_ = std::move(res.value());
  store_->GetTransactionKV(btree_name, &btree);
  EXPECT_NE(btree, nullptr);

  // lookup the restored btree
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(key, copy_value_out), OpCode::kNotFound);
    }
  });
}

} // namespace leanstore::test