#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

using namespace leanstore::utils;
using namespace leanstore;

namespace leanstore::test {

class TransactionKVTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;

  /// Create a leanstore instance for each test case
  TransactionKVTest() {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    auto* option = lean_store_option_create(("/tmp/leanstore/" + cur_test_name).c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 3;
    option->enable_eager_gc_ = true;
    auto res = LeanStore::Open(option);
    store_ = std::move(res.value());
  }

  ~TransactionKVTest() = default;
};

TEST_F(TransactionKVTest, Create) {
  // create leanstore btree for table records
  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(btree_name);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });

  // create btree with same should fail in the same worker
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(btree_name);
    EXPECT_FALSE(res);
  });

  // create btree with same should also fail in other workers
  store_->ExecSync(1, [&]() {
    auto res = store_->CreateTransactionKV(btree_name);
    EXPECT_FALSE(res);
  });

  // create btree with another different name should success
  const auto* btree_name2 = "testTree2";
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(btree_name2);
    EXPECT_TRUE(res);
    EXPECT_NE(res.value(), nullptr);
  });

  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    store_->DropTransactionKV(btree_name);
    store_->DropTransactionKV(btree_name2);
  });
}

TEST_F(TransactionKVTest, InsertAndLookup) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  size_t num_keys(10);
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key("key_btree_VI_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_VI_YYYYYYYYYYYY_" + std::to_string(i));
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
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
    CoroEnv::CurTxMgr().CommitTx();
  });

  // query on the created btree in the same worker
  store_->ExecSync(0, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });

  // query on the created btree in another worker
  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, expected_val] = kv_to_test[i];
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, expected_val);
    }
  });

  store_->ExecSync(1, [&]() {
    CoroEnv::CurTxMgr().StartTx();
    SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
    store_->DropTransactionKV(btree_name);
  });
}

TEST_F(TransactionKVTest, Insert1000KVs) {
  store_->ExecSync(0, [&]() {
    TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btree_name = "testTree1";

    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert numKVs tuples
    std::set<std::string> unique_keys;
    ssize_t num_keys(1000);
    CoroEnv::CurTxMgr().StartTx();
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
    }
    CoroEnv::CurTxMgr().CommitTx();

    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertDuplicates) {
  store_->ExecSync(0, [&]() {
    TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btree_name = "testTree1";

    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert numKVs tuples
    std::set<std::string> unique_keys;
    ssize_t num_keys(100);
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    // insert duplicated keys
    for (auto& key : unique_keys) {
      auto val = RandomGenerator::RandAlphString(128);
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(ToSlice(key), ToSlice(val)), OpCode::kDuplicated);
      CoroEnv::CurTxMgr().CommitTx();
    }

    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, Remove) {
  store_->ExecSync(0, [&]() {
    TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btree_name = "testTree1";

    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert numKVs tuples
    std::set<std::string> unique_keys;
    ssize_t num_keys(100);
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    for (auto& key : unique_keys) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    for (auto& key : unique_keys) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), [](Slice) {}),
                OpCode::kNotFound);
      CoroEnv::CurTxMgr().CommitTx();
    }

    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveNotExisted) {
  store_->ExecSync(0, [&]() {
    TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btree_name = "testTree1";

    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert numKVs tuples
    std::set<std::string> unique_keys;
    ssize_t num_keys(100);
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    // remove keys not existed
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);

      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kNotFound);
      CoroEnv::CurTxMgr().CommitTx();
    }

    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveFromOthers) {
  const auto* btree_name = "testTree1";
  std::set<std::string> unique_keys;
  TransactionKV* btree;

  store_->ExecSync(0, [&]() {
    // create leanstore btree for table records

    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert numKVs tuples
    ssize_t num_keys(100);
    for (ssize_t i = 0; i < num_keys; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (unique_keys.find(key) != unique_keys.end()) {
        i--;
        continue;
      }
      unique_keys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)val.data(), val.size())),
                OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }
  });

  store_->ExecSync(1, [&]() {
    // remove from another worker
    for (auto& key : unique_keys) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
    }

    // should not found any keys
    for (auto& key : unique_keys) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), [](Slice) {}),
                OpCode::kNotFound);
      CoroEnv::CurTxMgr().CommitTx();
    }
  });

  store_->ExecSync(0, [&]() {
    // lookup from another worker, should not found any keys
    for (auto& key : unique_keys) {
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), [](Slice) {}),
                OpCode::kNotFound);
      CoroEnv::CurTxMgr().CommitTx();
    }
  });

  store_->ExecSync(1, [&]() {
    // unregister the tree from another worker
    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, Update) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t num_keys(100);
  const size_t val_size = 120;
  std::vector<std::tuple<std::string, std::string>> kv_to_test;
  for (size_t i = 0; i < num_keys; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(val_size);
    kv_to_test.push_back(std::make_tuple(key, val));
  }

  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                               Slice((const uint8_t*)val.data(), val.size()));
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // update all the values to this newVal
    auto new_val = RandomGenerator::RandAlphString(val_size);
    auto update_call_back = [&](MutableSlice mut_raw_val) {
      std::memcpy(mut_raw_val.Data(), new_val.data(), mut_raw_val.Size());
    };

    // update in the same worker
    const uint64_t update_desc_buf_size = UpdateDesc::Size(1);
    uint8_t update_desc_buf[update_desc_buf_size];
    auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
    update_desc->num_slots_ = 1;
    update_desc->update_slots_[0].offset_ = 0;
    update_desc->update_slots_[0].size_ = val_size;
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()),
                                      update_call_back, *update_desc);
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // verify updated values
    std::string copied_value;
    auto copy_value_out = [&](Slice val) {
      copied_value = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < num_keys; ++i) {
      const auto& [key, val] = kv_to_test[i];
      CoroEnv::CurTxMgr().StartTx();
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(copied_value, new_val);
    }

    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanAsc) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t num_keys(100);
  const size_t val_size = 120;
  std::unordered_map<std::string, std::string> kv_to_test;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < num_keys; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(val_size);
    if (kv_to_test.find(key) != kv_to_test.end()) {
      i--;
      continue;
    }
    kv_to_test.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                               Slice((const uint8_t*)val.data(), val.size()));
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // scan in ascending order
    std::unordered_map<std::string, std::string> copied_k_vs;
    auto scan_call_back = [&](Slice key, Slice val) {
      copied_k_vs.emplace(std::string((const char*)key.data(), key.size()),
                          std::string((const char*)val.data(), val.size()));
      return true;
    };

    // scan from the smallest key
    copied_k_vs.clear();
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(
        btree->ScanAsc(Slice((const uint8_t*)smallest.data(), smallest.size()), scan_call_back),
        OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(copied_k_vs.size(), num_keys);
    for (const auto& [key, val] : copied_k_vs) {
      EXPECT_EQ(val, kv_to_test[key]);
    }

    // scan from the bigest key
    copied_k_vs.clear();
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree->ScanAsc(Slice((const uint8_t*)bigest.data(), bigest.size()), scan_call_back),
              OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(copied_k_vs.size(), 1u);
    EXPECT_EQ(copied_k_vs[bigest], kv_to_test[bigest]);

    // destroy the tree
    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanDesc) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t num_keys(100);
  const size_t val_size = 120;
  std::unordered_map<std::string, std::string> kv_to_test;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < num_keys; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(val_size);
    if (kv_to_test.find(key) != kv_to_test.end()) {
      i--;
      continue;
    }
    kv_to_test.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btree_name = "testTree1";

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                               Slice((const uint8_t*)val.data(), val.size()));
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // scan in descending order
    std::unordered_map<std::string, std::string> copied_k_vs;
    auto scan_call_back = [&](Slice key, Slice val) {
      copied_k_vs.emplace(std::string((const char*)key.data(), key.size()),
                          std::string((const char*)val.data(), val.size()));
      return true;
    };

    // scan from the bigest key
    copied_k_vs.clear();
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(btree->ScanDesc(Slice((const uint8_t*)bigest.data(), bigest.size()), scan_call_back),
              OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(copied_k_vs.size(), num_keys);
    for (const auto& [key, val] : copied_k_vs) {
      EXPECT_EQ(val, kv_to_test[key]);
    }

    // scan from the smallest key
    copied_k_vs.clear();
    CoroEnv::CurTxMgr().StartTx();
    EXPECT_EQ(
        btree->ScanDesc(Slice((const uint8_t*)smallest.data(), smallest.size()), scan_call_back),
        OpCode::kOK);
    CoroEnv::CurTxMgr().CommitTx();
    EXPECT_EQ(copied_k_vs.size(), 1u);
    EXPECT_EQ(copied_k_vs[smallest], kv_to_test[smallest]);

    // destroy the tree
    CoroEnv::CurTxMgr().StartTx();
    store_->DropTransactionKV(btree_name);
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertAfterRemove) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t num_keys(1);
  const size_t val_size = 120;
  std::unordered_map<std::string, std::string> kv_to_test;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < num_keys; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(val_size);
    if (kv_to_test.find(key) != kv_to_test.end()) {
      i--;
      continue;
    }
    kv_to_test.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btree_name = "InsertAfterRemove";
  std::string new_val = RandomGenerator::RandAlphString(val_size);
  std::string copied_value;
  auto copy_value_out = [&](Slice val) {
    copied_value = std::string((const char*)val.data(), val.size());
  };

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                               Slice((const uint8_t*)val.data(), val.size()));
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // remove, insert, and lookup
    for (const auto& [key, val] : kv_to_test) {
      // remove
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());

      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kOK);

      // remove twice should got not found error
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kNotFound);

      // update should fail
      const uint64_t update_desc_buf_size = UpdateDesc::Size(1);
      uint8_t update_desc_buf[update_desc_buf_size];
      auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
      update_desc->num_slots_ = 1;
      update_desc->update_slots_[0].offset_ = 0;
      update_desc->update_slots_[0].size_ = val_size;
      auto update_call_back = [&](MutableSlice mut_raw_val) {
        std::memcpy(mut_raw_val.Data(), new_val.data(), mut_raw_val.Size());
      };
      EXPECT_EQ(btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()),
                                     update_call_back, *update_desc),
                OpCode::kNotFound);

      // lookup should not found
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kNotFound);

      // insert with another val should success
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)new_val.data(), new_val.size())),
                OpCode::kOK);

      // lookup the new value should success
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, new_val);
    }
  });

  LEAN_DLOG("InsertAfterRemoveDifferentWorkers, key={}, val={}, newVal={}",
            kv_to_test.begin()->first, kv_to_test.begin()->second, new_val);
  store_->ExecSync(1, [&]() {
    // lookup the new value
    CoroEnv::CurTxMgr().StartTx();
    for (const auto& [key, val] : kv_to_test) {
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, new_val);
    }
    CoroEnv::CurTxMgr().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertAfterRemoveDifferentWorkers) {
  TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t num_keys(1);
  const size_t val_size = 120;
  std::unordered_map<std::string, std::string> kv_to_test;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < num_keys; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(val_size);
    if (kv_to_test.find(key) != kv_to_test.end()) {
      i--;
      continue;
    }
    kv_to_test.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btree_name = "InsertAfterRemoveDifferentWorkers";
  std::string new_val = RandomGenerator::RandAlphString(val_size);
  std::string copied_value;
  auto copy_value_out = [&](Slice val) {
    copied_value = std::string((const char*)val.data(), val.size());
  };

  store_->ExecSync(0, [&]() {
    // create btree
    auto res = store_->CreateTransactionKV(btree_name);
    btree = res.value();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      auto res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                               Slice((const uint8_t*)val.data(), val.size()));
      CoroEnv::CurTxMgr().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // remove
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kOK);
    }
  });

  store_->ExecSync(1, [&]() {
    for (const auto& [key, val] : kv_to_test) {
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());

      // remove twice should got not found error
      EXPECT_EQ(btree->Remove(Slice((const uint8_t*)key.data(), key.size())), OpCode::kNotFound);

      // update should fail
      const uint64_t update_desc_buf_size = UpdateDesc::Size(1);
      uint8_t update_desc_buf[update_desc_buf_size];
      auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
      update_desc->num_slots_ = 1;
      update_desc->update_slots_[0].offset_ = 0;
      update_desc->update_slots_[0].size_ = val_size;
      auto update_call_back = [&](MutableSlice mut_raw_val) {
        std::memcpy(mut_raw_val.Data(), new_val.data(), mut_raw_val.Size());
      };
      EXPECT_EQ(btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()),
                                     update_call_back, *update_desc),
                OpCode::kNotFound);

      // lookup should not found
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kNotFound);

      // insert with another val should success
      EXPECT_EQ(btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                              Slice((const uint8_t*)new_val.data(), new_val.size())),
                OpCode::kOK);

      // lookup the new value should success
      EXPECT_EQ(btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out),
                OpCode::kOK);
      EXPECT_EQ(copied_value, new_val);
    }
  });
}

TEST_F(TransactionKVTest, ConcurrentInsertWithSplit) {
  // prepare a btree for insert
  TransactionKV* btree;
  store_->ExecSync(0, [&]() {
    auto res = store_->CreateTransactionKV(
        ::testing::UnitTest::GetInstance()->current_test_info()->name());
    btree = res.value();
    EXPECT_NE(btree, nullptr);
  });

  std::atomic<bool> stop = false;
  auto key_size = 24;
  auto val_size = 120;

  // insert in worker 0 asynchorously
  store_->ExecAsync(0, [&]() {
    for (auto i = 0; !stop; i++) {
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
      auto key = std::format("{}_{}_{}", RandomGenerator::RandAlphString(key_size), 0, i);
      auto val = RandomGenerator::RandAlphString(val_size);
      auto res = btree->Insert(key, val);
      EXPECT_EQ(res, OpCode::kOK);
    }
  });

  // insert in worker 1 asynchorously
  store_->ExecAsync(1, [&]() {
    for (auto i = 0; !stop; i++) {
      CoroEnv::CurTxMgr().StartTx();
      SCOPED_DEFER(CoroEnv::CurTxMgr().CommitTx());
      auto key = std::format("{}_{}_{}", RandomGenerator::RandAlphString(key_size), 1, i);
      auto val = RandomGenerator::RandAlphString(val_size);
      auto res = btree->Insert(key, val);
      EXPECT_EQ(res, OpCode::kOK);
    }
  });

  // sleep for 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop.store(true);
  store_->Wait(0);
  store_->Wait(1);
}

} // namespace leanstore::test
