#include "storage/btree/TransactionKV.hpp"

#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class TransactionKVTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  /// Create a leanstore instance for each test case
  TransactionKVTest() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());
    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = "/tmp/" + curTestName;
    FLAGS_worker_threads = 3;
    FLAGS_enable_eager_garbage_collection = true;
    auto res = LeanStore::Open();
    mStore = std::move(res.value());
  }

  ~TransactionKVTest() = default;
};

TEST_F(TransactionKVTest, Create) {
  storage::btree::TransactionKV* btree;
  storage::btree::TransactionKV* another;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->CreateTransactionKV(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->CreateTransactionKV(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  const auto* btreeName2 = "testTree2";
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->CreateTransactionKV(btreeName2, btreeConfig, &another);
    EXPECT_NE(btree, nullptr);
  });

  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->DropTransactionKV(btreeName);
    mStore->DropTransactionKV(btreeName2);
  });
}

TEST_F(TransactionKVTest, InsertAndLookup) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  size_t numKVs(10);
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    std::string key("key_btree_VI_xxxxxxxxxxxx_" + std::to_string(i));
    std::string val("VAL_BTREE_VI_YYYYYYYYYYYY_" + std::to_string(i));
    kvToTest.push_back(std::make_tuple(key, val));
  }

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert some values
    cr::Worker::My().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::My().CommitTx();
  });

  // query on the created btree in the same worker
  mStore->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  // query on the created btree in another worker
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, expectedVal] = kvToTest[i];
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, expectedVal);
    }
  });

  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    mStore->DropTransactionKV(btreeName);
  });
}

TEST_F(TransactionKVTest, Insert1000KVs) {
  mStore->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(1000);
    cr::Worker::My().StartTx();
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::My().CommitTx();

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertDuplicates) {
  mStore->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    // insert duplicated keys
    for (auto& key : uniqueKeys) {
      auto val = RandomGenerator::RandAlphString(128);
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kDuplicated);
      cr::Worker::My().CommitTx();
    }

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, Remove) {
  mStore->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::My().CommitTx();
    }

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveNotExisted) {
  mStore->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    // remove keys not existed
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);

      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kNotFound);
      cr::Worker::My().CommitTx();
    }

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveFromOthers) {
  const auto* btreeName = "testTree1";
  std::set<std::string> uniqueKeys;
  storage::btree::TransactionKV* btree;

  mStore->ExecSync(0, [&]() {
    // create leanstore btree for table records
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert numKVs tuples
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomGenerator::RandAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomGenerator::RandAlphString(128);

      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }
  });

  mStore->ExecSync(1, [&]() {
    // remove from another worker
    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);
      cr::Worker::My().CommitTx();
    }

    // should not found any keys
    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::My().CommitTx();
    }
  });

  mStore->ExecSync(0, [&]() {
    // lookup from another worker, should not found any keys
    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::My().CommitTx();
    }
  });

  mStore->ExecSync(1, [&]() {
    // unregister the tree from another worker
    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ToJson) {
  mStore->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // prepare key-value pairs to insert
    size_t numKVs(10);
    std::vector<std::tuple<std::string, std::string>> kvToTest;
    for (size_t i = 0; i < numKVs; ++i) {
      std::string key("key_btree_VI_xxxxxxxxxxxx_" + std::to_string(i));
      std::string val("VAL_BTREE_VI_YYYYYYYYYYYY_" + std::to_string(i));
      kvToTest.push_back(std::make_tuple(key, val));
    }
    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::My().CommitTx();

    // insert some values
    cr::Worker::My().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::My().CommitTx();

    rapidjson::Document doc(rapidjson::kObjectType);
    leanstore::storage::btree::BTreeGeneric::ToJson(*btree, &doc);
    EXPECT_GE(leanstore::utils::JsonToStr(&doc).size(), 0u);

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, Update) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(valSize);
    kvToTest.push_back(std::make_tuple(key, val));
  }

  const auto* btreeName = "testTree1";

  mStore->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    cr::Worker::My().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      auto res = btree->Insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // update all the values to this newVal
    auto newVal = RandomGenerator::RandAlphString(valSize);
    auto updateCallBack = [&](MutableSlice mutRawVal) {
      std::memcpy(mutRawVal.Data(), newVal.data(), mutRawVal.Size());
    };

    // update in the same worker
    const u64 updateDescBufSize = UpdateDesc::Size(1);
    u8 updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = valSize;
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      auto res = btree->UpdatePartial(Slice((const u8*)key.data(), key.size()),
                                      updateCallBack, *updateDesc);
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // verify updated values
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::My().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      cr::Worker::My().CommitTx();
      EXPECT_EQ(copiedValue, newVal);
    }

    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanAsc) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(valSize);
    if (kvToTest.find(key) != kvToTest.end()) {
      i--;
      continue;
    }
    kvToTest.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btreeName = "testTree1";

  mStore->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    cr::Worker::My().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      auto res = btree->Insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // scan in ascending order
    std::unordered_map<std::string, std::string> copiedKVs;
    auto scanCallBack = [&](Slice key, Slice val) {
      copiedKVs.emplace(std::string((const char*)key.data(), key.size()),
                        std::string((const char*)val.data(), val.size()));
      return true;
    };

    // scan from the smallest key
    copiedKVs.clear();
    cr::Worker::My().StartTx();
    EXPECT_EQ(btree->ScanAsc(Slice((const u8*)smallest.data(), smallest.size()),
                             scanCallBack),
              OpCode::kOK);
    cr::Worker::My().CommitTx();
    EXPECT_EQ(copiedKVs.size(), numKVs);
    for (const auto& [key, val] : copiedKVs) {
      EXPECT_EQ(val, kvToTest[key]);
    }

    // scan from the bigest key
    copiedKVs.clear();
    cr::Worker::My().StartTx();
    EXPECT_EQ(btree->ScanAsc(Slice((const u8*)bigest.data(), bigest.size()),
                             scanCallBack),
              OpCode::kOK);
    cr::Worker::My().CommitTx();
    EXPECT_EQ(copiedKVs.size(), 1u);
    EXPECT_EQ(copiedKVs[bigest], kvToTest[bigest]);

    // destroy the tree
    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanDesc) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(valSize);
    if (kvToTest.find(key) != kvToTest.end()) {
      i--;
      continue;
    }
    kvToTest.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btreeName = "testTree1";

  mStore->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    cr::Worker::My().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      auto res = btree->Insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // scan in descending order
    std::unordered_map<std::string, std::string> copiedKVs;
    auto scanCallBack = [&](Slice key, Slice val) {
      copiedKVs.emplace(std::string((const char*)key.data(), key.size()),
                        std::string((const char*)val.data(), val.size()));
      return true;
    };

    // scan from the bigest key
    copiedKVs.clear();
    cr::Worker::My().StartTx();
    EXPECT_EQ(btree->ScanDesc(Slice((const u8*)bigest.data(), bigest.size()),
                              scanCallBack),
              OpCode::kOK);
    cr::Worker::My().CommitTx();
    EXPECT_EQ(copiedKVs.size(), numKVs);
    for (const auto& [key, val] : copiedKVs) {
      EXPECT_EQ(val, kvToTest[key]);
    }

    // scan from the smallest key
    copiedKVs.clear();
    cr::Worker::My().StartTx();
    EXPECT_EQ(
        btree->ScanDesc(Slice((const u8*)smallest.data(), smallest.size()),
                        scanCallBack),
        OpCode::kOK);
    cr::Worker::My().CommitTx();
    EXPECT_EQ(copiedKVs.size(), 1u);
    EXPECT_EQ(copiedKVs[smallest], kvToTest[smallest]);

    // destroy the tree
    cr::Worker::My().StartTx();
    mStore->DropTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertAfterRemove) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(1);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(valSize);
    if (kvToTest.find(key) != kvToTest.end()) {
      i--;
      continue;
    }
    kvToTest.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btreeName = "InsertAfterRemove";
  std::string newVal = RandomGenerator::RandAlphString(valSize);
  std::string copiedValue;
  auto copyValueOut = [&](Slice val) {
    copiedValue = std::string((const char*)val.data(), val.size());
  };

  mStore->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    cr::Worker::My().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      auto res = btree->Insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // remove, insert, and lookup
    for (const auto& [key, val] : kvToTest) {
      // remove
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());

      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);

      // remove twice should got not found error
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kNotFound);

      // update should fail
      const u64 updateDescBufSize = UpdateDesc::Size(1);
      u8 updateDescBuf[updateDescBufSize];
      auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
      updateDesc->mNumSlots = 1;
      updateDesc->mUpdateSlots[0].mOffset = 0;
      updateDesc->mUpdateSlots[0].mSize = valSize;
      auto updateCallBack = [&](MutableSlice mutRawVal) {
        std::memcpy(mutRawVal.Data(), newVal.data(), mutRawVal.Size());
      };
      EXPECT_EQ(btree->UpdatePartial(Slice((const u8*)key.data(), key.size()),
                                     updateCallBack, *updateDesc),
                OpCode::kNotFound);

      // lookup should not found
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kNotFound);

      // insert with another val should success
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)newVal.data(), newVal.size())),
                OpCode::kOK);

      // lookup the new value should success
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, newVal);
    }
  });

  LOG(INFO) << "key=" << kvToTest.begin()->first
            << ", val=" << kvToTest.begin()->second << ", newVal=" << newVal;
  mStore->ExecSync(1, [&]() {
    // lookup the new value
    cr::Worker::My().StartTx();
    for (const auto& [key, val] : kvToTest) {
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, newVal);
    }
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertAfterRemoveDifferentWorkers) {
  storage::btree::TransactionKV* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(1);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomGenerator::RandAlphString(24);
    auto val = RandomGenerator::RandAlphString(valSize);
    if (kvToTest.find(key) != kvToTest.end()) {
      i--;
      continue;
    }
    kvToTest.emplace(key, val);
    if (smallest.size() == 0 || smallest > key) {
      smallest = key;
    }
    if (bigest.size() == 0 || bigest < key) {
      bigest = key;
    }
  }

  const auto* btreeName = "InsertAfterRemoveDifferentWorkers";
  std::string newVal = RandomGenerator::RandAlphString(valSize);
  std::string copiedValue;
  auto copyValueOut = [&](Slice val) {
    copiedValue = std::string((const char*)val.data(), val.size());
  };

  mStore->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    mStore->CreateTransactionKV(btreeName, btreeConfig, &btree);
    cr::Worker::My().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      auto res = btree->Insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::My().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // remove
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);
    }
  });

  mStore->ExecSync(1, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());

      // remove twice should got not found error
      EXPECT_EQ(btree->Remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kNotFound);

      // update should fail
      const u64 updateDescBufSize = UpdateDesc::Size(1);
      u8 updateDescBuf[updateDescBufSize];
      auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
      updateDesc->mNumSlots = 1;
      updateDesc->mUpdateSlots[0].mOffset = 0;
      updateDesc->mUpdateSlots[0].mSize = valSize;
      auto updateCallBack = [&](MutableSlice mutRawVal) {
        std::memcpy(mutRawVal.Data(), newVal.data(), mutRawVal.Size());
      };
      EXPECT_EQ(btree->UpdatePartial(Slice((const u8*)key.data(), key.size()),
                                     updateCallBack, *updateDesc),
                OpCode::kNotFound);

      // lookup should not found
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kNotFound);

      // insert with another val should success
      EXPECT_EQ(btree->Insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)newVal.data(), newVal.size())),
                OpCode::kOK);

      // lookup the new value should success
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      EXPECT_EQ(copiedValue, newVal);
    }
  });
}

} // namespace leanstore::test
