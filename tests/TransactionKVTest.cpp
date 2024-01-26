#include "storage/btree/TransactionKV.hpp"

#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "storage/btree/core/BTreeGeneric.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class TransactionKVTest : public ::testing::Test {
protected:
  TransactionKVTest() = default;

  ~TransactionKVTest() = default;

public:
  inline static leanstore::LeanStore* GetLeanStore() {
    static LeanStore* sStore = nullptr;
    if (sStore != nullptr) {
      return sStore;
    }

    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = "/tmp/TransactionKVTest";
    FLAGS_worker_threads = 3;
    FLAGS_enable_eager_garbage_collection = true;
    auto res = LeanStore::Open();
    sStore = res.value();
    return sStore;
  }
};

TEST_F(TransactionKVTest, Create) {
  GetLeanStore();
  storage::btree::TransactionKV* btree;
  storage::btree::TransactionKV* another;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  const auto* btreeName2 = "testTree2";
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->RegisterTransactionKV(btreeName2, btreeConfig, &another);
    EXPECT_NE(btree, nullptr);
  });

  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    GetLeanStore()->UnRegisterTransactionKV(btreeName2);
  });
}

TEST_F(TransactionKVTest, InsertAndLookup) {
  GetLeanStore();
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
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
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
  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
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

  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
  });
}

TEST_F(TransactionKVTest, Insert1000KVs) {
  GetLeanStore();
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertDuplicates) {
  GetLeanStore();
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, Remove) {
  GetLeanStore();
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveNotExisted) {
  GetLeanStore();
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    storage::btree::TransactionKV* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, RemoveFromOthers) {
  GetLeanStore();
  const auto* btreeName = "testTree1";
  std::set<std::string> uniqueKeys;
  storage::btree::TransactionKV* btree;

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    // create leanstore btree for table records
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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

  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    // lookup from another worker, should not found any keys
    for (auto& key : uniqueKeys) {
      cr::Worker::My().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::My().CommitTx();
    }
  });

  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
    // unregister the tree from another worker
    cr::Worker::My().StartTx();
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ToJson) {
  GetLeanStore();
  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
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
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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

    // auto doc = leanstore::storage::btree::BTreeGeneric::ToJson(*btree);
    // std::cout << leanstore::utils::JsonToStr(&doc);

    cr::Worker::My().StartTx();
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, Update) {
  GetLeanStore();
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanAsc) {
  GetLeanStore();
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, ScanDesc) {
  GetLeanStore();
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
    GetLeanStore()->UnRegisterTransactionKV(btreeName);
    cr::Worker::My().CommitTx();
  });
}

TEST_F(TransactionKVTest, InsertAfterRemove) {
  GetLeanStore();
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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
  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
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
  GetLeanStore();
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

  GetLeanStore()->mCRManager->ExecSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::My().StartTx();
    GetLeanStore()->RegisterTransactionKV(btreeName, btreeConfig, &btree);
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

  GetLeanStore()->mCRManager->ExecSync(1, [&]() {
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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}