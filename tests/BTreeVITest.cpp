#include "LeanStore.hpp"
#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace leanstore {

template <typename T = std::mt19937> auto RandomGenerator() -> T {
  auto constexpr fixed_seed = 123456789; // Fixed seed for deterministic output
  return T{fixed_seed};
}

static std::string RandomAlphString(std::size_t len) {
  static constexpr auto chars = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  thread_local auto rng = RandomGenerator<>();
  auto dist = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[dist(rng)]; });
  return result;
}

static auto InitLeanStore() {
  FLAGS_enable_print_btree_stats_on_exit = true;
  FLAGS_wal = true;
  FLAGS_bulk_insert = false;
  FLAGS_worker_threads = 2;
  FLAGS_recover = false;
  FLAGS_data_dir = "/tmp/BTreeVITest";

  std::filesystem::path dirPath = FLAGS_data_dir;
  std::filesystem::remove_all(dirPath);
  std::filesystem::create_directories(dirPath);
  return std::make_unique<leanstore::LeanStore>();
}

static leanstore::LeanStore* GetLeanStore() {
  static auto leanStore = InitLeanStore();
  return leanStore.get();
}

class BTreeVITest : public ::testing::Test {
protected:
  BTreeVITest() = default;

  ~BTreeVITest() = default;
};

TEST_F(BTreeVITest, Create) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;
  storage::btree::BTreeVI* another;

  // create leanstore btree for table records
  const auto* btreeName = "testTree1";
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
  });

  // create btree with same should fail in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with same should also fail in other workers
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &another);
    EXPECT_EQ(another, nullptr);
  });

  // create btree with another different name should success
  const auto* btreeName2 = "testTree2";
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->RegisterBTreeVI(btreeName2, btreeConfig, &another);
    EXPECT_NE(btree, nullptr);
  });

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    GetLeanStore()->UnRegisterBTreeVI(btreeName2);
  });
}

TEST_F(BTreeVITest, InsertAndLookup) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;

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
  auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert some values
    cr::Worker::my().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::my().CommitTx();
  });

  // query on the created btree in the same worker
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
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
  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
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

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    cr::Worker::my().StartTx();
    SCOPED_DEFER(cr::Worker::my().CommitTx());
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
  });
}

TEST_F(BTreeVITest, Insert1000KVs) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(1000);
    cr::Worker::my().StartTx();
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomAlphString(128);
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::my().CommitTx();

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, InsertDuplicates) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomAlphString(128);
      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }

    // insert duplicated keys
    for (auto& key : uniqueKeys) {
      auto val = RandomAlphString(128);
      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kDuplicated);
      cr::Worker::my().CommitTx();
    }

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, Remove) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomAlphString(128);

      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }

    for (auto& key : uniqueKeys) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }

    for (auto& key : uniqueKeys) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::my().CommitTx();
    }

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, RemoveNotExisted) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

    // create leanstore btree for table records
    const auto* btreeName = "testTree1";
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert numKVs tuples
    std::set<std::string> uniqueKeys;
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomAlphString(128);

      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }

    // remove keys not existed
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);

      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kNotFound);
      cr::Worker::my().CommitTx();
    }

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, RemoveFromOthers) {
  GetLeanStore();
  const auto* btreeName = "testTree1";
  std::set<std::string> uniqueKeys;
  storage::btree::BTreeVI* btree;

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    // create leanstore btree for table records
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert numKVs tuples
    ssize_t numKVs(100);
    for (ssize_t i = 0; i < numKVs; ++i) {
      auto key = RandomAlphString(24);
      if (uniqueKeys.find(key) != uniqueKeys.end()) {
        i--;
        continue;
      }
      uniqueKeys.insert(key);
      auto val = RandomAlphString(128);

      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }
  });

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    // remove from another worker
    for (auto& key : uniqueKeys) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(btree->remove(Slice((const u8*)key.data(), key.size())),
                OpCode::kOK);
      cr::Worker::my().CommitTx();
    }

    // should not found any keys
    for (auto& key : uniqueKeys) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::my().CommitTx();
    }
  });

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    // lookup from another worker, should not found any keys
    for (auto& key : uniqueKeys) {
      cr::Worker::my().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), [](Slice) {}),
          OpCode::kNotFound);
      cr::Worker::my().CommitTx();
    }
  });

  cr::CRManager::sInstance->scheduleJobSync(1, [&]() {
    // unregister the tree from another worker
    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, ToJSON) {
  GetLeanStore();
  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    storage::btree::BTreeVI* btree;

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
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    EXPECT_NE(btree, nullptr);
    cr::Worker::my().CommitTx();

    // insert some values
    cr::Worker::my().StartTx();
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      EXPECT_EQ(btree->insert(Slice((const u8*)key.data(), key.size()),
                              Slice((const u8*)val.data(), val.size())),
                OpCode::kOK);
    }
    cr::Worker::my().CommitTx();

    // auto doc = leanstore::storage::btree::BTreeGeneric::ToJSON(*btree);
    // std::cout << leanstore::utils::JsonToStr(&doc);

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, Update) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::vector<std::tuple<std::string, std::string>> kvToTest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomAlphString(24);
    auto val = RandomAlphString(valSize);
    kvToTest.push_back(std::make_tuple(key, val));
  }

  const auto* btreeName = "testTree1";

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    cr::Worker::my().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::my().StartTx();
      auto res = btree->insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::my().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // update all the values to this newVal
    auto newVal = RandomAlphString(valSize);
    auto updateCallBack = [&](MutableSlice val) {
      std::memcpy(val.data(), newVal.data(), val.length());
    };

    // update in the same worker
    const u64 updateDescBufSize = UpdateDesc::Size(1);
    u8 updateDescBuf[updateDescBufSize];
    auto updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mDiffSlots[0].offset = 0;
    updateDesc->mDiffSlots[0].length = valSize;
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::my().StartTx();
      auto res =
          btree->updateSameSizeInPlace(Slice((const u8*)key.data(), key.size()),
                                       updateCallBack, *updateDesc);
      cr::Worker::my().CommitTx();
      EXPECT_EQ(res, OpCode::kOK);
    }

    // verify updated values
    std::string copiedValue;
    auto copyValueOut = [&](Slice val) {
      copiedValue = std::string((const char*)val.data(), val.size());
    };
    for (size_t i = 0; i < numKVs; ++i) {
      const auto& [key, val] = kvToTest[i];
      cr::Worker::my().StartTx();
      EXPECT_EQ(
          btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut),
          OpCode::kOK);
      cr::Worker::my().CommitTx();
      EXPECT_EQ(copiedValue, newVal);
    }

    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, ScanAsc) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomAlphString(24);
    auto val = RandomAlphString(valSize);
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

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    cr::Worker::my().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::my().StartTx();
      auto res = btree->insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::my().CommitTx();
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
    cr::Worker::my().StartTx();
    EXPECT_EQ(btree->ScanAsc(Slice((const u8*)smallest.data(), smallest.size()),
                             scanCallBack),
              OpCode::kOK);
    cr::Worker::my().CommitTx();
    EXPECT_EQ(copiedKVs.size(), numKVs);
    for (const auto& [key, val] : copiedKVs) {
      EXPECT_EQ(val, kvToTest[key]);
    }

    // scan from the bigest key
    copiedKVs.clear();
    cr::Worker::my().StartTx();
    EXPECT_EQ(btree->ScanAsc(Slice((const u8*)bigest.data(), bigest.size()),
                             scanCallBack),
              OpCode::kOK);
    cr::Worker::my().CommitTx();
    EXPECT_EQ(copiedKVs.size(), 1u);
    EXPECT_EQ(copiedKVs[bigest], kvToTest[bigest]);

    // destroy the tree
    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

TEST_F(BTreeVITest, ScanDesc) {
  GetLeanStore();
  storage::btree::BTreeVI* btree;

  // prepare key-value pairs to insert
  const size_t numKVs(100);
  const size_t valSize = 120;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallest;
  std::string bigest;
  for (size_t i = 0; i < numKVs; ++i) {
    auto key = RandomAlphString(24);
    auto val = RandomAlphString(valSize);
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

  cr::CRManager::sInstance->scheduleJobSync(0, [&]() {
    auto btreeConfig = leanstore::storage::btree::BTreeGeneric::Config{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };

    // create btree
    cr::Worker::my().StartTx();
    GetLeanStore()->RegisterBTreeVI(btreeName, btreeConfig, &btree);
    cr::Worker::my().CommitTx();
    EXPECT_NE(btree, nullptr);

    // insert values
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::my().StartTx();
      auto res = btree->insert(Slice((const u8*)key.data(), key.size()),
                               Slice((const u8*)val.data(), val.size()));
      cr::Worker::my().CommitTx();
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
    cr::Worker::my().StartTx();
    EXPECT_EQ(btree->ScanDesc(Slice((const u8*)bigest.data(), bigest.size()),
                              scanCallBack),
              OpCode::kOK);
    cr::Worker::my().CommitTx();
    EXPECT_EQ(copiedKVs.size(), numKVs);
    for (const auto& [key, val] : copiedKVs) {
      EXPECT_EQ(val, kvToTest[key]);
    }

    // scan from the smallest key
    copiedKVs.clear();
    cr::Worker::my().StartTx();
    EXPECT_EQ(
        btree->ScanDesc(Slice((const u8*)smallest.data(), smallest.size()),
                        scanCallBack),
        OpCode::kOK);
    cr::Worker::my().CommitTx();
    EXPECT_EQ(copiedKVs.size(), 1u);
    EXPECT_EQ(copiedKVs[smallest], kvToTest[smallest]);

    // destroy the tree
    cr::Worker::my().StartTx();
    GetLeanStore()->UnRegisterBTreeVI(btreeName);
    cr::Worker::my().CommitTx();
  });
}

} // namespace leanstore

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}