#include "btree/BasicKV.hpp"
#include "btree/TransactionKV.hpp"
#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "concurrency/HistoryStorage.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/StoreOption.hpp"
#include "utils/Defer.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

using namespace leanstore::utils;
using namespace leanstore::storage::btree;

namespace leanstore::test {

class LongRunningTxTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;
  std::string mTreeName;
  TransactionKV* mKv;

protected:
  LongRunningTxTest() = default;

  ~LongRunningTxTest() = default;

  void SetUp() override {
    // Create a leanstore instance for the test case
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());

    auto res = LeanStore::Open(StoreOption{
        .mCreateFromScratch = true,
        .mStoreDir = "/tmp/" + curTestName,
        .mWorkerThreads = 3,
        .mEnableEagerGc = true,
    });
    ASSERT_TRUE(res);
    mStore = std::move(res.value());

    // Worker 0, create a btree for test
    mTreeName = RandomGenerator::RandAlphString(10);
    mStore->ExecSync(0, [&]() {
      auto res = mStore->CreateTransactionKV(mTreeName);
      ASSERT_TRUE(res);
      mKv = res.value();
      ASSERT_NE(mKv, nullptr);
    });

    // Worker 0, do extra insert and remove transactions in worker 0 to make it
    // have more than one entries in the commit log, which helps to advance the
    // global lower watermarks for garbage collection
    mStore->ExecSync(0, [&]() {
      cr::Worker::My().StartTx();
      ASSERT_EQ(mKv->Insert(ToSlice("0"), ToSlice("0")), OpCode::kOK);
      cr::Worker::My().CommitTx();

      cr::Worker::My().StartTx();
      ASSERT_EQ(mKv->Remove(ToSlice("0")), OpCode::kOK);
      cr::Worker::My().CommitTx();
    });
  }

  void TearDown() override {
    // Worker 0, remove the btree
    mStore->ExecSync(0, [&]() {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      mStore->DropTransactionKV(mTreeName);
    });
  }
};

// TODO(lookup from graveyard)
TEST_F(LongRunningTxTest, LookupFromGraveyard) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copiedVal;
  auto copyValue = [&](Slice val) { copiedVal = std::string((const char*)val.data(), val.size()); };

  // Insert 2 key-values as the test base.
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(key1, ToSlice(val1)), OpCode::kOK);
    cr::Worker::My().CommitTx();

    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(key2, ToSlice(val2)), OpCode::kOK);
    cr::Worker::My().CommitTx();
  });

  mStore->ExecSync(1, [&]() { cr::Worker::My().StartTx(); });

  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);

    // get the old value in worker 2
    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // remove the key in worker 1
  mStore->ExecSync(1, [&]() {
    EXPECT_EQ(mKv->Remove(key1), OpCode::kOK);
    EXPECT_EQ(mKv->Remove(key2), OpCode::kOK);
  });

  // get the old value in worker 2
  mStore->ExecSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // commit the transaction in worker 1, after garbage collection when
  // committing the transaction, tombstones should be moved to the graveyard.
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().CommitTx();
    EXPECT_EQ(mKv->mGraveyard->CountEntries(), 2u);
  });

  // lookup from graveyard, still get the old value in worker 2
  mStore->ExecSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // now worker 2 can not get the old value
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning, IsolationLevel::kSnapshotIsolation);
    SCOPED_DEFER(cr::Worker::My().CommitTx());

    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kNotFound);
    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kNotFound);
  });
}

TEST_F(LongRunningTxTest, LookupAfterUpdate100Times) {
  std::string key1("1"), val1("10");
  std::string key2("2"), val2("20");
  std::string res;

  std::string copiedVal;
  auto copyValue = [&](Slice val) { copiedVal = std::string((const char*)val.data(), val.size()); };

  // Work 1, insert 2 key-values as the test base
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(key1, ToSlice(val1)), OpCode::kOK);
    cr::Worker::My().CommitTx();

    cr::Worker::My().StartTx();
    EXPECT_EQ(mKv->Insert(key2, ToSlice(val2)), OpCode::kOK);
    cr::Worker::My().CommitTx();
  });

  // Worker 1, start a short-running transaction
  mStore->ExecSync(1, [&]() { cr::Worker::My().StartTx(); });

  // Worker 2, start a long-running transaction, lookup, get the old value
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);

    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // Worker 1, update key1 100 times with random values
  std::string newVal;
  mStore->ExecSync(1, [&]() {
    auto updateDescBufSize = UpdateDesc::Size(1);
    uint8_t updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = val1.size();

    auto updateCallBack = [&](MutableSlice toUpdate) {
      auto newValSize = updateDesc->mUpdateSlots[0].mSize;
      newVal = RandomGenerator::RandAlphString(newValSize);
      std::memcpy(toUpdate.Data(), newVal.data(), newVal.size());
    };

    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(mKv->UpdatePartial(key1, updateCallBack, *updateDesc), OpCode::kOK);
    }
  });

  // Worker 2, lookup, get the old value
  mStore->ExecSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });

  // Worker 1, commit the transaction, graveyard should be empty, update history
  // trees should have 100 versions
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().CommitTx();

    EXPECT_EQ(mKv->mGraveyard->CountEntries(), 0u);
    auto* updateTree = cr::Worker::My().mCc.mHistoryStorage.GetUpdateIndex();
    auto* removeTree = cr::Worker::My().mCc.mHistoryStorage.GetRemoveIndex();
    EXPECT_EQ(updateTree->CountEntries(), 100u);
    EXPECT_EQ(removeTree->CountEntries(), 0u);
  });

  // Worker 2, lookup, skip the update versions, still get old values, commit
  mStore->ExecSync(2, [&]() {
    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val1);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // Worker 2, now get the updated new value
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);
    SCOPED_DEFER(cr::Worker::My().CommitTx());

    EXPECT_EQ(mKv->Lookup(key1, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, newVal);

    EXPECT_EQ(mKv->Lookup(key2, copyValue), OpCode::kOK);
    EXPECT_EQ(copiedVal, val2);
  });
}

TEST_F(LongRunningTxTest, ScanAscFromGraveyard) {
  // randomly generate 100 unique key-values for s1 to insert
  size_t numKV = 100;
  std::unordered_map<std::string, std::string> kvToTest;
  std::string smallestKey;
  for (size_t i = 0; i < numKV; ++i) {
    std::string key = RandomGenerator::RandAlphString(10);
    std::string val = RandomGenerator::RandAlphString(10);
    if (kvToTest.find(key) != kvToTest.end()) {
      --i;
      continue;
    }

    // update the smallest key
    kvToTest[key] = val;
    if (smallestKey.empty() || smallestKey > key) {
      smallestKey = key;
    }
  }

  // insert the key-values in worker 0
  mStore->ExecSync(0, [&]() {
    for (const auto& [key, val] : kvToTest) {
      cr::Worker::My().StartTx();
      SCOPED_DEFER(cr::Worker::My().CommitTx());
      EXPECT_EQ(mKv->Insert(key, ToSlice(val)), OpCode::kOK);
    }
  });

  // start transaction on worker 2, get the inserted values
  std::string copiedKey, copiedVal;
  auto copyKeyVal = [&](Slice key, Slice val) {
    copiedKey = std::string((const char*)key.data(), key.size());
    copiedVal = std::string((const char*)val.data(), val.size());
    EXPECT_EQ(copiedVal, kvToTest[copiedKey]);
    return true;
  };
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });

  // remove the key-values in worker 1
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().StartTx();
    for (const auto& [key, val] : kvToTest) {
      EXPECT_EQ(mKv->Remove(key), OpCode::kOK);
    }
  });

  // get the old values in worker 2
  mStore->ExecSync(
      2, [&]() { EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK); });

  // commit the transaction in worker 1, all the removed key-values should be
  // moved to graveyard
  mStore->ExecSync(1, [&]() {
    cr::Worker::My().CommitTx();
    EXPECT_EQ(mKv->mGraveyard->CountEntries(), kvToTest.size());
  });

  // still get the old values in worker 2
  mStore->ExecSync(2, [&]() {
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);

    // commit the transaction in worker 2
    cr::Worker::My().CommitTx();
  });

  // now worker 2 can not get the old values
  mStore->ExecSync(2, [&]() {
    cr::Worker::My().StartTx(TxMode::kLongRunning);
    SCOPED_DEFER(cr::Worker::My().CommitTx());
    EXPECT_EQ(mKv->ScanAsc(ToSlice(smallestKey), copyKeyVal), OpCode::kOK);
  });
}

} // namespace leanstore::test
