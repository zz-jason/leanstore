#include "btree/core/BTreeWalPayload.hpp"

#include "btree/core/BTreeNode.hpp"
#include "leanstore/KVInterface.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>

namespace leanstore::storage::btree::test {

class BTreeWalPayloadTest : public ::testing::Test {
protected:
  std::string toString(const rapidjson::Document* doc) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc->Accept(writer);
    return std::string(buffer.GetString());
  }
};

TEST_F(BTreeWalPayloadTest, Size) {
  // class WalInsert;
  // class WalTxInsert;
  // class WalUpdate;
  // class WalTxUpdate;
  // class WalRemove;
  // class WalTxRemove;
  // class WalInitPage;
  // class WalSplitRoot;
  // class WalSplitNonRoot;
}

TEST_F(BTreeWalPayloadTest, ToJson) {
  std::unique_ptr<WalPayload> wal = std::make_unique<WalInsert>("", "");
  auto walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalInsert"));
  EXPECT_TRUE(walStr.contains("mKeySize"));
  EXPECT_TRUE(walStr.contains("mKey"));
  EXPECT_TRUE(walStr.contains("mValSize"));
  EXPECT_TRUE(walStr.contains("mVal"));

  wal = std::make_unique<WalTxInsert>("", "", 0, 0, 0);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalTxInsert"));
  EXPECT_TRUE(walStr.contains("mKeySize"));
  EXPECT_TRUE(walStr.contains("mKey"));
  EXPECT_TRUE(walStr.contains("mValSize"));
  EXPECT_TRUE(walStr.contains("mVal"));

  wal = std::make_unique<WalUpdate>();
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalUpdate"));
  EXPECT_TRUE(walStr.contains("Not implemented"));

  UpdateDesc upateDesc;
  wal = std::make_unique<WalTxUpdate>("", upateDesc, 0, 0, 0, 0);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalTxUpdate"));
  EXPECT_TRUE(walStr.contains("Not implemented"));

  wal = std::make_unique<WalRemove>("", "");
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalRemove"));
  EXPECT_TRUE(walStr.contains("Not implemented"));

  wal = std::make_unique<WalTxRemove>("", "", 0, 0, 0);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalTxRemove"));
  EXPECT_TRUE(walStr.contains("Not implemented"));

  wal = std::make_unique<WalInitPage>(0, false);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalInitPage"));
  EXPECT_TRUE(walStr.contains("mTreeId"));
  EXPECT_TRUE(walStr.contains("mIsLeaf"));

  BTreeNode::SeparatorInfo sepInfo;
  wal = std::make_unique<WalSplitRoot>(0, 0, 0, sepInfo);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalSplitRoot"));
  EXPECT_TRUE(walStr.contains("mNewLeft"));
  EXPECT_TRUE(walStr.contains("mNewRoot"));

  wal = std::make_unique<WalSplitNonRoot>(0, 0, sepInfo);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalSplitNonRoot"));
  EXPECT_TRUE(walStr.contains("mParentPageId"));
}

} // namespace leanstore::storage::btree::test