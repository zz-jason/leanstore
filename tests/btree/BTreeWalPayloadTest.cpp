#include "btree/core/BTreeWalPayload.hpp"

#include "leanstore/KVInterface.hpp"
#include "leanstore/btree/core/BTreeNode.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

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
  EXPECT_EQ(sizeof(WalPayload), 1);
  EXPECT_EQ(sizeof(WalInsert), 6);
  EXPECT_EQ(sizeof(WalTxInsert), 24);
  EXPECT_EQ(sizeof(WalUpdate), 6);
  EXPECT_EQ(sizeof(WalTxUpdate), 48);
  EXPECT_EQ(sizeof(WalRemove), 6);
  EXPECT_EQ(sizeof(WalTxRemove), 24);
  EXPECT_EQ(sizeof(WalInitPage), 32);
  EXPECT_EQ(sizeof(WalSplitRoot), 48);
  EXPECT_EQ(sizeof(WalSplitNonRoot), 40);
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

  wal = std::make_unique<WalInitPage>(0, 0, false);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalInitPage"));
  EXPECT_TRUE(walStr.contains("mTreeId"));
  EXPECT_TRUE(walStr.contains("mIsLeaf"));

  BTreeNode::SeparatorInfo sepInfo;
  wal = std::make_unique<WalSplitRoot>(0, 0, 0, 0, sepInfo);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalSplitRoot"));
  EXPECT_TRUE(walStr.contains("mNewLeft"));
  EXPECT_TRUE(walStr.contains("mNewRoot"));

  wal = std::make_unique<WalSplitNonRoot>(0, 0, 0, sepInfo);
  walStr = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(walStr.contains("mType"));
  EXPECT_TRUE(walStr.contains("kWalSplitNonRoot"));
  EXPECT_TRUE(walStr.contains("mParentPageId"));
}

} // namespace leanstore::storage::btree::test
