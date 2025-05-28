#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore/btree/core/b_tree_node.hpp"
#include "leanstore/kv_interface.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <memory>

namespace leanstore::storage::btree::test {

class BTreeWalPayloadTest : public ::testing::Test {
protected:
  std::string to_string(const rapidjson::Document* doc) {
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
  auto wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalInsert"));
  EXPECT_TRUE(wal_str.contains("key_size_"));
  EXPECT_TRUE(wal_str.contains("key_"));
  EXPECT_TRUE(wal_str.contains("val_size_"));
  EXPECT_TRUE(wal_str.contains("val_"));

  wal = std::make_unique<WalTxInsert>("", "", 0, 0, 0);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalTxInsert"));
  EXPECT_TRUE(wal_str.contains("key_size_"));
  EXPECT_TRUE(wal_str.contains("key_"));
  EXPECT_TRUE(wal_str.contains("val_size_"));
  EXPECT_TRUE(wal_str.contains("val_"));

  wal = std::make_unique<WalUpdate>();
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalUpdate"));
  EXPECT_TRUE(wal_str.contains("Not implemented"));

  UpdateDesc upate_desc;
  wal = std::make_unique<WalTxUpdate>("", upate_desc, 0, 0, 0, 0);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalTxUpdate"));
  EXPECT_TRUE(wal_str.contains("Not implemented"));

  wal = std::make_unique<WalRemove>("", "");
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalRemove"));
  EXPECT_TRUE(wal_str.contains("Not implemented"));

  wal = std::make_unique<WalTxRemove>("", "", 0, 0, 0);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalTxRemove"));
  EXPECT_TRUE(wal_str.contains("Not implemented"));

  wal = std::make_unique<WalInitPage>(0, 0, false);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalInitPage"));
  EXPECT_TRUE(wal_str.contains("tree_id_"));
  EXPECT_TRUE(wal_str.contains("is_leaf_"));

  BTreeNode::SeparatorInfo sep_info;
  wal = std::make_unique<WalSplitRoot>(0, 0, 0, 0, sep_info);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalSplitRoot"));
  EXPECT_TRUE(wal_str.contains("new_left_"));
  EXPECT_TRUE(wal_str.contains("new_root_"));

  wal = std::make_unique<WalSplitNonRoot>(0, 0, 0, sep_info);
  wal_str = WalPayload::ToJsonString(wal.get());
  EXPECT_TRUE(wal_str.contains("type_"));
  EXPECT_TRUE(wal_str.contains("kWalSplitNonRoot"));
  EXPECT_TRUE(wal_str.contains("parent_page_id_"));
}

} // namespace leanstore::storage::btree::test
