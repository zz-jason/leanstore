#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_cursor.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <format>
#include <memory>
#include <optional>

namespace leanstore::test {

class BTreeGenericTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> session_;
  Optional<LeanBTree> tree_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 1;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());
    session_ = store_->Connect();
    auto created = session_->CreateBTree("summary", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    tree_ = std::move(created.value());
  }
};

TEST_F(BTreeGenericTest, GetSummary) {
  session_->StartTx();
  for (int i = 0; i < 200; ++i) {
    ASSERT_TRUE(tree_->Insert(std::format("k{:03d}", i), std::string(176, 'v')));
  }
  session_->CommitTx();

  session_->StartTx();
  auto cursor = tree_->OpenCursor();
  size_t entries = 0;
  if (cursor.SeekToFirst()) {
    do {
      entries++;
    } while (cursor.Next());
  }
  session_->CommitTx();
  EXPECT_EQ(entries, 200u);
}

} // namespace leanstore::test
