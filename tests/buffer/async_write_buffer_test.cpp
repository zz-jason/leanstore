#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <format>
#include <memory>
#include <optional>
#include <string>

namespace leanstore::test {

class AsyncWriteBufferTest : public LeanTestSuite {
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
    auto created = session_->CreateBTree("buffer_basic", LEAN_BTREE_TYPE_ATOMIC);
    ASSERT_TRUE(created);
    tree_ = std::move(created.value());
  }
};

TEST_F(AsyncWriteBufferTest, Basic) {
  for (int i = 0; i < 64; ++i) {
    std::string key = std::format("k{:03d}", i);
    std::string value(512, static_cast<char>('a' + (i % 26)));
    ASSERT_TRUE(tree_->Insert(key, value));
  }

  for (int i = 0; i < 64; ++i) {
    std::string key = std::format("k{:03d}", i);
    auto value = tree_->Lookup(key);
    ASSERT_TRUE(value);
    ASSERT_EQ(value.value().size(), 512u);
  }
}

} // namespace leanstore::test
