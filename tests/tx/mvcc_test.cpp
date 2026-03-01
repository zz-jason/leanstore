#include "common/lean_test_suite.hpp"
#include "leanstore/c/types.h"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/lean_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

namespace leanstore::test {

namespace {
auto ReadString(LeanBTree& tree, const std::string& key) -> Optional<std::string> {
  auto res = tree.Lookup(key);
  if (!res) {
    return std::nullopt;
  }
  return std::string(reinterpret_cast<const char*>(res.value().data()), res.value().size());
}
} // namespace

class MvccTest : public LeanTestSuite {
protected:
  std::unique_ptr<LeanStore> store_;
  Optional<LeanSession> writer_;
  Optional<LeanSession> reader_;
  Optional<LeanBTree> tw_;
  Optional<LeanBTree> tr_;

  void SetUp() override {
    auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    auto opened = LeanStore::Open(option);
    ASSERT_TRUE(opened);
    store_ = std::move(opened.value());

    writer_ = store_->Connect();
    reader_ = store_->Connect();
    auto created = writer_->CreateBTree("mvcc", LEAN_BTREE_TYPE_MVCC);
    ASSERT_TRUE(created);
    tw_ = std::move(created.value());
    auto fetched = reader_->GetBTree("mvcc");
    ASSERT_TRUE(fetched);
    tr_ = std::move(fetched.value());
  }
};

TEST_F(MvccTest, LookupWhileInsert) {
  ASSERT_TRUE(tw_->Insert("k0", "v0"));

  writer_->StartTx();
  ASSERT_TRUE(tw_->Insert("k1", "v1"));

  reader_->StartTx();
  ASSERT_EQ(ReadString(*tr_, "k0"), Optional<std::string>("v0"));
  ASSERT_FALSE(ReadString(*tr_, "k1").has_value());
  reader_->CommitTx();

  writer_->CommitTx();

  reader_->StartTx();
  ASSERT_EQ(ReadString(*tr_, "k1"), Optional<std::string>("v1"));
  reader_->CommitTx();
}

TEST_F(MvccTest, InsertConflict) {
  ASSERT_TRUE(tw_->Insert("k0", "v0"));

  writer_->StartTx();
  ASSERT_TRUE(tw_->Insert("k1", "v1"));

  reader_->StartTx();
  auto conflict = tr_->Insert("k1", "v1");
  ASSERT_FALSE(conflict);
  reader_->AbortTx();

  writer_->CommitTx();

  reader_->StartTx();
  ASSERT_EQ(ReadString(*tr_, "k1"), Optional<std::string>("v1"));
  reader_->CommitTx();
}

} // namespace leanstore::test
