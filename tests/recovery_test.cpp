#include "failpoint/failpoint.hpp"
#include "lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/common/types.h"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <format>
#include <string>

#include <sys/socket.h>

using namespace leanstore;

namespace leanstore::test {

class RecoveryTest : public LeanTestSuite {
protected:
  RecoveryTest() = default;
  ~RecoveryTest() = default;

  void SetUp() override;
  void TearDown() override;

  lean_store* store_ = nullptr;
};

void RecoveryTest::SetUp() {
  auto* option = lean_store_option_create(TestCaseStoreDir().c_str());
  option->create_from_scratch_ = true;
  option->worker_threads_ = 2;
  option->enable_bulk_insert_ = false;
  option->enable_eager_gc_ = true;

  auto status = lean_open_store(option, &store_);
  ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
  ASSERT_NE(store_, nullptr);
}

void RecoveryTest::TearDown() {
  if (store_) {
    store_->close(store_);
  }
}

TEST_F(RecoveryTest, AtomicBTreeRecoverAfterInsert) {
  auto* s0 = store_->connect(store_);
  ASSERT_NE(s0, nullptr);
  SCOPED_DEFER({ s0->close(s0); })

  const char* btree_name = "recovery_test_tree";
  auto status = s0->create_btree(s0, btree_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC);
  ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
  auto* btree_s0 = s0->get_btree(s0, btree_name);
  ASSERT_NE(btree_s0, nullptr);
  SCOPED_DEFER({ btree_s0->close(btree_s0); })

  auto* s1 = store_->connect(store_);
  ASSERT_NE(s1, nullptr);
  SCOPED_DEFER({ s1->close(s1); })

  auto* btree_s1 = s1->get_btree(s1, btree_name);
  ASSERT_NE(btree_s1, nullptr);
  SCOPED_DEFER({ btree_s1->close(btree_s1); })

  // Insert some entries
  size_t num_entries = 100;
  for (auto i = 0u; i < num_entries; i++) {
    std::string key = std::format("key_{:03}", i);
    std::string val = std::format("val_{:03}", i);
    lean_str_view key_view = {key.data(), key.size()};
    lean_str_view val_view = {val.data(), val.size()};

    // randomly choose a session to perform the insert
    utils::RandomGenerator::RandU64() % 2 == 0
        ? status = btree_s1->insert(btree_s1, key_view, val_view)
        : status = btree_s0->insert(btree_s0, key_view, val_view);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
  }

  // Close the store without checkpointing, to simulate a crash after inserts
  FailPoint::Enable(FailPoint::kSkipCheckpointAll);
  SCOPED_DEFER(FailPoint::Disable(FailPoint::kSkipCheckpointAll));
  store_->close(store_);
  store_ = nullptr;
}

} // namespace leanstore::test