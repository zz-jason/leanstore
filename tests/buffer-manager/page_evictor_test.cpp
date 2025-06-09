#include "leanstore/buffer-manager/buffer_manager.hpp"
#include "leanstore/buffer-manager/page_evictor.hpp"

#include <gtest/gtest.h>

#include <cstring>

namespace leanstore::storage::test {
class PageEvictorTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> store_;

  PageEvictorTest() = default;

  ~PageEvictorTest() = default;

  void SetUp() override {
    auto* cur_test = ::testing::UnitTest::GetInstance()->current_test_info();
    auto cur_test_name =
        std::string(cur_test->test_case_name()) + "_" + std::string(cur_test->name());
    const int page_size = 4096;
    const int page_header_size = 512;
    auto store_dir_str = "/tmp/leanstore/" + cur_test_name;
    auto* option = CreateStoreOption(store_dir_str.c_str());
    option->create_from_scratch_ = true;
    option->log_level_ = LogLevel::kDebug;
    option->worker_threads_ = 2;
    option->num_partitions_ = 1;
    option->buffer_pool_size_ = 70 * (page_header_size + page_size);
    option->free_pct_ = 20;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = false;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    store_ = std::move(res.value());
  }
};

TEST_F(PageEvictorTest, page_evict_basic) {
  EXPECT_EQ(store_->buffer_manager_->page_evictors_.size(), 1);
  auto& page_evictor = store_->buffer_manager_->page_evictors_[0];
  EXPECT_TRUE(page_evictor->IsStarted());
  EXPECT_EQ(page_evictor->GetPartitions().size(), 1);
  page_evictor->GetPartitions()[0]->NeedMoreFreeBfs();
  page_evictor->PickBufferFramesToCool(*page_evictor->GetPartitions()[0]);
  page_evictor->PrepareAsyncWriteBuffer(*page_evictor->GetPartitions()[0]);
  page_evictor->FlushAndRecycleBufferFrames(*page_evictor->GetPartitions()[0]);
}

} // namespace leanstore::storage::test