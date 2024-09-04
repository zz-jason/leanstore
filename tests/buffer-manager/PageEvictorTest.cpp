#include "leanstore/buffer-manager/PageEvictor.hpp"

#include "leanstore/buffer-manager/BufferManager.hpp"

#include <gtest/gtest.h>

#include <cstring>

namespace leanstore::storage::test {
class PageEvictorTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  PageEvictorTest() = default;

  ~PageEvictorTest() = default;

  void SetUp() override {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" + std::string(curTest->name());
    const int pageSize = 4096;
    const int pageHeaderSize = 512;
    auto storeDirStr = "/tmp/leanstore/" + curTestName;
    auto* option = CreateStoreOption(storeDirStr.c_str());
    option->mCreateFromScratch = true;
    option->mLogLevel = LogLevel::kDebug;
    option->mWorkerThreads = 2;
    option->mNumPartitions = 1;
    option->mBufferPoolSize = 70 * (pageHeaderSize + pageSize);
    option->mFreePct = 20;
    option->mEnableBulkInsert = false;
    option->mEnableEagerGc = false;
    auto res = LeanStore::Open(option);
    ASSERT_TRUE(res);
    mStore = std::move(res.value());
  }
};

TEST_F(PageEvictorTest, pageEvictBasic) {
  EXPECT_EQ(mStore->mBufferManager->mPageEvictors.size(), 1);
  auto& pageEvictor = mStore->mBufferManager->mPageEvictors[0];
  EXPECT_TRUE(pageEvictor->IsStarted());
  EXPECT_EQ(pageEvictor->mPartitions.size(), 1);
  EXPECT_FALSE(pageEvictor->mPartitions[0]->NeedMoreFreeBfs());
  pageEvictor->PickBufferFramesToCool(*pageEvictor->mPartitions[0]);
  pageEvictor->PrepareAsyncWriteBuffer(*pageEvictor->mPartitions[0]);
  pageEvictor->FlushAndRecycleBufferFrames(*pageEvictor->mPartitions[0]);
}

} // namespace leanstore::storage::test