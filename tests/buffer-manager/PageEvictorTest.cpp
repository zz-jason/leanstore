#include "leanstore/buffer-manager/AsyncWriteBuffer.hpp"

#include "leanstore/buffer-manager/BufferFrame.hpp"
#include "leanstore/buffer-manager/Swip.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/buffer-manager/PageEvictor.hpp"
#include "leanstore/buffer-manager/BufferManager.hpp"
#include <iostream>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>

#include <fcntl.h>

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
    auto res = LeanStore::Open(StoreOption{
        .mCreateFromScratch = true,
        .mStoreDir = "/tmp/" + curTestName,
        .mLogLevel = LogLevel::kDebug,
        .mWorkerThreads = 2,
        .mNumPartitions = 1,
        .mBufferPoolSize = 70 * (pageHeaderSize + pageSize),
        .mFreePct = 20,
        .mEnableBulkInsert = false,
        .mEnableEagerGc = false,
        
    });
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