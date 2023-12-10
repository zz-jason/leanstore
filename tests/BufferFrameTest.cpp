#include "storage/buffer-manager/BufferFrame.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <ctime>
#include <string>

#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <termios.h>
#include <unistd.h>

namespace leanstore {
namespace storage {

static std::string MakeRandomFileName() {
  std::srand(std::time(nullptr));
  std::string fileName("tmp/BufferManagerTestFile");
  for (int i = 0; i < 6; ++i) {
    fileName += std::to_string(std::rand());
  }
  return fileName;
}

class BufferManagerTest : public ::testing::Test {
protected:
  const std::string mFilePath;
  s32 mFd;
  std::unique_ptr<BufferManager> mBufferMgr;

  BufferManagerTest() : mFilePath(MakeRandomFileName()) {
  }

  ~BufferManagerTest() = default;

  void SetUp() override {
    ASSERT_GT(mFilePath.size(), 0u);

    int flags = O_RDWR | O_DIRECT | O_TRUNC | O_CREAT;
    mFd = open(mFilePath.c_str(), flags, 0666);
    ASSERT_NE(mFd, -1);

    mBufferMgr = std::make_unique<BufferManager>(mFd);
    ASSERT_NE(mBufferMgr, nullptr);
  }

  void TearDown() override {
  }
};

TEST_F(BufferManagerTest, Basic) {
  EXPECT_EQ(mBufferMgr->mPageFd, mFd);
}

TEST(BufferFrameTest, EmptyBufferFrame) {
  leanstore::storage::BufferFrame bf;

  EXPECT_EQ(sizeof(bf.page), leanstore::storage::PAGE_SIZE);
}

} // namespace storage
} // namespace leanstore