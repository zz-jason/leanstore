#include "buffer-manager/AsyncWriteBuffer.hpp"

#include "buffer-manager/BufferFrame.hpp"
#include "buffer-manager/Swip.hpp"
#include "leanstore/Config.hpp"
#include "utils/Defer.hpp"
#include "utils/Misc.hpp"
#include "utils/RandomGenerator.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>

#include <fcntl.h>

namespace leanstore::storage::test {

class AsyncWriteBufferTest : public ::testing::Test {
protected:
  std::string mTestDir = "/tmp/leanstore/AsyncWriteBufferTest";

  struct BufferFrameHolder {
    utils::AlignedBuffer<512> mBuffer;
    BufferFrame* mBf;

    BufferFrameHolder(size_t pageSize, PID pageId)
        : mBuffer(512 + pageSize),
          mBf(new(mBuffer.Get()) BufferFrame()) {
      mBf->mHeader.mPageId = pageId;
    }
  };

  void SetUp() override {
    // remove the test directory if it exists
    TearDown();

    // create the test directory
    auto ret = system(std::format("mkdir -p {}", mTestDir).c_str());
    EXPECT_EQ(ret, 0) << std::format(
        "Failed to create test directory, testDir={}, errno={}, error={}",
        mTestDir, errno, strerror(errno));
  }

  void TearDown() override {
    // remove the test directory
    // auto ret = system(std::format("rm -rf {}", mTestDir).c_str());
    // EXPECT_EQ(ret, 0) << std::format(
    //     "Failed to remove test directory, testDir={}, errno={}, error={}",
    //     mTestDir, errno, strerror(errno));
  }

  std::string getRandTestFile() {
    return std::format("{}/{}", mTestDir,
                       utils::RandomGenerator::RandAlphString(8));
  }

  int openFile(const std::string& fileName) {
    // open the file
    auto flag = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    int fd = open(fileName.c_str(), flag, 0666);
    EXPECT_NE(fd, -1) << std::format(
        "Failed to open file, fileName={}, errno={}, error={}", fileName, errno,
        strerror(errno));

    return fd;
  }

  void closeFile(int fd) {
    ASSERT_EQ(close(fd), 0)
        << std::format("Failed to close file, fd={}, errno={}, error={}", fd,
                       errno, strerror(errno));
  }

  void removeFile(const std::string& fileName) {
    ASSERT_EQ(remove(fileName.c_str()), 0)
        << std::format("Failed to remove file, fileName={}, errno={}, error={}",
                       fileName, errno, strerror(errno));
  }
};

TEST_F(AsyncWriteBufferTest, Basic) {
  FLAGS_init = false;

  auto testFile = getRandTestFile();
  auto testFd = openFile(testFile);
  SCOPED_DEFER({
    closeFile(testFd);
    LOG(INFO) << "Test file=" << testFile;
    // removeFile(testFile);
  });

  auto testPageSize = 512;
  auto testMaxBatchSize = 8;
  AsyncWriteBuffer testWriteBuffer(testFd, testPageSize, testMaxBatchSize);

  for (int i = 0; i < testMaxBatchSize; i++) {
    EXPECT_FALSE(testWriteBuffer.IsFull());
    BufferFrameHolder bfHolder(testPageSize, i);

    // set the payload to the pageId
    *reinterpret_cast<int64_t*>(bfHolder.mBf->mPage.mPayload) = i;
    testWriteBuffer.Add(*bfHolder.mBf);
  }

  // now the write buffer should be full
  EXPECT_TRUE(testWriteBuffer.IsFull());

  // submit the IO request
  auto result = testWriteBuffer.SubmitAll();
  ASSERT_TRUE(result) << "Failed to submit IO request, error="
                      << result.error().ToString();
  EXPECT_EQ(result.value(), testMaxBatchSize);

  // wait for the IO request to complete
  result = testWriteBuffer.WaitAll();
  auto doneRequests = result.value();
  EXPECT_EQ(doneRequests, testMaxBatchSize);
  EXPECT_EQ(testWriteBuffer.GetPendingRequests(), 0);

  // read the file content
  for (int i = 0; i < testMaxBatchSize; i++) {
    BufferFrameHolder bfHolder(testPageSize, i);
    auto ret =
        pread(testFd, &bfHolder.mBf->mPage, testPageSize, testPageSize * i);
    EXPECT_EQ(ret, testPageSize);
    auto payload = *reinterpret_cast<int64_t*>(bfHolder.mBf->mPage.mPayload);
    EXPECT_EQ(payload, i);
  }
}

} // namespace leanstore::storage::test