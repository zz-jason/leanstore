#include "leanstore/buffer-manager/async_write_buffer.hpp"
#include "leanstore/buffer-manager/buffer_frame.hpp"
#include "leanstore/buffer-manager/swip.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/misc.hpp"
#include "leanstore/utils/random_generator.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <vector>

#include <fcntl.h>

namespace leanstore::test {

class AsyncWriteBufferTest : public ::testing::Test {
protected:
  std::string test_dir_ = "/tmp/leanstore/AsyncWriteBufferTest";

  struct BufferFrameHolder {
    utils::AlignedBuffer<512> buffer_;
    BufferFrame* bf_;

    BufferFrameHolder(size_t page_size, lean_pid_t page_id)
        : buffer_(512 + page_size),
          bf_(new(buffer_.Get()) BufferFrame()) {
      bf_->Init(page_id);
    }
  };

  void SetUp() override {
    // remove the test directory if it exists
    TearDown();

    // create the test directory
    auto ret = system(std::format("mkdir -p {}", test_dir_).c_str());
    EXPECT_EQ(ret, 0) << std::format(
        "Failed to create test directory, testDir={}, errno={}, error={}", test_dir_, errno,
        strerror(errno));
  }

  void TearDown() override {
  }

  std::string get_rand_test_file() {
    return std::format("{}/{}", test_dir_, utils::RandomGenerator::RandAlphString(8));
  }

  int open_file(const std::string& file_name) {
    // open the file
    auto flag = O_TRUNC | O_CREAT | O_RDWR | O_DIRECT;
    int fd = open(file_name.c_str(), flag, 0666);
    EXPECT_NE(fd, -1) << std::format("Failed to open file, fileName={}, errno={}, error={}",
                                     file_name, errno, strerror(errno));

    return fd;
  }

  void close_file(int fd) {
    ASSERT_EQ(close(fd), 0) << std::format("Failed to close file, fd={}, errno={}, error={}", fd,
                                           errno, strerror(errno));
  }

  void remove_file(const std::string& file_name) {
    ASSERT_EQ(remove(file_name.c_str()), 0)
        << std::format("Failed to remove file, fileName={}, errno={}, error={}", file_name, errno,
                       strerror(errno));
  }
};

TEST_F(AsyncWriteBufferTest, Basic) {
  auto test_file = get_rand_test_file();
  auto test_fd = open_file(test_file);
  SCOPED_DEFER({
    close_file(test_fd);
    Log::Info("Test file={}", test_file);
  });

  auto test_page_size = 512;
  auto test_max_batch_size = 8;
  AsyncWriteBuffer test_write_buffer(test_fd, test_page_size, test_max_batch_size);
  std::vector<std::unique_ptr<BufferFrameHolder>> bf_holders;
  for (int i = 0; i < test_max_batch_size; i++) {
    bf_holders.push_back(std::make_unique<BufferFrameHolder>(test_page_size, i));
    EXPECT_FALSE(test_write_buffer.IsFull());
    // set the payload to the pageId
    *reinterpret_cast<int64_t*>(bf_holders[i]->bf_->page_.payload_) = i;
    test_write_buffer.Add(*bf_holders[i]->bf_);
  }

  // now the write buffer should be full
  EXPECT_TRUE(test_write_buffer.IsFull());

  // submit the IO request
  auto result = test_write_buffer.SubmitAll();
  ASSERT_TRUE(result) << "Failed to submit IO request, error=" << result.error().ToString();
  EXPECT_EQ(result.value(), test_max_batch_size);

  // wait for the IO request to complete
  result = test_write_buffer.WaitAll();
  auto done_requests = result.value();
  EXPECT_EQ(done_requests, test_max_batch_size);
  EXPECT_EQ(test_write_buffer.GetPendingRequests(), 0);

  // check the flushed content
  test_write_buffer.IterateFlushedBfs(
      [](BufferFrame& flushed_bf, uint64_t flushed_psn) {
        EXPECT_FALSE(flushed_bf.IsDirty());
        EXPECT_FALSE(flushed_bf.IsFree());
        EXPECT_EQ(flushed_psn, 0);
      },
      test_max_batch_size);

  // read the file content
  for (int i = 0; i < test_max_batch_size; i++) {
    BufferFrameHolder bf_holder(test_page_size, i);
    auto ret = pread(test_fd, reinterpret_cast<void*>(bf_holder.buffer_.Get() + 512),
                     test_page_size, test_page_size * i);
    EXPECT_EQ(ret, test_page_size);
    auto payload = *reinterpret_cast<int64_t*>(bf_holder.bf_->page_.payload_);
    EXPECT_EQ(payload, i);
  }
}

} // namespace leanstore::test