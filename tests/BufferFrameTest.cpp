#include "storage/buffer-manager/BufferFrame.hpp"

#include <gtest/gtest.h>

using namespace leanstore::storage;

namespace leanstore::test {

// Test buffer frame related sizes
TEST(BufferFrameTest, BufferFrameSize) {
  EXPECT_EQ(BufferFrame::Size() - 512, FLAGS_page_size);
}

} // namespace leanstore::test