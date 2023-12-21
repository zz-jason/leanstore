#include "storage/buffer-manager/BufferFrame.hpp"

#include <gtest/gtest.h>

namespace leanstore {

using namespace leanstore::storage;

// Test buffer frame related sizes
TEST(BufferFrameTest, BufferFrameSize) {
  EXPECT_EQ(BufferFrame::Size() - 512, FLAGS_page_size);
}

} // namespace leanstore