#include "leanstore/sync/optimistic_guarded.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <thread>

namespace leanstore::test {

class OptimisticGuardedTest : public ::testing::Test {
protected:
  struct TestPayload {
    int64_t a_;
    int64_t b_;
  };
};

TEST_F(OptimisticGuardedTest, Set) {
  OptimisticGuarded<TestPayload> guarded_val({.a_ = 0, .b_ = 100});

  std::thread writer([&]() {
    for (int64_t i = 0; i < 100; i++) {
      guarded_val.Set(TestPayload{.a_ = i, .b_ = 100 - i});
    }
  });

  std::thread reader([&]() {
    TestPayload copied_val;
    auto version = guarded_val.Get(copied_val);
    for (int64_t i = 0; i < 200; i++) {
      auto curr_version = guarded_val.Get(copied_val);
      if (curr_version != version) {
        EXPECT_EQ(copied_val.a_ + copied_val.b_, 100);
        EXPECT_EQ((curr_version - version) % 2, 0U);
        version = curr_version;
      }
    }
  });

  writer.join();
  reader.join();
}

TEST_F(OptimisticGuardedTest, UpdateAttribute) {
  OptimisticGuarded<TestPayload> guarded_val({.a_ = 0, .b_ = 100});

  std::thread writer([&]() {
    for (int64_t i = 0; i < 100; i++) {
      guarded_val.UpdateAttribute(&TestPayload::a_, i);
    }
  });

  std::thread reader([&]() {
    TestPayload copied_val;
    auto version = guarded_val.Get(copied_val);
    for (int64_t i = 0; i < 200; i++) {
      auto curr_version = guarded_val.Get(copied_val);
      if (curr_version != version) {
        EXPECT_EQ(copied_val.b_, 100);
        EXPECT_EQ((curr_version - version) % 2, 0U);
        version = curr_version;
      }
    }
  });

  writer.join();
  reader.join();
}

} // namespace leanstore::test
