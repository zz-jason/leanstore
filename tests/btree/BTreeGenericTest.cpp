#include "btree/core/BTreeGeneric.hpp"

#include "buffer-manager/BufferManager.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "utils/Defer.hpp"
#include "utils/JsonUtil.hpp"
#include "utils/RandomGenerator.hpp"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace leanstore::btree::test {

class BTreeGenericTest : public ::testing::Test {
protected:
  std::unique_ptr<LeanStore> mStore;

  /// Create a leanstore instance for each test case
  BTreeGenericTest() {
    auto* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    auto curTestName = std::string(curTest->test_case_name()) + "_" +
                       std::string(curTest->name());
    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = "/tmp/" + curTestName;
    FLAGS_worker_threads = 3;
    FLAGS_enable_eager_garbage_collection = true;
    auto res = LeanStore::Open();
    mStore = std::move(res.value());
  }

  ~BTreeGenericTest() = default;
};

TEST_F(BTreeGenericTest, TryMergeMayJump) {
}

} // namespace leanstore::btree::test