#include "common/lean_test_suite.hpp"
#include "leanstore/base/optional.hpp"
#include "leanstore/utils/json.hpp"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <cstring>
#include <format>

namespace leanstore::test {

class OptionalTest : public LeanTestSuite {};

TEST_F(OptionalTest, MoveSemantics) {
  utils::JsonArray json_array;
  for (int i = 0; i < 5; i++) {
    json_array.AppendString(std::format("value_{}", i));
  }

  utils::JsonObj json_obj;
  const char* key = "test_array";
  json_obj.AddJsonArray(key, json_array);

  auto opt1 = Optional<utils::JsonObj>(std::move(json_obj));
  ASSERT_TRUE(opt1);

  ASSERT_TRUE(opt1->HasMember(key));
  ASSERT_TRUE(opt1->GetJsonArray(key));
  ASSERT_TRUE(opt1->GetJsonArray(key)->Size() == 5);

  auto opt2 = opt1->GetJsonArray(key);
  ASSERT_TRUE(opt2);
  ASSERT_EQ(opt2->Size(), 5);
}

} // namespace leanstore::test
