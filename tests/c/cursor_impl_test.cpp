#include "common/lean_test_suite.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/c/status.h"
#include "leanstore/c/types.h"

#include "gtest/gtest.h"
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace leanstore::test {

class CursorImplTest : public LeanTestSuite {
protected:
  void SetUp() override {
    lean_store_option* option = lean_store_option_create(TestCaseStoreDir().c_str());
    option->create_from_scratch_ = true;
    option->worker_threads_ = 2;
    option->enable_bulk_insert_ = false;
    option->enable_eager_gc_ = true;

    auto status = lean_open_store(option, &store_);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);
    ASSERT_NE(store_, nullptr);

    auto* btree_name = "atomic_iterator_test_tree";
    session_ = store_->connect(store_);
    ASSERT_NE(session_, nullptr);

    status = session_->create_btree(session_, btree_name, lean_btree_type::LEAN_BTREE_TYPE_ATOMIC);
    ASSERT_EQ(status, lean_status::LEAN_STATUS_OK);

    btree_ = session_->get_btree(session_, btree_name);
    ASSERT_NE(btree_, nullptr);
  }

  void TearDown() override {
    if (btree_) {
      btree_->close(btree_);
    }
    if (session_) {
      session_->close(session_);
    }
    if (store_) {
      store_->close(store_);
    }
  }

  void InsertTestData(int num_entries) {
    keys_.clear();
    vals_.clear();
    keys_.resize(num_entries);
    vals_.resize(num_entries);

    for (int i = 0; i < num_entries; i++) {
      // Use fixed-width keys with zero padding to ensure string order matches numeric order
      keys_[i] = std::to_string(1000000 + i).substr(1); // Creates 6-digit zero-padded strings
      vals_[i] = std::to_string(i * 2);
    }

    for (int i = 0; i < num_entries; i++) {
      lean_str_view key = {keys_[i].data(), keys_[i].size()};
      lean_str_view val = {vals_[i].data(), vals_[i].size()};
      ASSERT_EQ(btree_->insert(btree_, key, val), lean_status::LEAN_STATUS_OK);
    }
  }

  void InsertRandomTestData(int num_entries, int seed = 42) {
    keys_.clear();
    vals_.clear();

    std::vector<int> indices(num_entries);
    std::iota(indices.begin(), indices.end(), 0);

    std::mt19937 rng(seed);
    std::shuffle(indices.begin(), indices.end(), rng);

    keys_.resize(num_entries);
    vals_.resize(num_entries);

    for (int i = 0; i < num_entries; i++) {
      // Use fixed-width keys with zero padding
      keys_[i] = std::to_string(1000000 + indices[i]).substr(1);
      vals_[i] = std::to_string(indices[i] * 2);
    }

    for (int i = 0; i < num_entries; i++) {
      lean_str_view key = {keys_[i].data(), keys_[i].size()};
      lean_str_view val = {vals_[i].data(), vals_[i].size()};
      ASSERT_EQ(btree_->insert(btree_, key, val), lean_status::LEAN_STATUS_OK);
    }
  }

  lean_store* store_;
  lean_session* session_;
  lean_btree* btree_;
  std::vector<std::string> keys_;
  std::vector<std::string> vals_;
};

// Test iterator on empty BTree
TEST_F(CursorImplTest, EmptyBTreeIterator) {
  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  // Test all methods on empty btree
  EXPECT_FALSE(cursor->seek_to_first(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  EXPECT_FALSE(cursor->seek_to_last(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  EXPECT_FALSE(cursor->next(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  EXPECT_FALSE(cursor->prev(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Test seek operations on empty btree
  lean_str_view test_key = {"test", 4};
  EXPECT_FALSE(cursor->seek_to_first_ge(cursor, test_key));
  EXPECT_FALSE(cursor->is_valid(cursor));

  EXPECT_FALSE(cursor->seek_to_last_le(cursor, test_key));
  EXPECT_FALSE(cursor->is_valid(cursor));

  cursor->close(cursor);
}

// Test iterator invalid operations
TEST_F(CursorImplTest, InvalidOperations) {
  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Try to get current key/value without valid position
  // Note: These may crash or return undefined behavior, but we test if they're handled gracefully
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Most implementations would either crash or return false/null for invalid operations
  // We test the expected behavior based on the iterator state

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test forward iteration
TEST_F(CursorImplTest, ForwardIteration) {
  const int num_entries = 100;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test seek_to_first and forward iteration
  EXPECT_TRUE(cursor->seek_to_first(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));

  int count = 0;
  std::vector<std::string> visited_keys;

  do {
    cursor->current_key(cursor, &key);
    cursor->current_value(cursor, &value);

    std::string key_str(key.data, key.size);
    int val_int = std::stoi(std::string(value.data, value.size));

    visited_keys.push_back(key_str);

    // Convert zero-padded key back to int for validation
    int key_int = std::stoi(key_str);
    EXPECT_EQ(key_int * 2, val_int);
    count++;
  } while (cursor->next(cursor));

  EXPECT_EQ(count, num_entries);
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Verify keys are in ascending order (string comparison should work with zero-padded keys)
  for (size_t i = 1; i < visited_keys.size(); i++) {
    EXPECT_LT(visited_keys[i - 1], visited_keys[i]);
  }

  // Try to go further
  EXPECT_FALSE(cursor->next(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test backward iteration
TEST_F(CursorImplTest, BackwardIteration) {
  const int num_entries = 100;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test seek_to_last and backward iteration
  EXPECT_TRUE(cursor->seek_to_last(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));

  int count = 0;
  std::vector<std::string> visited_keys;

  do {
    cursor->current_key(cursor, &key);
    cursor->current_value(cursor, &value);

    std::string key_str(key.data, key.size);
    int val_int = std::stoi(std::string(value.data, value.size));

    visited_keys.push_back(key_str);

    // Convert zero-padded key back to int for validation
    int key_int = std::stoi(key_str);
    EXPECT_EQ(key_int * 2, val_int);
    count++;
  } while (cursor->prev(cursor));

  EXPECT_EQ(count, num_entries);
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Verify keys are in descending order (string comparison)
  for (size_t i = 1; i < visited_keys.size(); i++) {
    EXPECT_GT(visited_keys[i - 1], visited_keys[i]);
  }

  // Try to go further
  EXPECT_FALSE(cursor->prev(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test seek_to_first_ge functionality
TEST_F(CursorImplTest, SeekToFirstGreaterEqual) {
  const int num_entries = 50;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test seeking to existing keys
  for (int i = 0; i < num_entries; i++) {
    std::string seek_key = std::to_string(1000000 + i).substr(1);
    lean_str_view seek_key_view = {seek_key.data(), seek_key.size()};

    EXPECT_TRUE(cursor->seek_to_first_ge(cursor, seek_key_view));
    EXPECT_TRUE(cursor->is_valid(cursor));

    cursor->current_key(cursor, &key);
    std::string found_key_str(key.data, key.size);
    EXPECT_GE(found_key_str, seek_key);
  }

  // Test seeking to non-existing key in the middle
  std::string middle_key = std::to_string(1000000 + 25).substr(1) + ".5"; // "000025.5"
  lean_str_view middle_key_view = {middle_key.data(), middle_key.size()};
  EXPECT_TRUE(cursor->seek_to_first_ge(cursor, middle_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string found_key_str(key.data, key.size);
  EXPECT_GE(found_key_str, middle_key);

  // Test seeking beyond all keys
  std::string large_key = std::to_string(1000000 + num_entries + 100).substr(1);
  lean_str_view large_key_view = {large_key.data(), large_key.size()};
  EXPECT_FALSE(cursor->seek_to_first_ge(cursor, large_key_view));
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test seek_to_last_le functionality
TEST_F(CursorImplTest, SeekToLastLessEqual) {
  const int num_entries = 50;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test seeking to existing keys
  for (int i = 0; i < num_entries; i++) {
    std::string seek_key = std::to_string(1000000 + i).substr(1);
    lean_str_view seek_key_view = {seek_key.data(), seek_key.size()};

    EXPECT_TRUE(cursor->seek_to_last_le(cursor, seek_key_view));
    EXPECT_TRUE(cursor->is_valid(cursor));

    cursor->current_key(cursor, &key);
    std::string found_key_str(key.data, key.size);
    EXPECT_LE(found_key_str, seek_key);
  }

  // Test seeking to non-existing key in the middle
  std::string middle_key = std::to_string(1000000 + 25).substr(1) + ".5"; // "000025.5"
  lean_str_view middle_key_view = {middle_key.data(), middle_key.size()};
  EXPECT_TRUE(cursor->seek_to_last_le(cursor, middle_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string found_key_str(key.data, key.size);
  EXPECT_LE(found_key_str, middle_key);

  // Test seeking before all keys
  std::string small_key = "00000/"; // Before "000000"
  lean_str_view small_key_view = {small_key.data(), small_key.size()};
  EXPECT_FALSE(cursor->seek_to_last_le(cursor, small_key_view));
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test exact key finding using seek_to_first_ge
TEST_F(CursorImplTest, ExactKeyFinding) {
  const int num_entries = 50;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test finding existing keys using seek_to_first_ge
  for (int i = 0; i < num_entries; i++) {
    std::string seek_key = std::to_string(1000000 + i).substr(1);
    lean_str_view seek_key_view = {seek_key.data(), seek_key.size()};

    EXPECT_TRUE(cursor->seek_to_first_ge(cursor, seek_key_view));
    EXPECT_TRUE(cursor->is_valid(cursor));

    cursor->current_key(cursor, &key);
    cursor->current_value(cursor, &value);

    EXPECT_EQ(key.size, seek_key.size());
    EXPECT_EQ(memcmp(key.data, seek_key.data(), key.size), 0);

    int val_int = std::stoi(std::string(value.data, value.size));
    EXPECT_EQ(i * 2, val_int);
  }

  // Test seeking to non-existing keys
  std::string non_existing_key = std::to_string(1000000 + 999).substr(1);
  lean_str_view non_existing_key_view = {non_existing_key.data(), non_existing_key.size()};
  EXPECT_FALSE(cursor->seek_to_first_ge(cursor, non_existing_key_view));
  EXPECT_FALSE(cursor->is_valid(cursor));

  std::string partial_key = std::to_string(1000000 + 2).substr(1) + ".5"; // "000002.5"
  lean_str_view partial_key_view = {partial_key.data(), partial_key.size()};
  EXPECT_TRUE(cursor->seek_to_first_ge(cursor, partial_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  // Should find key "000003" (first key >= "000002.5")
  cursor->current_key(cursor, &key);
  std::string found_key_str(key.data, key.size);
  EXPECT_GE(found_key_str, partial_key);

  // Should be exactly "000003"
  std::string expected_key = std::to_string(1000000 + 3).substr(1);
  EXPECT_EQ(found_key_str, expected_key);

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test mixed iteration patterns
TEST_F(CursorImplTest, MixedIterationPatterns) {
  const int num_entries = 30;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Start from middle using seek_to_first_ge, go forward, then backward
  std::string middle_key = std::to_string(1000000 + num_entries / 2).substr(1);
  lean_str_view middle_key_view = {middle_key.data(), middle_key.size()};

  EXPECT_TRUE(cursor->seek_to_first_ge(cursor, middle_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  // Verify we found the exact key
  cursor->current_key(cursor, &key);
  EXPECT_EQ(key.size, middle_key.size());
  EXPECT_EQ(memcmp(key.data, middle_key.data(), key.size), 0);

  std::string starting_key_str(key.data, key.size);
  int starting_key_int = std::stoi(starting_key_str);

  // Go forward a few steps
  int forward_steps = 0;
  const int max_forward_steps = 5;
  for (int i = 0; i < max_forward_steps && cursor->next(cursor); i++) {
    EXPECT_TRUE(cursor->is_valid(cursor));
    cursor->current_key(cursor, &key);
    std::string key_str(key.data, key.size);
    EXPECT_GT(key_str, starting_key_str);

    // Verify we're moving forward in the expected sequence
    int key_int = std::stoi(key_str);
    EXPECT_EQ(key_int, starting_key_int + i + 1);
    forward_steps++;
  }

  // Verify we actually moved forward the expected number of steps
  // Should be min(max_forward_steps, remaining_keys_after_middle)
  int remaining_keys = num_entries - 1 - (num_entries / 2); // keys after middle position
  int expected_forward_steps = std::min(max_forward_steps, remaining_keys);
  EXPECT_EQ(forward_steps, expected_forward_steps);

  std::string final_forward_key_str(key.data, key.size);
  int final_forward_key_int = std::stoi(final_forward_key_str);

  // Go backward to original position and beyond
  int backward_steps = 0;
  std::string prev_key_str = final_forward_key_str;
  const int max_backward_steps = 10;

  for (int i = 0; i < max_backward_steps && cursor->prev(cursor); i++) {
    EXPECT_TRUE(cursor->is_valid(cursor));
    cursor->current_key(cursor, &key);
    std::string key_str(key.data, key.size);

    // Should be going in decreasing order
    EXPECT_LT(key_str, prev_key_str);

    // Verify we're moving backward in the expected sequence
    int key_int = std::stoi(key_str);
    EXPECT_EQ(key_int, final_forward_key_int - (i + 1));

    prev_key_str = key_str;
    backward_steps++;
  }

  // Verify we moved backward the expected number of steps
  // Should be min(max_backward_steps, keys_from_final_position_to_start)
  int keys_from_final_to_start =
      final_forward_key_int; // keys before final position (including key 0)
  int expected_backward_steps = std::min(max_backward_steps, keys_from_final_to_start);
  EXPECT_EQ(backward_steps, expected_backward_steps);

  // Test seek to a different position and continue iteration
  std::string new_key = std::to_string(1000000 + 5).substr(1);
  lean_str_view new_key_view = {new_key.data(), new_key.size()};
  EXPECT_TRUE(cursor->seek_to_first_ge(cursor, new_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string found_key_str(key.data, key.size);
  EXPECT_EQ(found_key_str, new_key);

  // Continue forward from new position
  int count_from_5 = 1; // Already at key 5
  std::string expected_key = new_key;
  while (cursor->next(cursor)) {
    EXPECT_TRUE(cursor->is_valid(cursor));
    cursor->current_key(cursor, &key);
    std::string key_str(key.data, key.size);

    expected_key = std::to_string(1000000 + 5 + count_from_5).substr(1);
    EXPECT_EQ(key_str, expected_key);
    count_from_5++;
  }

  // Should have iterated through all keys from 5 to num_entries-1
  EXPECT_EQ(count_from_5, num_entries - 5);
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test boundary conditions
TEST_F(CursorImplTest, BoundaryConditions) {
  const int num_entries = 3; // Small dataset for boundary testing
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Test first element
  EXPECT_TRUE(cursor->seek_to_first(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));
  cursor->current_key(cursor, &key);
  std::string first_key_str(key.data, key.size);
  std::string expected_first = std::to_string(1000000).substr(1);
  EXPECT_EQ(first_key_str, expected_first);

  // Can't go before first
  EXPECT_FALSE(cursor->prev(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Test last element
  EXPECT_TRUE(cursor->seek_to_last(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));
  cursor->current_key(cursor, &key);
  std::string last_key_str(key.data, key.size);
  std::string expected_last = std::to_string(1000000 + num_entries - 1).substr(1);
  EXPECT_EQ(last_key_str, expected_last);

  // Can't go past last
  EXPECT_FALSE(cursor->next(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test large dataset performance and correctness
TEST_F(CursorImplTest, LargeDatasetIteration) {
  const int num_entries = 1000;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // Forward iteration count
  EXPECT_TRUE(cursor->seek_to_first(cursor));
  int forward_count = 0;
  do {
    EXPECT_TRUE(cursor->is_valid(cursor));
    forward_count++;
  } while (cursor->next(cursor));

  EXPECT_EQ(forward_count, num_entries);

  // Backward iteration count
  EXPECT_TRUE(cursor->seek_to_last(cursor));
  int backward_count = 0;
  do {
    EXPECT_TRUE(cursor->is_valid(cursor));
    backward_count++;
  } while (cursor->prev(cursor));

  EXPECT_EQ(backward_count, num_entries);

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

// Test multiple cursors
TEST_F(CursorImplTest, MultipleCursors) {
  const int num_entries = 50;
  InsertTestData(num_entries);

  lean_cursor* cursor1 = btree_->open_cursor(btree_);
  lean_cursor* cursor2 = btree_->open_cursor(btree_);
  ASSERT_NE(cursor1, nullptr);
  ASSERT_NE(cursor2, nullptr);

  lean_str key1, value1, key2, value2;
  lean_str_init(&key1, 16);
  lean_str_init(&value1, 16);
  lean_str_init(&key2, 16);
  lean_str_init(&value2, 16);

  // Position cursors at different locations
  EXPECT_TRUE(cursor1->seek_to_first(cursor1));
  EXPECT_TRUE(cursor2->seek_to_last(cursor2));

  // Verify they're at different positions
  cursor1->current_key(cursor1, &key1);
  cursor2->current_key(cursor2, &key2);

  int key1_int = std::stoi(std::string(key1.data, key1.size));
  int key2_int = std::stoi(std::string(key2.data, key2.size));

  EXPECT_NE(key1_int, key2_int);
  EXPECT_LT(key1_int, key2_int);

  // Move both cursors and verify independence
  EXPECT_TRUE(cursor1->next(cursor1));
  EXPECT_TRUE(cursor2->prev(cursor2));

  cursor1->current_key(cursor1, &key1);
  cursor2->current_key(cursor2, &key2);

  int new_key1_int = std::stoi(std::string(key1.data, key1.size));
  int new_key2_int = std::stoi(std::string(key2.data, key2.size));

  EXPECT_GT(new_key1_int, key1_int); // cursor1 moved forward
  EXPECT_LT(new_key2_int, key2_int); // cursor2 moved backward

  lean_str_deinit(&key1);
  lean_str_deinit(&value1);
  lean_str_deinit(&key2);
  lean_str_deinit(&value2);
  cursor1->close(cursor1);
  cursor2->close(cursor2);
}

// Test cursor reuse after becoming invalid
TEST_F(CursorImplTest, CursorReuse) {
  const int num_entries = 20;
  InsertTestData(num_entries);

  lean_cursor* cursor = btree_->open_cursor(btree_);
  ASSERT_NE(cursor, nullptr);

  lean_str key, value;
  lean_str_init(&key, 16);
  lean_str_init(&value, 16);

  // First use: iterate to end
  EXPECT_TRUE(cursor->seek_to_first(cursor));
  int count = 0;
  do {
    EXPECT_TRUE(cursor->is_valid(cursor));
    count++;
  } while (cursor->next(cursor));

  EXPECT_EQ(count, num_entries);
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Reuse: seek to middle and iterate backward
  std::string middle_key = std::to_string(1000000 + num_entries / 2).substr(1);
  lean_str_view middle_key_view = {middle_key.data(), middle_key.size()};
  EXPECT_TRUE(cursor->seek_to_first_ge(cursor, middle_key_view));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string middle_key_str(key.data, key.size);
  EXPECT_EQ(middle_key_str, middle_key);

  // Move backward several steps
  for (int i = 0; i < 5 && cursor->prev(cursor); i++) {
    EXPECT_TRUE(cursor->is_valid(cursor));
    cursor->current_key(cursor, &key);
    std::string key_str(key.data, key.size);

    std::string expected_key = std::to_string(1000000 + num_entries / 2 - (i + 1)).substr(1);
    EXPECT_EQ(key_str, expected_key);
  }

  // Reuse again: seek to last and try to iterate forward (should fail)
  EXPECT_TRUE(cursor->seek_to_last(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string last_key_str(key.data, key.size);
  std::string expected_last = std::to_string(1000000 + num_entries - 1).substr(1);
  EXPECT_EQ(last_key_str, expected_last);

  EXPECT_FALSE(cursor->next(cursor));
  EXPECT_FALSE(cursor->is_valid(cursor));

  // Reuse once more: seek to first and verify
  EXPECT_TRUE(cursor->seek_to_first(cursor));
  EXPECT_TRUE(cursor->is_valid(cursor));

  cursor->current_key(cursor, &key);
  std::string first_key_str(key.data, key.size);
  std::string expected_first = std::to_string(1000000).substr(1);
  EXPECT_EQ(first_key_str, expected_first);

  lean_str_deinit(&key);
  lean_str_deinit(&value);
  cursor->close(cursor);
}

} // namespace leanstore::test