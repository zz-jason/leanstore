#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/base/slice.hpp"
#include "leanstore/btree/btree_iter.hpp"

#include <cstdint>
#include <memory>

namespace leanstore {

// Forward declarations
class LeanBTree;

/// LeanCursor represents a cursor for iterating over a B-tree.
/// It provides navigation and data access methods.
class LeanCursor {
public:
  /// Constructor: internal use only.
  LeanCursor(LeanBTree* btree, std::unique_ptr<BTreeIter> cursor);

  LeanCursor(const LeanCursor&) = delete;
  auto operator=(const LeanCursor&) -> LeanCursor& = delete;
  LeanCursor(LeanCursor&&) noexcept = default;
  auto operator=(LeanCursor&&) noexcept -> LeanCursor& = default;

  /// Destructor automatically closes the cursor.
  ~LeanCursor();

  /// Position the cursor at the first key in the B-tree.
  /// @return true if the cursor is valid, false if the B-tree is empty.
  bool SeekToFirst();

  /// Position the cursor at the first key greater than or equal to the given key.
  /// @param key The key to seek to.
  /// @return true if the cursor is valid, false if no such key exists.
  bool SeekToFirstGe(Slice key);

  /// Position the cursor at the last key in the B-tree.
  /// @return true if the cursor is valid, false if the B-tree is empty.
  bool SeekToLast();

  /// Position the cursor at the last key less than or equal to the given key.
  /// @param key The key to seek to.
  /// @return true if the cursor is valid, false if no such key exists.
  bool SeekToLastLe(Slice key);

  /// Move the cursor to the next key.
  /// @return true if the cursor is valid after moving, false if no more keys.
  bool Next();

  /// Move the cursor to the previous key.
  /// @return true if the cursor is valid after moving, false if no more keys.
  bool Prev();

  /// Check if the cursor is currently pointing to a valid key-value pair.
  /// @return true if valid, false otherwise.
  bool IsValid() const;

  /// Get the current key pointed to by the cursor.
  /// @return The key as a Slice.
  Slice CurrentKey() const;

  /// Get the current value pointed to by the cursor.
  /// @return The value as a Slice.
  Slice CurrentValue() const;

  /// Remove the current key-value pair from the B-tree.
  /// @return Result indicating success or failure.
  Result<void> RemoveCurrent();

  /// Update the value of the current key-value pair.
  /// @param new_value The new value.
  /// @return Result indicating success or failure.
  Result<void> UpdateCurrent(Slice new_value);

  /// Close the cursor (RAII automatically calls this).
  void Close();

private:
  LeanBTree* btree_;
  std::unique_ptr<BTreeIter> cursor_;
};

} // namespace leanstore
