#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/base/slice.hpp"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/tx/transaction_kv.hpp"

#include <cstdint>
#include <memory>
#include <variant>
#include <vector>

namespace leanstore {

// Forward declarations
class LeanSession;
class LeanCursor;

/// LeanBTree represents a B-tree index (basic or MVCC) for coroutine mode.
/// It provides key-value operations and cursor creation.
class LeanBTree {
public:
  /// Constructor: internal use only.
  LeanBTree(LeanSession* session, std::variant<BasicKV*, TransactionKV*> btree);

  /// Insert a key-value pair into the B-tree.
  /// @param key The key to insert.
  /// @param value The value to insert.
  /// @return Result indicating success or failure.
  Result<void> Insert(Slice key, Slice value);

  /// Remove a key from the B-tree.
  /// @param key The key to remove.
  /// @return Result indicating success or failure.
  Result<void> Remove(Slice key);

  /// Update a key to a new value.
  /// If the key does not exist, it is inserted.
  /// @param key The key to update.
  /// @param value The new value.
  /// @return Result indicating success or failure.
  Result<void> Update(Slice key, Slice value);

  /// Lookup a key in the B-tree.
  /// @param key The key to look up.
  /// @return Result containing the value if found, or an error.
  Result<std::vector<uint8_t>> Lookup(Slice key);

  /// Open a cursor for iterating over the B-tree.
  /// @return A cursor object.
  LeanCursor OpenCursor();

  /// Close the B-tree handle (RAII automatically calls this).
  void Close();

private:
  friend class LeanSession;
  friend class LeanCursor;
  LeanSession* session_;
  std::variant<BasicKV*, TransactionKV*> btree_;
};

} // namespace leanstore
