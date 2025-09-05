#ifndef LEANSTORE_CPP_LEANSTORE_HPP
#define LEANSTORE_CPP_LEANSTORE_HPP

#include "leanstore/common/types.h"
#include "leanstore/cpp/status.hpp"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace leanstore {

/// Forward declarations for C++ implementation classes
class Store;   // Main database store instance with connection management
class Session; // Database session for transaction and B-tree management
class BTree;   // B-tree index for key-value storage operations
class Cursor;  // Iterator for traversing B-tree entries

/// Main database store instance with connection management
/// Concrete implementation (not an interface)
class Store {
public:
  /// Constructor - implementation defined
  Store();

  /// Destructor
  ~Store();

  /// Create new session (blocking)
  /// @return unique pointer to session, nullptr on failure
  std::unique_ptr<Session> Connect();

  /// Try to create session (non-blocking)
  /// @return unique pointer to session if successful, nullptr otherwise
  std::unique_ptr<Session> TryConnect();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  // Non-copyable, non-movable
  Store(const Store&) = delete;
  Store& operator=(const Store&) = delete;
  Store(Store&&) = delete;
  Store& operator=(Store&&) = delete;
};

/// Database session for transaction and B-tree management
/// Concrete implementation (not an interface)
class Session {
public:
  /// Constructor - implementation defined
  Session();

  /// Destructor
  ~Session();

  /// Begin new transaction
  void StartTx();

  /// Commit current transaction
  void CommitTx();

  /// Abort current transaction
  void AbortTx();

  /// Create new B-tree index
  /// @param btree_name name of the B-tree
  /// @param btree_type type of B-tree (atomic/versioned)
  /// @return Status indicating success or failure
  Status CreateBTree(const std::string& btree_name, lean_btree_type btree_type);

  /// Delete B-tree index
  /// @param btree_name name of the B-tree to delete
  void DropBTree(const std::string& btree_name);

  /// Get B-tree by name
  /// @param btree_name name of the B-tree
  /// @return unique pointer to B-tree, nullptr if not found
  std::unique_ptr<BTree> GetBTree(const std::string& btree_name);

private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  // Non-copyable, non-movable
  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;
  Session(Session&&) = delete;
  Session& operator=(Session&&) = delete;
};

/// B-tree index for key-value storage operations
/// Abstract interface (may have multiple implementations)
class BTree {
public:
  /// Virtual destructor for proper cleanup
  virtual ~BTree() = default;

  /// Insert key-value pair
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return Status indicating success or failure
  virtual Status Insert(std::string_view key, std::string_view value) = 0;

  /// Remove key and its value
  /// @param key the key to remove
  /// @return Status indicating success or failure
  virtual Status Remove(std::string_view key) = 0;

  /// Find value by key
  /// @param key the key to search for
  /// @param[out] value buffer to store the found value
  /// @return Status indicating success or failure
  virtual Status Lookup(std::string_view key, std::string* value) const = 0;

  /// Create cursor for iteration
  /// @return unique pointer to cursor, nullptr on failure
  virtual std::unique_ptr<Cursor> OpenCursor() = 0;

  // Convenience methods for easier usage

  /// Check if key exists
  /// @param key the key to check
  /// @return true if key exists
  bool Contains(std::string_view key) const {
    std::string dummy;
    return Lookup(key, &dummy).IsOk();
  }

  /// Find value by key (returns optional)
  /// @param key the key to search for
  /// @return the associated value if found, std::nullopt otherwise
  std::optional<std::string> Get(std::string_view key) const {
    std::string value;
    if (Lookup(key, &value).IsOk()) {
      return value;
    }
    return std::nullopt;
  }

protected:
  BTree() = default;

private:
  // Non-copyable, non-movable
  BTree(const BTree&) = delete;
  BTree& operator=(const BTree&) = delete;
  BTree(BTree&&) = delete;
  BTree& operator=(BTree&&) = delete;
};

/// Iterator for traversing B-tree entries
/// Abstract interface (may have multiple implementations)
class Cursor {
public:
  /// Virtual destructor for proper cleanup
  virtual ~Cursor() = default;

  /// Move to first entry
  /// @return true if successful and cursor is valid
  virtual bool SeekToFirst() = 0;

  /// Move to first entry >= key
  /// @param key the search key
  /// @return true if found and cursor is valid
  virtual bool SeekToFirstGe(std::string_view key) = 0;

  /// Move to next entry
  /// @return true if moved successfully and cursor is still valid
  virtual bool Next() = 0;

  /// Move to last entry
  /// @return true if successful and cursor is valid
  virtual bool SeekToLast() = 0;

  /// Move to last entry <= key
  /// @param key the search key
  /// @return true if found and cursor is valid
  virtual bool SeekToLastLe(std::string_view key) = 0;

  /// Move to previous entry
  /// @return true if moved successfully and cursor is still valid
  virtual bool Prev() = 0;

  /// Check if cursor is valid (positioned at a valid entry)
  /// @return true if cursor points to a valid entry
  virtual bool IsValid() const = 0;

  /// Get current key
  /// @param[out] key buffer to store the key
  virtual void GetCurrentKey(std::string* key) const = 0;

  /// Get current value
  /// @param[out] value buffer to store the value
  virtual void GetCurrentValue(std::string* value) const = 0;

  /// Remove current entry
  /// @return Status indicating success or failure
  virtual Status RemoveCurrent() = 0;

  /// Update current value
  /// @param new_value the new value to set
  /// @return Status indicating success or failure
  virtual Status UpdateCurrent(std::string_view new_value) = 0;

  // Convenience methods for easier usage

  /// Get current key as optional
  /// @return optional containing the key if cursor is valid
  std::optional<std::string> CurrentKey() const {
    if (!IsValid()) {
      return std::nullopt;
    }
    std::string key;
    GetCurrentKey(&key);
    return key;
  }

  /// Get current value as optional
  /// @return optional containing the value if cursor is valid
  std::optional<std::string> CurrentValue() const {
    if (!IsValid()) {
      return std::nullopt;
    }
    std::string value;
    GetCurrentValue(&value);
    return value;
  }

protected:
  Cursor() = default;

private:
  // Non-copyable, non-movable
  Cursor(const Cursor&) = delete;
  Cursor& operator=(const Cursor&) = delete;
  Cursor(Cursor&&) = delete;
  Cursor& operator=(Cursor&&) = delete;
};

/// Store management functions

/// Factory function to open database store
/// @param option store configuration options
/// @return unique pointer to store, nullptr on failure
std::unique_ptr<Store> OpenStore(lean_store_option* option);

/// Convenience function to open store with directory only
/// @param store_dir directory for the store
/// @return unique pointer to store, nullptr on failure
std::unique_ptr<Store> OpenStore(const std::string& store_dir);

} // namespace leanstore

#endif // LEANSTORE_CPP_LEANSTORE_HPP