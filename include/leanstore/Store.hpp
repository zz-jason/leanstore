#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/Slice.hpp"
#include "leanstore/Units.hpp"
#include "utils/Result.hpp"

#include <expected>
#include <functional>
#include <memory>

namespace leanstore {

class StoreOption;
class TxWorker;
class TableIterator;

using TableRef = void*;

class Store {
public:
  /// Create a leanstore instance, a instance is composed of:
  ///   - Database files for pages
  ///   - Wal files for write-ahead log
  ///   - An in memory buffer pool for pages
  ///   - A set of worker threads
  ///   - A set of buffer provider threads
  ///   - A group commit thread
  ///
  /// Typically, users only need to open one leanstore instance per process.
  /// The store is automatically closed when the returned unique_ptr is
  /// destroyed.
  static Result<std::unique_ptr<Store>> New(StoreOption option);

public:
  virtual ~Store() = default;

  /// Get a transaction worker, all the transaction operations are dispatched
  /// to the worker thread.
  virtual Result<std::unique_ptr<TxWorker>> GetTxWorker(WORKERID workerId) = 0;

  /// Execute a custom user function on a worker thread.
  virtual void ExecSync(WORKERID workerId, std::function<void()> fn) = 0;

  /// Execute a custom user function on a worker thread asynchronously. It's
  /// expected to call this function with Wait(). For example:
  ///   store.ExecAsync(0, []() { /* do something */ });
  ///   /* do something else ... */
  ///   store.Wait(0);
  virtual void ExecAsync(WORKERID workerId, std::function<void()> fn) = 0;

  /// Wait the completion of the execution of a custom user function on a worker
  /// thread.
  virtual void Wait(WORKERID workerId) = 0;
};

class StoreOption {
public:
  /// The directory to store all the files.
  std::string mStoreDir = "";

  /// Create from scratch if true, otherwise open from existing files.
  bool mCreateFromScratch = false;

  /// The number of partitions. For buffer manager.
  uint32_t mNumPartitions = 64;

  /// The buffer pool size (bytes). For buffer manager.
  uint64_t mBufferPoolSize = 1 * 1024 * 1024 * 1024;

  /// The page size (bytes). For buffer manager.
  uint64_t mPageSize = 4 * 1024;

  /// The in-memory wal ring-buffer size.
  uint64_t mWalRingBufferSize = 1024;

  /// The number of transaction worker threads.
  uint32_t mNumTxWorkers = 4;

  /// The number of buffer provider threads.
  uint32_t mNumBufferProviders = 1;

  /// Whether to enable garbage collection.
  bool mEnableGc = true;

  /// Whether to enable eager garbage collection. To enable eager garbage
  /// collection, the garbage collection must be enabled first. Once  enabled,
  /// the garbage collection will be triggered after each transaction commit and
  /// abort.
  bool mEnableEagerGc = true;
};

class TxWorker {
public:
  virtual ~TxWorker() = default;

  /// Start a transaction.
  virtual Result<void> StartTx(TxMode mode, IsolationLevel level) = 0;

  /// Commit a transaction.
  virtual Result<void> CommitTx() = 0;

  /// Abort a transaction.
  virtual Result<void> AbortTx() = 0;

  /// Create a table, should be executed inside a transaction.
  virtual Result<TableRef> CreateTable(const std::string& name) = 0;

  /// Get a table, should be executed inside a transaction.
  virtual Result<TableRef> GetTable(const std::string& name) = 0;

  /// Drop a table, should be executed inside a transaction.
  virtual Result<void> DropTable(const std::string& name) = 0;

  /// Put a key-value pair into a table, should be executed inside a
  /// transaction.
  virtual Result<void> Put(TableRef table, Slice key, Slice value) = 0;

  /// Update a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Update(TableRef table, Slice key, Slice value) = 0;

  /// Delete a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Delete(TableRef table, Slice key) = 0;

  /// Get a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Get(TableRef table, Slice key, std::string* value) = 0;

  /// Create an iterator to scan a table, should be executed inside a
  /// transaction.
  virtual Result<std::unique_ptr<TableIterator>> NewTableIterator(
      TableRef table) = 0;

  /// Get the total key-value pairs in a table, should be executed inside a
  /// transaction.
  virtual Result<uint64_t> GetTableSize(TableRef table) = 0;
};

/// TableIterator is used to scan a table.
/// Example 1, scan ascending from key "a" to key "z":
///
///   auto iter = tx.NewTableIterator(tblRef);
///   for (iter.Seek("a"); iter.Valid(); iter.Next()) {
///     auto [key, value] = iter.KeyAndValue();
///     if (key > "z") {
///       break;
///     }
///   }
///
/// Example 2, scan descending from key "z" to key "a":
///
///   auto iter = tx.NewTableIterator(tblRef);
///   for (iter.Seek("z"); iter.Valid(); iter.Prev()) {
///     auto [key, value] = iter.KeyAndValue();
///     if (key < "a") {
///       break;
///     }
///   }
///
class TableIterator {
public:
  ~TableIterator() = default;

  /// Seek the iterator to the first key-value pair.
  virtual void SeekToFirst() = 0;

  /// Seek the iterator to the last key-value pair.
  virtual void SeekToLast() = 0;

  /// Seek the iterator to the key-value pair with the given key.
  virtual void Seek(Slice target) = 0;

  /// Check if the iterator is valid.
  virtual bool Valid() = 0;

  /// Return the error message if the iterator is invalid.
  virtual Result<void> Error() = 0;

  /// Move the iterator to the next key-value pair.
  virtual void Next() = 0;

  /// Move the iterator to the previous key-value pair.
  virtual void Prev() = 0;

  /// Get the key of the current key-value pair.
  virtual Slice Key() = 0;

  /// Get the value of the current key-value pair.
  virtual Slice Value() = 0;

  /// Get the key and value of the current key-value pair.
  virtual std::pair<Slice, Slice> KeyAndValue() = 0;
};

} // namespace leanstore