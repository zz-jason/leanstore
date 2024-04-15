#pragma once

#include "leanstore/Store.hpp"
#include "leanstore/Units.hpp"
#include "telemetry/MetricsManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Result.hpp"

#include <rapidjson/document.h>

#include <atomic>
#include <cstdint>
#include <expected>
#include <memory>

namespace leanstore::storage::btree {

class BTreeConfig;
class BTreeGeneric;
class Config;
class BasicKV;
class TransactionKV;

} // namespace leanstore::storage::btree

namespace leanstore::storage {

class TreeRegistry;
class BufferManager;

} // namespace leanstore::storage

namespace leanstore::cr {

class CRManager;

} // namespace leanstore::cr

namespace leanstore {

class LeanStore {
public:
  static Result<std::unique_ptr<LeanStore>> Open(
      StoreOption option = StoreOption{});

  /// The storage option for leanstore
  StoreOption mStoreOption;

  /// The file descriptor for pages
  int32_t mPageFd;

  /// The file descriptor for write-ahead log
  int32_t mWalFd;

  /// The tree registry
  std::unique_ptr<storage::TreeRegistry> mTreeRegistry;

  /// The Buffer manager
  std::unique_ptr<storage::BufferManager> mBufferManager;

  /// The concurrent resource manager
  std::unique_ptr<cr::CRManager> mCRManager;

  /// The global timestamp oracle, used to generate start and commit timestamps
  /// for all transactions in the store. Start from a positive number, 0
  /// indicates invalid timestamp
  std::atomic<uint64_t> mTimestampOracle = 1;

  /// The metrics manager
  telemetry::MetricsManager mMetricsManager;

#ifdef DEBUG
  utils::DebugFlagsRegistry mDebugFlagsRegistry;
#endif

  LeanStore(StoreOption option = StoreOption{});

  ~LeanStore();

  /// Create a BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  Result<storage::btree::BasicKV*> CreateBasicKV(
      const std::string& name, storage::btree::BTreeConfig& config);

  /// Get a registered BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetBasicKV(const std::string& name, storage::btree::BasicKV** btree);

  /// Unregister a BasicKV
  /// @param name The unique name of the btree
  void DropBasicKV(const std::string& name);

  /// Register a TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  Result<storage::btree::TransactionKV*> CreateTransactionKV(
      const std::string& name, storage::btree::BTreeConfig& config);

  /// Get a registered TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetTransactionKV(const std::string& name,
                        storage::btree::TransactionKV** btree);

  /// Unregister a TransactionKV
  /// @param name The unique name of the btree
  void DropTransactionKV(const std::string& name);

  uint64_t GetTs() {
    return mTimestampOracle.load();
  }

  /// Alloc a new timestamp from the timestamp oracle
  uint64_t AllocTs() {
    return mTimestampOracle.fetch_add(1);
  }

  /// Execute a custom user function on a worker thread.
  /// @param workerId worker to compute job
  /// @param job job
  void ExecSync(uint64_t workerId, std::function<void()> fn);

  /// Execute a custom user function on a worker thread asynchronously.
  /// @param workerId worker to compute job
  /// @param job job
  void ExecAsync(uint64_t workerId, std::function<void()> fn);

  /// Waits for the worker to complete.
  void Wait(WORKERID workerId);

  /// Waits for all Workers to complete.
  void WaitAll();

private:
  /// serializeMeta serializes all the metadata about concurrent resources,
  /// buffer manager, btrees, and flags
  void serializeMeta();

  /// serializeFlags serializes all the persisted flags to the provided json.
  void serializeFlags(rapidjson::Document& d);

  /// deserializeMeta deserializes all the metadata except for the flags.
  void deserializeMeta();

  /// deserializeFlags deserializes the flags.
  void deserializeFlags();

  void initPageAndWalFd();
};

} // namespace leanstore
