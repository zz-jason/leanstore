#pragma once

#include "leanstore/Store.hpp"
#include "leanstore/Units.hpp"
#include "telemetry/MetricsManager.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Error.hpp"

#include <gflags/gflags.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <expected>
#include <limits>
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
  static std::expected<std::unique_ptr<LeanStore>, utils::Error> Open();

public:
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

public:
  LeanStore();

  ~LeanStore();

public:
  /// Create a BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  std::expected<storage::btree::BasicKV*, utils::Error> CreateBasicKV(
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
  std::expected<storage::btree::TransactionKV*, utils::Error>
  CreateTransactionKV(const std::string& name,
                      storage::btree::BTreeConfig& config);

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
    // return std::numeric_limits<uint64_t>::max();
    return mTimestampOracle.load();
  }

  /// Alloc a new timestamp from the timestamp oracle
  uint64_t AllocTs() {
    return mTimestampOracle.fetch_add(1);
  }

  std::expected<std::unique_ptr<TxWorker>, utils::Error> GetTxWorker(
      WORKERID workerId);

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

  void initStoreOption();

  void initGoogleLog();

  void initPageAndWalFd();
};

} // namespace leanstore
