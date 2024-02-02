#pragma once

#include "leanstore/Store.hpp"
#include "profiling/tables/ConfigsTable.hpp"
#include "shared-headers/Units.hpp"
#include "utils/DebugFlags.hpp"
#include "utils/Error.hpp"

#include <gflags/gflags.h>
#include <rapidjson/document.h>


#include <atomic>
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

struct GlobalStats {
  u64 mAccumulatedTxCounter = 0;
};

class LeanStore {
public:
  static std::expected<std::unique_ptr<LeanStore>, utils::Error> Open();

public:
  /// The storage option for leanstore
  StoreOption mStoreOption;

  /// The file descriptor for pages
  s32 mPageFd;

  /// The file descriptor for write-ahead log
  s32 mWalFd;

  std::atomic<u64> mNumProfilingThreads = 0;

  std::atomic<bool> mProfilingThreadKeepRunning = true;

  profiling::ConfigsTable mConfigsTable;

  u64 mConfigHash = 0;

  GlobalStats mGlobalStats;

  /// The tree registry
  std::unique_ptr<storage::TreeRegistry> mTreeRegistry;

  /// The Buffer manager
  std::unique_ptr<storage::BufferManager> mBufferManager;

  /// The concurrent resource manager
  std::unique_ptr<cr::CRManager> mCRManager;

  /// The global timestamp oracle, used to generate start and commit timestamps
  /// for all transactions in the store. Start from a positive number, 0
  /// indicates invalid timestamp
  std::atomic<u64> mTimestampOracle = 1;

#ifdef DEBUG
  utils::DebugFlagsRegistry mDebugFlagsRegistry;
#endif

public:
  LeanStore();

  ~LeanStore();

public:
  /// Register a BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void CreateBasicKV(const std::string& name,
                     storage::btree::BTreeConfig& config,
                     storage::btree::BasicKV** btree);

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
  void CreateTransactionKV(const std::string& name,
                           storage::btree::BTreeConfig& config,
                           storage::btree::TransactionKV** btree);

  /// Get a registered TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetTransactionKV(const std::string& name,
                        storage::btree::TransactionKV** btree);

  /// Unregister a TransactionKV
  /// @param name The unique name of the btree
  void DropTransactionKV(const std::string& name);

  /// Alloc a new timestamp from the timestamp oracle
  u64 AllocTs() {
    return mTimestampOracle.fetch_add(1);
  }

  std::expected<std::unique_ptr<TxWorker>, utils::Error> GetTxWorker(
      WORKERID workerId);

  /// Execute a custom user function on a worker thread.
  /// @param workerId worker to compute job
  /// @param job job
  void ExecSync(u64 workerId, std::function<void()> fn);

  /// Execute a custom user function on a worker thread asynchronously.
  /// @param workerId worker to compute job
  /// @param job job
  void ExecAsync(u64 workerId, std::function<void()> fn);

  /// Waits for the worker to complete.
  void Wait(WORKERID workerId);

  /// Waits for all Workers to complete.
  void WaitAll();

  void StartProfilingThread();

private:
  u64 getConfigHash() {
    return mConfigHash;
  }

  GlobalStats getGlobalStats() {
    return mGlobalStats;
  }

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
