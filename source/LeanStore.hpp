#pragma once

#include "leanstore/Store.hpp"
#include "profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BasicKV.hpp"
#include "storage/btree/TransactionKV.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "utils/Error.hpp"

#include <rapidjson/document.h>

#include <atomic>
#include <expected>
#include <list>

namespace leanstore {

namespace cr {

class CRManager;

} // namespace cr

using FlagListString = std::list<std::tuple<string, fLS::clstring*>>;
using FlagListS64 = std::list<std::tuple<string, s64*>>;

struct GlobalStats {
  u64 mAccumulatedTxCounter = 0;
};

class LeanStore {
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
  storage::TreeRegistry* mTreeRegistry;

  /// The Buffer manager
  storage::BufferManager* mBufferManager;

  /// The concurrent resource manager
  cr::CRManager* mCRManager;

public:
  LeanStore();
  ~LeanStore();

public:
  /// Register a BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterBasicKV(const std::string& name,
                       storage::btree::BTreeGeneric::Config& config,
                       storage::btree::BasicKV** btree);

  /// Get a registered BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetBasicKV(const std::string& name, storage::btree::BasicKV** btree) {
    *btree =
        dynamic_cast<storage::btree::BasicKV*>(mTreeRegistry->GetTree(name));
  }

  /// Unregister a BasicKV
  /// @param name The unique name of the btree
  void UnRegisterBasicKV(const std::string& name);

  /// Register a TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterTransactionKV(const std::string& name,
                             storage::btree::BTreeGeneric::Config& config,
                             storage::btree::TransactionKV** btree);

  /// Get a registered TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetTransactionKV(const std::string& name,
                        storage::btree::TransactionKV** btree) {
    *btree = dynamic_cast<storage::btree::TransactionKV*>(
        mTreeRegistry->GetTree(name));
  }

  /// Unregister a TransactionKV
  /// @param name The unique name of the btree
  void UnRegisterTransactionKV(const std::string& name);

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

  void recoverFromExistingStore();

private:
  //----------------------------------------------------------------------------
  // static members
  //----------------------------------------------------------------------------

  static FlagListString sPersistedStringFlags;
  static FlagListS64 sPersistedS64Flags;

public:
  //----------------------------------------------------------------------------
  // static functions
  //----------------------------------------------------------------------------

  static void AddStringFlag(string name, fLS::clstring* flag) {
    sPersistedStringFlags.push_back(std::make_tuple(name, flag));
  }

  static void AddS64Flag(string name, s64* flag) {
    sPersistedS64Flags.push_back(std::make_tuple(name, flag));
  }

  static std::expected<LeanStore*, utils::Error> Open();
};

} // namespace leanstore
