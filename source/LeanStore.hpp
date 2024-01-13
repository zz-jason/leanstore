#pragma once

#include "profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/BTreeVI.hpp"
#include "storage/buffer-manager/BufferManager.hpp"

#include <rapidjson/document.h>

#include <atomic>

namespace leanstore {

using FlagListString = std::list<std::tuple<string, fLS::clstring*>>;
using FlagListS64 = std::list<std::tuple<string, s64*>>;

struct GlobalStats {
  u64 accumulated_tx_counter = 0;
};

class LeanStore {
public:
  /// The file descriptor for pages
  s32 mPageFd;

  /// The file descriptor for write-ahead log
  s32 mWalFd;

  std::atomic<u64> mNumProfilingThreads = 0;

  std::atomic<bool> mProfilingThreadKeepRunning = true;

  profiling::ConfigsTable configs_table;

  u64 config_hash = 0;

  GlobalStats global_stats;

public:
  LeanStore();
  ~LeanStore();

public:
  /// Register a BTreeLL
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterBTreeLL(const std::string& name,
                       storage::btree::BTreeGeneric::Config& config,
                       storage::btree::BTreeLL** btree) {
    DCHECK(cr::Worker::my().IsTxStarted());
    auto res = storage::btree::BTreeLL::Create(name, config);
    if (!res) {
      LOG(ERROR) << "Failed to register BTreeLL"
                 << ", name=" << name << ", error=" << res.error().mMessage;
      *btree = nullptr;
      return;
    }

    *btree = res.value();
  }

  /// Get a registered BTreeLL
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetBTreeLL(const std::string& name, storage::btree::BTreeLL** btree) {
    *btree = dynamic_cast<storage::btree::BTreeLL*>(
        storage::TreeRegistry::sInstance->GetTree(name));
  }

  /// Unregister a BTreeLL
  /// @param name The unique name of the btree
  void UnRegisterBTreeLL(const std::string& name) {
    DCHECK(cr::Worker::my().IsTxStarted());
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(name));
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    auto res = storage::TreeRegistry::sInstance->UnregisterTree(name);
    if (!res) {
      LOG(ERROR) << "UnRegister BTreeLL failed"
                 << ", error=" << res.error().mMessage;
    }
  }

  /// Register a BTreeVI
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterBTreeVI(const std::string& name,
                       storage::btree::BTreeGeneric::Config& config,
                       storage::btree::BTreeVI** btree) {
    DCHECK(cr::Worker::my().IsTxStarted());
    *btree = nullptr;

    // create btree for graveyard
    auto graveyardName = "_" + name + "_graveyard";
    auto graveyardConfig = storage::btree::BTreeGeneric::Config{
        .mEnableWal = false, .mUseBulkInsert = false};
    auto res = storage::btree::BTreeLL::Create(graveyardName, graveyardConfig);
    if (!res) {
      LOG(ERROR) << "Failed to create BTreeVI graveyard"
                 << ", btreeVI=" << name << ", graveyardName=" << graveyardName
                 << ", error=" << res.error().mMessage;
      return;
    }
    auto* graveyard = res.value();

    // clean resource on failure
    SCOPED_DEFER(if (*btree == nullptr && graveyard != nullptr) {
      leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(
          *static_cast<leanstore::storage::btree::BTreeGeneric*>(graveyard));
      auto res = TreeRegistry::sInstance->UnRegisterTree(graveyard->mTreeId);
      if (!res) {
        LOG(ERROR) << "UnRegister graveyard failed"
                   << ", error=" << res.error().mMessage;
      }
    });

    // create btree for main data
    *btree = storage::btree::BTreeVI::Create(name, config, graveyard);
  }

  /// Get a registered BTreeVI
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetBTreeVI(const std::string& name, storage::btree::BTreeVI** btree) {
    *btree = dynamic_cast<storage::btree::BTreeVI*>(
        storage::TreeRegistry::sInstance->GetTree(name));
  }

  /// Unregister a BTreeVI
  /// @param name The unique name of the btree
  void UnRegisterBTreeVI(const std::string& name) {
    DCHECK(cr::Worker::my().IsTxStarted());
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(name));
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    auto res = storage::TreeRegistry::sInstance->UnregisterTree(name);
    if (!res) {
      LOG(ERROR) << "UnRegister BTreeVI failed"
                 << ", error=" << res.error().mMessage;
    }

    auto graveyardName = "_" + name + "_graveyard";
    btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(graveyardName));
    DCHECK(btree != nullptr) << "graveyard not found";
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    res = storage::TreeRegistry::sInstance->UnregisterTree(graveyardName);
    if (!res) {
      LOG(ERROR) << "UnRegister BTreeVI graveyard failed"
                 << ", error=" << res.error().mMessage;
    }
  }

  void startProfilingThread();

private:
  u64 getConfigHash() {
    return config_hash;
  }

  GlobalStats getGlobalStats() {
    return global_stats;
  }

  /// SerializeMeta serializes all the metadata about concurrent resources,
  /// buffer manager, btrees, and flags
  void SerializeMeta();

  /// SerializeFlags serializes all the persisted flags to the provided json.
  void SerializeFlags(rapidjson::Document& d);

  /// DeSerializeMeta deserializes all the metadata except for the flags.
  void DeSerializeMeta();

  /// DeSerializeFlags deserializes the flags.
  void DeSerializeFlags();

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

  static void addStringFlag(string name, fLS::clstring* flag) {
    sPersistedStringFlags.push_back(std::make_tuple(name, flag));
  }

  static void addS64Flag(string name, s64* flag) {
    sPersistedS64Flags.push_back(std::make_tuple(name, flag));
  }
};

} // namespace leanstore
