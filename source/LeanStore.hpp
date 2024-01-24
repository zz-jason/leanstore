#pragma once

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
  /// Register a BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterBasicKV(const std::string& name,
                       storage::btree::BTreeGeneric::Config& config,
                       storage::btree::BasicKV** btree) {
    DCHECK(cr::Worker::My().IsTxStarted());
    auto res = storage::btree::BasicKV::Create(name, config);
    if (!res) {
      LOG(ERROR) << "Failed to register BasicKV"
                 << ", name=" << name << ", error=" << res.error().ToString();
      *btree = nullptr;
      return;
    }

    *btree = res.value();
  }

  /// Get a registered BasicKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetBasicKV(const std::string& name, storage::btree::BasicKV** btree) {
    *btree = dynamic_cast<storage::btree::BasicKV*>(
        storage::TreeRegistry::sInstance->GetTree(name));
  }

  /// Unregister a BasicKV
  /// @param name The unique name of the btree
  void UnRegisterBasicKV(const std::string& name) {
    DCHECK(cr::Worker::My().IsTxStarted());
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(name));
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    auto res = storage::TreeRegistry::sInstance->UnregisterTree(name);
    if (!res) {
      LOG(ERROR) << "UnRegister BasicKV failed"
                 << ", error=" << res.error().ToString();
    }
  }

  /// Register a TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param config The config of the btree
  /// @param btree The pointer to store the registered btree
  void RegisterTransactionKV(const std::string& name,
                             storage::btree::BTreeGeneric::Config& config,
                             storage::btree::TransactionKV** btree) {
    DCHECK(cr::Worker::My().IsTxStarted());
    *btree = nullptr;

    // create btree for graveyard
    auto graveyardName = "_" + name + "_graveyard";
    auto graveyardConfig = storage::btree::BTreeGeneric::Config{
        .mEnableWal = false, .mUseBulkInsert = false};
    auto res = storage::btree::BasicKV::Create(graveyardName, graveyardConfig);
    if (!res) {
      LOG(ERROR) << "Failed to create TransactionKV graveyard"
                 << ", btreeVI=" << name << ", graveyardName=" << graveyardName
                 << ", error=" << res.error().ToString();
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
                   << ", error=" << res.error().ToString();
      }
    });

    // create btree for main data
    *btree = storage::btree::TransactionKV::Create(name, config, graveyard);
  }

  /// Get a registered TransactionKV
  ///
  /// @param name The unique name of the btree
  /// @param btree The pointer to store the found btree
  void GetTransactionKV(const std::string& name,
                        storage::btree::TransactionKV** btree) {
    *btree = dynamic_cast<storage::btree::TransactionKV*>(
        storage::TreeRegistry::sInstance->GetTree(name));
  }

  /// Unregister a TransactionKV
  /// @param name The unique name of the btree
  void UnRegisterTransactionKV(const std::string& name) {
    DCHECK(cr::Worker::My().IsTxStarted());
    auto* btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(name));
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    auto res = storage::TreeRegistry::sInstance->UnregisterTree(name);
    if (!res) {
      LOG(ERROR) << "UnRegister TransactionKV failed"
                 << ", error=" << res.error().ToString();
    }

    auto graveyardName = "_" + name + "_graveyard";
    btree = dynamic_cast<storage::btree::BTreeGeneric*>(
        storage::TreeRegistry::sInstance->GetTree(graveyardName));
    DCHECK(btree != nullptr) << "graveyard not found";
    leanstore::storage::btree::BTreeGeneric::FreeAndReclaim(*btree);
    res = storage::TreeRegistry::sInstance->UnregisterTree(graveyardName);
    if (!res) {
      LOG(ERROR) << "UnRegister TransactionKV graveyard failed"
                 << ", error=" << res.error().ToString();
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

  static std::expected<LeanStore*, utils::Error> Open();
};

} // namespace leanstore
