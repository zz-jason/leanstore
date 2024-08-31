#pragma once

#include "leanstore-c/StoreOption.h"
#include "leanstore/Units.hpp"
#include "leanstore/utils/DebugFlags.hpp"
#include "leanstore/utils/Result.hpp"

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

//! forward declarations
namespace leanstore::telemetry {

class MetricsManager;
class MetricsHttpExposer;

} // namespace leanstore::telemetry

//! forward declarations
namespace leanstore::storage::btree {

class BTreeGeneric;
class Config;
class BasicKV;
class TransactionKV;

} // namespace leanstore::storage::btree

//! forward declarations
namespace leanstore::storage {

class TreeRegistry;
class BufferManager;

} // namespace leanstore::storage

//! forward declarations
namespace leanstore::cr {

class CRManager;

} // namespace leanstore::cr

namespace leanstore {

class LeanStore {
public:
  //! Opens a LeanStore instance with the provided options.
  //! NOTE: The option is created by LeanStore user, its ownership is transferred to the LeanStore
  //!       instance after the call, it will be destroyed when the LeanStore instance is destroyed.
  static Result<std::unique_ptr<LeanStore>> Open(StoreOption* option);

  //! The storage option for leanstore
  const StoreOption* mStoreOption;

  //! The file descriptor for pages
  int32_t mPageFd;

  //! The file descriptor for write-ahead log
  int32_t mWalFd;

  //! The tree registry
  std::unique_ptr<storage::TreeRegistry> mTreeRegistry;

  //! The Buffer manager
  std::unique_ptr<storage::BufferManager> mBufferManager;

  //! The concurrent resource manager
  //! NOTE: Ownerd by LeanStore instance, should be destroyed together with it
  cr::CRManager* mCRManager;

  //! The global timestamp oracle for user transactions. Used to generate start and commit
  //! timestamps for user transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> mUsrTso = 1;

  //! The global timestamp oracle for system transactions. Used to generate timestamps for system
  //! transactions. Start from a positive number, 0 indicates invalid timestamp
  std::atomic<uint64_t> mSysTso = 1;

  //! The metrics manager
  std::unique_ptr<leanstore::telemetry::MetricsManager> mMetricsManager;

  //! The http metrics exposer
  std::unique_ptr<leanstore::telemetry::MetricsHttpExposer> mMetricsExposer;

#ifdef DEBUG
  utils::DebugFlagsRegistry mDebugFlagsRegistry;
#endif

  //! The LeanStore constructor
  //! NOTE: The option is created by LeanStore user, its ownership is transferred to the LeanStore
  //!       instance after the call, it will be destroyed when the LeanStore instance is destroyed.
  LeanStore(StoreOption* option);

  //! The LeanStore destructor
  ~LeanStore();

  //! Create a BasicKV
  Result<leanstore::storage::btree::BasicKV*> CreateBasicKV(
      const std::string& name,
      BTreeConfig config = BTreeConfig{.mEnableWal = true, .mUseBulkInsert = false});

  //! Get a registered BasicKV
  void GetBasicKV(const std::string& name, storage::btree::BasicKV** btree);

  //! Unregister a BasicKV
  void DropBasicKV(const std::string& name);

  //! Register a TransactionKV
  Result<leanstore::storage::btree::TransactionKV*> CreateTransactionKV(
      const std::string& name,
      BTreeConfig config = BTreeConfig{.mEnableWal = true, .mUseBulkInsert = false});

  //! Get a registered TransactionKV
  void GetTransactionKV(const std::string& name, storage::btree::TransactionKV** btree);

  //! Unregister a TransactionKV
  void DropTransactionKV(const std::string& name);

  uint64_t GetUsrTxTs() {
    return mUsrTso.load();
  }

  uint64_t GetSysTxTs() {
    return mSysTso.load();
  }

  //! Alloc a new timestamp from the timestamp oracle
  uint64_t AllocUsrTxTs() {
    return mUsrTso.fetch_add(1);
  }

  uint64_t AllocSysTxTs() {
    return mSysTso.fetch_add(1);
  }

  //! Execute a custom user function on a worker thread.
  void ExecSync(uint64_t workerId, std::function<void()> fn);

  //! Execute a custom user function on a worker thread asynchronously.
  void ExecAsync(uint64_t workerId, std::function<void()> fn);

  //! Waits for the worker to complete.
  void Wait(WORKERID workerId);

  //! Waits for all Workers to complete.
  void WaitAll();

  std::string GetMetaFilePath() const {
    return std::string(mStoreOption->mStoreDir) + "/db.meta.json";
  }

  std::string GetDbFilePath() const {
    return std::string(mStoreOption->mStoreDir) + "/db.pages";
  }

  std::string GetWalFilePath() const {
    return std::string(mStoreOption->mStoreDir) + "/db.wal";
  }

private:
  //! serializeMeta serializes all the metadata about concurrent resources,
  //! buffer manager, btrees, and flags
  void serializeMeta(bool allPagesUpToDate);

  //! serializeFlags serializes all the persisted flags to the provided json.
  void serializeFlags(uint8_t* dest);

  //! deserializeMeta deserializes all the metadata except for the flags.
  bool deserializeMeta();

  //! deserializeFlags deserializes the flags.
  void deserializeFlags();

  void initPageAndWalFd();
};

} // namespace leanstore
