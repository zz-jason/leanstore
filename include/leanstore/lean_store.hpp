#pragma once

#include "coroutine/coro_future.hpp"
#include "coroutine/coro_scheduler.hpp"
#include "leanstore/common/types.h"
#include "leanstore/utils/debug_flags.hpp"
#include "leanstore/utils/result.hpp"

#include <cassert>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

namespace leanstore::utils {

class JsonObj;

} // namespace leanstore::utils

namespace leanstore {

/// forward declarations
class BTreeGeneric;
class Config;
class BasicKV;
class TransactionKV;
class TreeRegistry;
class BufferManager;
class CRManager;
class MvccManager;

class LeanStore {
public:
  /// Opens a LeanStore instance with the provided options.
  /// NOTE: The option is created by LeanStore user, its ownership is transferred to the LeanStore
  ///       instance after the call, it will be destroyed when the LeanStore instance is destroyed.
  static Result<std::unique_ptr<LeanStore>> Open(lean_store_option* option);

  /// The storage option for leanstore
  const lean_store_option* store_option_;

  /// The file descriptor for pages
  int32_t page_fd_;

  /// The file descriptor for write-ahead log
  int32_t wal_fd_;

  /// The tree registry
  std::unique_ptr<TreeRegistry> tree_registry_;

  /// The Buffer manager
  std::unique_ptr<BufferManager> buffer_manager_;

  /// The concurrency control protocol used by the store.
  std::unique_ptr<leanstore::MvccManager> mvcc_mgr_;

  /// The concurrent resource manager
  /// NOTE: Ownerd by LeanStore instance, should be destroyed together with it
  CRManager* crmanager_;

  CoroScheduler* coro_scheduler_;

#ifdef DEBUG
  utils::DebugFlagsRegistry debug_flags_registry_;
#endif

  /// The LeanStore constructor
  /// NOTE: The option is created by LeanStore user, its ownership is transferred to the LeanStore
  ///       instance after the call, it will be destroyed when the LeanStore instance is destroyed.
  LeanStore(lean_store_option* option);

  /// The LeanStore destructor
  ~LeanStore();

  /// Create a BasicKV
  Result<BasicKV*> CreateBasicKv(const std::string& name,
                                 lean_btree_config config = lean_btree_config{
                                     .enable_wal_ = true, .use_bulk_insert_ = false});

  /// Get a registered BasicKV
  void GetBasicKV(const std::string& name, BasicKV** btree);

  /// Unregister a BasicKV
  void DropBasicKV(const std::string& name);

  /// Register a TransactionKV
  Result<TransactionKV*> CreateTransactionKV(const std::string& name,
                                             lean_btree_config config = lean_btree_config{
                                                 .enable_wal_ = true, .use_bulk_insert_ = false});

  /// Get a registered TransactionKV
  void GetTransactionKV(const std::string& name, TransactionKV** btree);

  /// Unregister a TransactionKV
  void DropTransactionKV(const std::string& name);

  std::unique_ptr<MvccManager>& GetMvccManager() {
    return mvcc_mgr_;
  }

  CoroScheduler* GetCoroScheduler() {
    return coro_scheduler_;
  }

  /// Execute a custom user function on a worker thread.
  void ExecSync(uint64_t worker_id, std::function<void()> fn);

  /// Execute a custom user function on a worker thread asynchronously.
  void ExecAsync(uint64_t worker_id, std::function<void()> fn);

  /// Waits for the worker to complete.
  void Wait(lean_wid_t worker_id);

  /// Waits for all Workers to complete.
  void WaitAll();

  void ParallelRange(uint64_t num_jobs,
                     std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler);

  std::string GetMetaFilePath() const {
    return std::string(store_option_->store_dir_) + "/db.meta.json";
  }

  std::string GetDbFilePath() const {
    return std::string(store_option_->store_dir_) + "/db.pages";
  }

  std::string GetWalFilePath() const {
    return std::string(store_option_->store_dir_) + "/db.wal";
  }

  std::string GetWalDir() const {
    return std::string(store_option_->store_dir_) + "/wal";
  }

private:
  void StartBackgroundThreads();
  void StopBackgroundThreads();

  /// serializeMeta serializes all the metadata about concurrent resources,
  /// buffer manager, btrees, and flags
  void SerializeMeta(bool all_pages_up_to_date);

  /// deserializeMeta deserializes all the metadata except for the flags.
  bool DeserializeMeta();

  /// Init database files, i.e. page and wal file descriptors.
  void InitDbFiles();
};

} // namespace leanstore
