#pragma once

#include "leanstore/base/log.hpp"
#include "leanstore/base/result.hpp"
#include "leanstore/c/types.h"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/coro/coro_session.hpp"

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
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
class Table;
class TableRegistry;
struct TableDefinition;
class CheckpointProcessor;
class LeanSession;
class LeanBTree;
class LeanCursor;

class LeanStore {
  // Allow internal implementation to access private GetCoroScheduler()
  friend class CheckpointProcessor;

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

  /// Logical table registry
  std::unique_ptr<TableRegistry> table_registry_;

  /// The Buffer manager
  std::unique_ptr<BufferManager> buffer_manager_;

  /// The concurrency control protocol used by the store.
  std::unique_ptr<MvccManager> mvcc_mgr_;

  /// The concurrent resource manager
  std::unique_ptr<CRManager> crmanager_;

  /// The LeanStore constructor
  /// NOTE: The option is created by LeanStore user, its ownership is transferred to the LeanStore
  ///       instance after the call, it will be destroyed when the LeanStore instance is destroyed.
  explicit LeanStore(lean_store_option* option);

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

  /// Get the MVCC manager
  MvccManager& GetMvccManager() {
    LEAN_DCHECK(mvcc_mgr_ != nullptr, "MVCC manager is not initialized");
    return *mvcc_mgr_;
  }

  /// Create a logical table.
  Result<Table*> CreateTable(const TableDefinition& definition);

  /// Drop a logical table.
  Result<void> DropTable(const std::string& name);

  /// Lookup a logical table by name.
  Table* GetTable(const std::string& name);

  /// Register a logical table that already has its backing BTree.
  Result<Table*> RegisterTableWithExisting(const TableDefinition& definition);

  lean_lid_t AllocWalGsn();

  /// Execute a custom user function on a worker thread synchronously.
  void ExecSync(uint64_t worker_id, std::function<void()> fn);

  /// Execute a custom user function on a worker thread asynchronously.
  void ExecAsync(uint64_t worker_id, std::function<void()> fn);

  /// Waits for the worker to complete.
  void Wait(lean_wid_t worker_id);

  /// Waits for all Workers to complete.
  void WaitAll();

  void ParallelRange(uint64_t num_jobs,
                     std::function<void(uint64_t job_begin, uint64_t job_end)>&& job_handler);

  /// Reserve a worker session for submitting multiple tasks.
  /// Returns nullptr if no session is available.
  /// Must be released with ReleaseSession() when done.
  /// This is a high-performance alternative to ExecSync for coroutine-enabled builds.
  CoroSession* TryReserveSession(uint64_t worker_id);

  /// Reserve a worker session, blocking until one is available.
  /// Must be released with ReleaseSession() when done.
  CoroSession* ReserveSession(uint64_t worker_id);

  /// Release a previously reserved session back to the pool.
  void ReleaseSession(CoroSession* session);

  /// Submit a task on a reserved session and wait for completion.
  /// This is the high-performance alternative to ExecSync for coroutine-enabled builds.
  void SubmitAndWait(CoroSession* session, std::function<void()> task);

#ifdef LEAN_ENABLE_CORO
  /// Connect to the database and return a session for coroutine operations.
  /// This is a convenience wrapper around ReserveSession that returns a LeanSession object.
  auto Connect(uint64_t worker_id = 0) -> LeanSession;

  /// Try to connect to the database, returning an empty optional if no session is available.
  /// This is a convenience wrapper around TryReserveSession.
  auto TryConnect(uint64_t worker_id = 0) -> std::optional<LeanSession>;
#endif

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

  /// Internal accessor for coroutine scheduler (not exposed in public API)
  CoroScheduler& GetCoroScheduler() {
    LEAN_DCHECK(coro_scheduler_ != nullptr, "Coroutine scheduler is not initialized");
    return *coro_scheduler_;
  }

  /// The coroutine scheduler (implementation detail, hidden from public API)
  std::unique_ptr<CoroScheduler> coro_scheduler_ = nullptr;
};

} // namespace leanstore
