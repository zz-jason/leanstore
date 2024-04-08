#pragma once

#include "btree/core/BTreeGeneric.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Store.hpp"
#include "leanstore/Units.hpp"
#include "utils/Error.hpp"
#include "utils/Result.hpp"

#include <expected>
#include <memory>

namespace leanstore {

class LeanStore;

class TxWorkerImpl : public TxWorker {
private:
  LeanStore* mStore = nullptr;
  WORKERID mWorkerId = 0;
  bool mAutoCommit = true;

public:
  TxWorkerImpl(LeanStore* store, WORKERID workerId)
      : mStore(store),
        mWorkerId(workerId) {
  }

  virtual ~TxWorkerImpl() = default;

  /// Start a transaction.
  virtual Result<void> StartTx(TxMode mode, IsolationLevel level) override {
    bool txAlreadyStarted = false;
    mStore->ExecSync(mWorkerId, [&]() {
      if (cr::Worker::My().mActiveTx.mState == cr::TxState::kStarted) {
        txAlreadyStarted = true;
        return;
      }
      cr::Worker::My().StartTx(mode, level);
    });

    if (txAlreadyStarted) {
      return std::unexpected<utils::Error>(
          utils::Error::General("Nested transactions are not supported yet"));
    }
    return {};
  }

  /// Commit a transaction.
  virtual Result<void> CommitTx() override {
    mStore->ExecSync(mWorkerId, [&]() { cr::Worker::My().CommitTx(); });
    return {};
  }

  /// Abort a transaction.
  virtual Result<void> AbortTx() override {
    mStore->ExecSync(mWorkerId, [&]() { cr::Worker::My().AbortTx(); });
    return {};
  }

  /// Create a table, should be executed inside a transaction.
  virtual Result<TableRef> CreateTable(const std::string& name) override {
    storage::btree::TransactionKV* table;
    auto config = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };
    bool inAlreadyStartedTx = false;
    mStore->ExecSync(mWorkerId, [&]() {
      auto res = mStore->CreateTransactionKV(name, config);
      if (res) {
        table = res.value();
      }
    });
    if (!inAlreadyStartedTx && !mAutoCommit) {
      return std::unexpected<utils::Error>(utils::Error::General(
          "CreateTable should be executed inside a transaction"));
    }
    if (table == nullptr) {
      return std::unexpected<utils::Error>(
          utils::Error::General("CreateTable failed, tableName=" + name));
    }
    return reinterpret_cast<TableRef*>(table);
  }

  /// Get a table, should be executed inside a transaction.
  virtual Result<TableRef> GetTable(const std::string& name
                                    [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Drop a table, should be executed inside a transaction.
  virtual Result<void> DropTable(const std::string& name
                                 [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Put a key-value pair into a table, should be executed inside a
  /// transaction.
  virtual Result<void> Put(TableRef table [[maybe_unused]],
                           Slice key [[maybe_unused]],
                           Slice value [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Update a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Update(TableRef table [[maybe_unused]],
                              Slice key [[maybe_unused]],
                              Slice value [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Delete a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Delete(TableRef table [[maybe_unused]],
                              Slice key [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Get a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual Result<void> Get(TableRef table [[maybe_unused]],
                           Slice key [[maybe_unused]],
                           std::string* value [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Create an iterator to scan a table, should be executed inside a
  /// transaction.
  virtual Result<std::unique_ptr<TableIterator>> NewTableIterator(
      TableRef table [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Get the total key-value pairs in a table, should be executed inside a
  /// transaction.
  virtual Result<uint64_t> GetTableSize(TableRef table
                                        [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }
};
} // namespace leanstore