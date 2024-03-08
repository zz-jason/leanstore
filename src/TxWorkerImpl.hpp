#pragma once

#include "concurrency/Worker.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Store.hpp"
#include "leanstore/Units.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "utils/Error.hpp"

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
  virtual std::expected<void, utils::Error> StartTx(TxMode mode,
                                                    IsolationLevel level,
                                                    bool isReadOnly) override {
    bool txAlreadyStarted = false;
    mStore->ExecSync(mWorkerId, [&]() {
      if (cr::Worker::My().mActiveTx.mState == cr::TxState::kStarted) {
        txAlreadyStarted = true;
        return;
      }
      cr::Worker::My().StartTx(mode, level, isReadOnly);
    });

    if (txAlreadyStarted) {
      return std::unexpected<utils::Error>(
          utils::Error::General("Nested transactions are not supported yet"));
    }
    return {};
  }

  /// Commit a transaction.
  virtual std::expected<void, utils::Error> CommitTx() override {
    mStore->ExecSync(mWorkerId, [&]() { cr::Worker::My().CommitTx(); });
    return {};
  }

  /// Abort a transaction.
  virtual std::expected<void, utils::Error> AbortTx() override {
    mStore->ExecSync(mWorkerId, [&]() { cr::Worker::My().AbortTx(); });
    return {};
  }

  /// Create a table, should be executed inside a transaction.
  virtual std::expected<TableRef, utils::Error> CreateTable(
      const std::string& name) override {
    storage::btree::TransactionKV* table;
    auto config = leanstore::storage::btree::BTreeConfig{
        .mEnableWal = FLAGS_wal,
        .mUseBulkInsert = FLAGS_bulk_insert,
    };
    bool inAlreadyStartedTx = false;
    mStore->ExecSync(mWorkerId, [&]() {
      bool autoCommit = mAutoCommit;
      if (cr::Worker::My().mActiveTx.mState == cr::TxState::kStarted) {
        inAlreadyStartedTx = true;
        autoCommit = false;
      }
      if (!inAlreadyStartedTx && !mAutoCommit) {
        return;
      }

      if (autoCommit) {
        cr::Worker::My().StartTx(TxMode::kShortRunning,
                                 IsolationLevel::kSerializable, false);
      }
      mStore->CreateTransactionKV(name, config, &table);
      if (autoCommit) {
        cr::Worker::My().CommitTx();
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
  virtual std::expected<TableRef, utils::Error> GetTable(
      const std::string& name [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Drop a table, should be executed inside a transaction.
  virtual std::expected<void, utils::Error> DropTable(
      const std::string& name [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Put a key-value pair into a table, should be executed inside a
  /// transaction.
  virtual std::expected<void, utils::Error> Put(TableRef table [[maybe_unused]],
                                                Slice key [[maybe_unused]],
                                                Slice value
                                                [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Update a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual std::expected<void, utils::Error> Update(TableRef table
                                                   [[maybe_unused]],
                                                   Slice key [[maybe_unused]],
                                                   Slice value
                                                   [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Delete a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual std::expected<void, utils::Error> Delete(TableRef table
                                                   [[maybe_unused]],
                                                   Slice key
                                                   [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Get a key-value pair from a table, should be executed inside a
  /// transaction.
  virtual std::expected<void, utils::Error> Get(TableRef table [[maybe_unused]],
                                                Slice key [[maybe_unused]],
                                                std::string* value
                                                [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Create an iterator to scan a table, should be executed inside a
  /// transaction.
  virtual std::expected<std::unique_ptr<TableIterator>, utils::Error>
  NewTableIterator(TableRef table [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }

  /// Get the total key-value pairs in a table, should be executed inside a
  /// transaction.
  virtual std::expected<uint64_t, utils::Error> GetTableSize(
      TableRef table [[maybe_unused]]) override {
    return std::unexpected<utils::Error>(
        utils::Error::General("Not implemented yet"));
  }
};
} // namespace leanstore