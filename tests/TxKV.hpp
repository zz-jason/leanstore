#pragma once

#include "btree/TransactionKV.hpp"
#include "btree/core/BTreeGeneric.hpp"
#include "concurrency/CRManager.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "utils/Defer.hpp"
#include "utils/Error.hpp"

#include <glog/logging.h>

#include <cstring>
#include <expected>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace leanstore {
namespace test {

class Store;
class StoreFactory {
public:
  static std::unique_ptr<Store> NewLeanStoreMVCC(const std::string& storeDir,
                                                 uint32_t sessionLimit);

  inline static Store* GetLeanStoreSingleVersion() {
    return nullptr;
  }

  inline static Store* GetRocksDBStore() {
    return nullptr;
  }
};

class Session;
class Store {
public:
  Store() = default;
  virtual ~Store() = default;

public:
  virtual Session* GetSession(WORKERID sessionId) = 0;
};

class TableRef;
class Session {
public:
  // Transaction operations
  virtual void SetIsolationLevel(IsolationLevel) = 0;
  virtual void SetTxMode(TxMode) = 0;
  virtual void StartTx() = 0;
  virtual void CommitTx() = 0;
  virtual void AbortTx() = 0;

  // DDL operations
  [[nodiscard]] virtual auto CreateTable(const std::string& tblName,
                                         bool implicitTx = false)
      -> std::expected<TableRef*, utils::Error> = 0;

  [[nodiscard]] virtual auto DropTable(const std::string& tblName,
                                       bool implicitTx = false)
      -> std::expected<void, utils::Error> = 0;

  // DML operations
  [[nodiscard]] virtual auto Put(TableRef* tbl, Slice key, Slice val,
                                 bool implicitTx = false)
      -> std::expected<void, utils::Error> = 0;

  [[nodiscard]] virtual auto Update(TableRef* tbl, Slice key, Slice val,
                                    bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> = 0;

  [[nodiscard]] virtual auto Get(TableRef* tbl, Slice key, std::string& val,
                                 bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> = 0;

  [[nodiscard]] virtual auto Delete(TableRef* tbl, Slice key,
                                    bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> = 0;
};

class TableRef {
public:
};

class LeanStoreMVCCSession;
class LeanStoreMVCC : public Store {
public:
  std::unique_ptr<leanstore::LeanStore> mLeanStore = nullptr;
  std::unordered_map<uint64_t, LeanStoreMVCCSession> mSessions;

public:
  LeanStoreMVCC(const std::string& storeDir, uint32_t sessionLimit) {
    FLAGS_init = true;
    FLAGS_logtostdout = true;
    FLAGS_data_dir = storeDir;
    FLAGS_worker_threads = sessionLimit;
    auto res = LeanStore::Open();
    mLeanStore = std::move(res.value());
  }

  ~LeanStoreMVCC() override = default;

public:
  Session* GetSession(WORKERID sessionId) override;
};

class LeanStoreMVCCSession : public Session {
private:
  WORKERID mWorkerId;
  LeanStoreMVCC* mStore;
  TxMode mTxMode = TxMode::kShortRunning;
  IsolationLevel mIsolationLevel = IsolationLevel::kSnapshotIsolation;

public:
  LeanStoreMVCCSession(WORKERID sessionId, LeanStoreMVCC* store)
      : mWorkerId(sessionId),
        mStore(store),
        mIsolationLevel(IsolationLevel::kSnapshotIsolation) {
  }
  ~LeanStoreMVCCSession() = default;

public:
  // Transaction operations
  void SetIsolationLevel(IsolationLevel) override;
  void SetTxMode(TxMode) override;
  void StartTx() override;
  void CommitTx() override;
  void AbortTx() override;

  // DDL operations
  [[nodiscard]] auto CreateTable(const std::string& tblName,
                                 bool implicitTx = false)
      -> std::expected<TableRef*, utils::Error> override;

  [[nodiscard]] auto DropTable(const std::string& tblName,
                               bool implicitTx = false)
      -> std::expected<void, utils::Error> override;

  // DML operations
  [[nodiscard]] auto Put(TableRef* tbl, Slice key, Slice val,
                         bool implicitTx = false)
      -> std::expected<void, utils::Error> override;

  [[nodiscard]] auto Get(TableRef* tbl, Slice key, std::string& val,
                         bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> override;

  [[nodiscard]] auto Update(TableRef* tbl, Slice key, Slice val,
                            bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> override;

  [[nodiscard]] auto Delete(TableRef* tbl, Slice key, bool implicitTx = false)
      -> std::expected<uint64_t, utils::Error> override;
};

class LeanStoreMVCCTableRef : public TableRef {
public:
  leanstore::storage::btree::TransactionKV* mTree;
};

//------------------------------------------------------------------------------
// StoreFactory
//------------------------------------------------------------------------------
inline std::unique_ptr<Store> StoreFactory::NewLeanStoreMVCC(
    const std::string& storeDir, uint32_t sessionLimit) {
  return std::make_unique<LeanStoreMVCC>(storeDir, sessionLimit);
}

//------------------------------------------------------------------------------
// LeanStoreMVCC
//------------------------------------------------------------------------------

inline Session* LeanStoreMVCC::GetSession(WORKERID sessionId) {
  if (mSessions.find(sessionId) == mSessions.end()) {
    mSessions.emplace(sessionId, LeanStoreMVCCSession(sessionId, this));
  }
  auto it = mSessions.find(sessionId);
  DCHECK(it != mSessions.end());
  return &it->second;
}

//------------------------------------------------------------------------------
// LeanStoreMVCC
//------------------------------------------------------------------------------
inline void LeanStoreMVCCSession::SetIsolationLevel(IsolationLevel level) {
  mIsolationLevel = level;
}

inline void LeanStoreMVCCSession::SetTxMode(TxMode txMode) {
  mTxMode = txMode;
}

inline void LeanStoreMVCCSession::StartTx() {
  mStore->mLeanStore->ExecSync(
      mWorkerId, [&]() { cr::Worker::My().StartTx(mTxMode, mIsolationLevel); });
}

inline void LeanStoreMVCCSession::CommitTx() {
  mStore->mLeanStore->ExecSync(mWorkerId,
                               [&]() { cr::Worker::My().CommitTx(); });
}

inline void LeanStoreMVCCSession::AbortTx() {
  mStore->mLeanStore->ExecSync(mWorkerId,
                               [&]() { cr::Worker::My().AbortTx(); });
}

// DDL operations
inline auto LeanStoreMVCCSession::CreateTable(const std::string& tblName,
                                              bool implicitTx)
    -> std::expected<TableRef*, utils::Error> {
  auto config = storage::btree::BTreeConfig{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  storage::btree::TransactionKV* btree;
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    mStore->mLeanStore->CreateTransactionKV(tblName, config, &btree);
    if (implicitTx) {
      cr::Worker::My().CommitTx();
    }
  });
  if (btree == nullptr) {
    return std::unexpected<utils::Error>(utils::Error::General("failed"));
  }
  return reinterpret_cast<TableRef*>(btree);
}

inline auto LeanStoreMVCCSession::DropTable(const std::string& tblName,
                                            bool implicitTx)
    -> std::expected<void, utils::Error> {
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    mStore->mLeanStore->DropTransactionKV(tblName);
    if (implicitTx) {
      cr::Worker::My().CommitTx();
    }
  });
  return {};
}

// DML operations
inline auto LeanStoreMVCCSession::Put(TableRef* tbl, Slice key, Slice val,
                                      bool implicitTx)
    -> std::expected<void, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK) {
        cr::Worker::My().CommitTx();
      } else {
        cr::Worker::My().AbortTx();
      }
    });

    res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                        Slice((const uint8_t*)val.data(), val.size()));
  });
  if (res != OpCode::kOK) {
    return std::unexpected<utils::Error>(
        utils::Error::General("Put failed: " + ToString(res)));
  }
  return {};
}

inline auto LeanStoreMVCCSession::Get(TableRef* tbl, Slice key,
                                      std::string& val, bool implicitTx)
    -> std::expected<uint64_t, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  auto copyValueOut = [&](Slice res) {
    val.resize(res.size());
    memcpy(val.data(), res.data(), res.size());
  };

  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::My().CommitTx();
      } else {
        cr::Worker::My().AbortTx();
      }
    });

    res = btree->Lookup(Slice((const uint8_t*)key.data(), key.size()),
                        copyValueOut);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return std::unexpected<utils::Error>(
      utils::Error::General("Get failed: " + ToString(res)));
}

inline auto LeanStoreMVCCSession::Update(TableRef* tbl, Slice key, Slice val,
                                         bool implicitTx)
    -> std::expected<uint64_t, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  auto updateCallBack = [&](MutableSlice toUpdate) {
    std::memcpy(toUpdate.Data(), val.data(), val.length());
  };
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::My().CommitTx();
      } else {
        cr::Worker::My().AbortTx();
      }
    });

    const uint64_t updateDescBufSize = UpdateDesc::Size(1);
    uint8_t updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = val.size();
    res = btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()),
                               updateCallBack, *updateDesc);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return std::unexpected<utils::Error>(
      utils::Error::General("Update failed: " + ToString(res)));
}

inline auto LeanStoreMVCCSession::Delete(TableRef* tbl, Slice key,
                                         bool implicitTx)
    -> std::expected<uint64_t, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::My().CommitTx();
      } else {
        cr::Worker::My().AbortTx();
      }
    });

    res = btree->Remove(Slice((const uint8_t*)key.data(), key.size()));
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return std::unexpected<utils::Error>(
      utils::Error::General("Delete failed: " + ToString(res)));
}

} // namespace test
} // namespace leanstore
