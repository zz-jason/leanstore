#pragma once

#include "leanstore-c/store_option.h"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Units.hpp"
#include "leanstore/btree/TransactionKV.hpp"
#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/utils/Defer.hpp"
#include "leanstore/utils/Error.hpp"
#include "leanstore/utils/Log.hpp"
#include "leanstore/utils/Result.hpp"

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
  [[nodiscard]] virtual Result<TableRef*> CreateTable(const std::string& tblName,
                                                      bool implicitTx = false) = 0;

  [[nodiscard]] virtual Result<void> DropTable(const std::string& tblName,
                                               bool implicitTx = false) = 0;

  // DML operations
  [[nodiscard]] virtual Result<void> Put(TableRef* tbl, Slice key, Slice val,
                                         bool implicitTx = false) = 0;

  [[nodiscard]] virtual Result<uint64_t> Update(TableRef* tbl, Slice key, Slice val,
                                                bool implicitTx = false) = 0;

  [[nodiscard]] virtual Result<uint64_t> Get(TableRef* tbl, Slice key, std::string& val,
                                             bool implicitTx = false) = 0;

  [[nodiscard]] virtual Result<uint64_t> Delete(TableRef* tbl, Slice key,
                                                bool implicitTx = false) = 0;
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
    StoreOption* option = CreateStoreOption(storeDir.c_str());
    option->mWorkerThreads = sessionLimit;
    auto res = LeanStore::Open(option);
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
  [[nodiscard]] Result<TableRef*> CreateTable(const std::string& tblName,
                                              bool implicitTx = false) override;

  [[nodiscard]] Result<void> DropTable(const std::string& tblName,
                                       bool implicitTx = false) override;

  // DML operations
  [[nodiscard]] Result<void> Put(TableRef* tbl, Slice key, Slice val,
                                 bool implicitTx = false) override;

  [[nodiscard]] Result<uint64_t> Get(TableRef* tbl, Slice key, std::string& val,
                                     bool implicitTx = false) override;

  [[nodiscard]] Result<uint64_t> Update(TableRef* tbl, Slice key, Slice val,
                                        bool implicitTx = false) override;

  [[nodiscard]] Result<uint64_t> Delete(TableRef* tbl, Slice key, bool implicitTx = false) override;
};

class LeanStoreMVCCTableRef : public TableRef {
public:
  leanstore::storage::btree::TransactionKV* mTree;
};

//------------------------------------------------------------------------------
// StoreFactory
//------------------------------------------------------------------------------
inline std::unique_ptr<Store> StoreFactory::NewLeanStoreMVCC(const std::string& storeDir,
                                                             uint32_t sessionLimit) {
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
  LS_DCHECK(it != mSessions.end());
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
      mWorkerId, [&]() { cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel); });
}

inline void LeanStoreMVCCSession::CommitTx() {
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() { cr::WorkerContext::My().CommitTx(); });
}

inline void LeanStoreMVCCSession::AbortTx() {
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() { cr::WorkerContext::My().AbortTx(); });
}

// DDL operations
inline Result<TableRef*> LeanStoreMVCCSession::CreateTable(const std::string& tblName,
                                                           bool implicitTx [[maybe_unused]]) {
  storage::btree::TransactionKV* btree{nullptr};
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    auto res = mStore->mLeanStore->CreateTransactionKV(tblName);
    if (res) {
      btree = res.value();
    }
  });
  if (btree == nullptr) {
    return std::unexpected(utils::Error::General("failed"));
  }
  return reinterpret_cast<TableRef*>(btree);
}

inline Result<void> LeanStoreMVCCSession::DropTable(const std::string& tblName, bool implicitTx) {
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel);
    }
    mStore->mLeanStore->DropTransactionKV(tblName);
    if (implicitTx) {
      cr::WorkerContext::My().CommitTx();
    }
  });
  return {};
}

// DML operations
inline Result<void> LeanStoreMVCCSession::Put(TableRef* tbl, Slice key, Slice val,
                                              bool implicitTx) {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK) {
        cr::WorkerContext::My().CommitTx();
      } else {
        cr::WorkerContext::My().AbortTx();
      }
    });

    res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                        Slice((const uint8_t*)val.data(), val.size()));
  });
  if (res != OpCode::kOK) {
    return std::unexpected(utils::Error::General("Put failed: " + ToString(res)));
  }
  return {};
}

inline Result<uint64_t> LeanStoreMVCCSession::Get(TableRef* tbl, Slice key, std::string& val,
                                                  bool implicitTx) {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  auto copyValueOut = [&](Slice res) {
    val.resize(res.size());
    memcpy(val.data(), res.data(), res.size());
  };

  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::WorkerContext::My().CommitTx();
      } else {
        cr::WorkerContext::My().AbortTx();
      }
    });

    res = btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copyValueOut);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return std::unexpected(utils::Error::General("Get failed: " + ToString(res)));
}

inline Result<uint64_t> LeanStoreMVCCSession::Update(TableRef* tbl, Slice key, Slice val,
                                                     bool implicitTx) {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  auto updateCallBack = [&](MutableSlice toUpdate) {
    std::memcpy(toUpdate.Data(), val.data(), val.length());
  };
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::WorkerContext::My().CommitTx();
      } else {
        cr::WorkerContext::My().AbortTx();
      }
    });

    const uint64_t updateDescBufSize = UpdateDesc::Size(1);
    uint8_t updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = val.size();
    res = btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()), updateCallBack,
                               *updateDesc);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return std::unexpected(utils::Error::General("Update failed: " + ToString(res)));
}

inline Result<uint64_t> LeanStoreMVCCSession::Delete(TableRef* tbl, Slice key, bool implicitTx) {
  auto* btree = reinterpret_cast<storage::btree::TransactionKV*>(tbl);
  OpCode res;
  mStore->mLeanStore->ExecSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::WorkerContext::My().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::WorkerContext::My().CommitTx();
      } else {
        cr::WorkerContext::My().AbortTx();
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
  return std::unexpected(utils::Error::General("Delete failed: " + ToString(res)));
}

} // namespace test
} // namespace leanstore
