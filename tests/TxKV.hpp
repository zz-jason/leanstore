#pragma once

#include "KVInterface.hpp"
#include "LeanStore.hpp"
#include "concurrency-recovery/CRMG.hpp"
#include "concurrency-recovery/Transaction.hpp"
#include "shared-headers/Units.hpp"
#include "utils/Defer.hpp"
#include "utils/Error.hpp"

#include <glog/logging.h>

#include <cstring>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace leanstore {
namespace test {

class Store;
class StoreFactory {
public:
  static Store* GetLeanStoreMVCC(const std::string& storeDir, u32 sessionLimit);

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
      -> std::expected<u64, utils::Error> = 0;

  [[nodiscard]] virtual auto Get(TableRef* tbl, Slice key, std::string& val,
                                 bool implicitTx = false)
      -> std::expected<u64, utils::Error> = 0;

  [[nodiscard]] virtual auto Delete(TableRef* tbl, Slice key,
                                    bool implicitTx = false)
      -> std::expected<u64, utils::Error> = 0;
};

class TableRef {
public:
};

class LeanStoreMVCCSession;
class LeanStoreMVCC : public Store {
public:
  std::unique_ptr<leanstore::LeanStore> mLeanStore = nullptr;
  std::unordered_map<u64, LeanStoreMVCCSession> mSessions;

public:
  LeanStoreMVCC(const std::string& storeDir, u32 sessionLimit) {
    FLAGS_enable_print_btree_stats_on_exit = true;
    FLAGS_wal = true;
    FLAGS_bulk_insert = false;
    FLAGS_worker_threads = sessionLimit;
    FLAGS_recover = false;
    FLAGS_data_dir = storeDir;

    std::filesystem::path dirPath = FLAGS_data_dir;
    std::filesystem::remove_all(dirPath);
    std::filesystem::create_directories(dirPath);
    mLeanStore = std::make_unique<leanstore::LeanStore>();
  }

  ~LeanStoreMVCC() override = default;

public:
  Session* GetSession(WORKERID sessionId) override;
};

class LeanStoreMVCCSession : public Session {
private:
  WORKERID mWorkerId;
  LeanStoreMVCC* mStore;
  TxMode mTxMode = TxMode::kOLTP;
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
      -> std::expected<u64, utils::Error> override;

  [[nodiscard]] auto Update(TableRef* tbl, Slice key, Slice val,
                            bool implicitTx = false)
      -> std::expected<u64, utils::Error> override;

  [[nodiscard]] auto Delete(TableRef* tbl, Slice key, bool implicitTx = false)
      -> std::expected<u64, utils::Error> override;
};

class LeanStoreMVCCTableRef : public TableRef {
public:
  leanstore::storage::btree::BTreeVI* mTree;
};

//------------------------------------------------------------------------------
// StoreFactory
//------------------------------------------------------------------------------
inline Store* StoreFactory::GetLeanStoreMVCC(const std::string& storeDir,
                                             u32 sessionLimit) {
  static std::unique_ptr<LeanStoreMVCC> store = nullptr;
  static std::mutex storeMutex;
  if (store == nullptr) {
    std::unique_lock<std::mutex> guard(storeMutex);
    if (store == nullptr) {
      store = std::make_unique<LeanStoreMVCC>(storeDir, sessionLimit);
    }
  }
  return store.get();
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

inline void LeanStoreMVCCSession::StartTx() {
  cr::CRManager::sInstance->scheduleJobSync(
      mWorkerId, [&]() { cr::Worker::my().StartTx(mTxMode, mIsolationLevel); });
}

inline void LeanStoreMVCCSession::CommitTx() {
  cr::CRManager::sInstance->scheduleJobSync(
      mWorkerId, [&]() { cr::Worker::my().CommitTx(); });
}

inline void LeanStoreMVCCSession::AbortTx() {
  cr::CRManager::sInstance->scheduleJobSync(
      mWorkerId, [&]() { cr::Worker::my().AbortTx(); });
}

// DDL operations
inline auto LeanStoreMVCCSession::CreateTable(const std::string& tblName,
                                              bool implicitTx)
    -> std::expected<TableRef*, utils::Error> {
  auto config = storage::btree::BTreeGeneric::Config{
      .mEnableWal = FLAGS_wal,
      .mUseBulkInsert = FLAGS_bulk_insert,
  };

  storage::btree::BTreeVI* btree;
  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel);
    }
    mStore->mLeanStore->RegisterBTreeVI(tblName, config, &btree);
    if (implicitTx) {
      cr::Worker::my().CommitTx();
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
  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel);
    }
    mStore->mLeanStore->UnRegisterBTreeVI(tblName);
    if (implicitTx) {
      cr::Worker::my().CommitTx();
    }
  });
  return {};
}

// DML operations
inline auto LeanStoreMVCCSession::Put(TableRef* tbl, Slice key, Slice val,
                                      bool implicitTx)
    -> std::expected<void, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::BTreeVI*>(tbl);
  OpCode res;
  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK) {
        cr::Worker::my().CommitTx();
      } else {
        cr::Worker::my().AbortTx();
      }
    });

    res = btree->insert(Slice((const u8*)key.data(), key.size()),
                        Slice((const u8*)val.data(), val.size()));
  });
  if (res != OpCode::kOK) {
    return std::unexpected<utils::Error>(
        utils::Error::General("Put failed: " + ToString(res)));
  }
  return {};
}

inline auto LeanStoreMVCCSession::Get(TableRef* tbl, Slice key,
                                      std::string& val, bool implicitTx)
    -> std::expected<u64, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::BTreeVI*>(tbl);
  OpCode res;
  auto copyValueOut = [&](Slice res) {
    val.resize(res.size());
    memcpy(val.data(), res.data(), res.size());
  };

  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel, true);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::my().CommitTx();
      } else {
        cr::Worker::my().AbortTx();
      }
    });

    res = btree->Lookup(Slice((const u8*)key.data(), key.size()), copyValueOut);
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
    -> std::expected<u64, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::BTreeVI*>(tbl);
  OpCode res;
  auto updateCallBack = [&](MutableSlice toUpdate) {
    std::memcpy(toUpdate.Data(), val.data(), val.length());
  };
  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::my().CommitTx();
      } else {
        cr::Worker::my().AbortTx();
      }
    });

    const u64 updateDescBufSize = UpdateDesc::Size(1);
    u8 updateDescBuf[updateDescBufSize];
    auto* updateDesc = UpdateDesc::CreateFrom(updateDescBuf);
    updateDesc->mNumSlots = 1;
    updateDesc->mUpdateSlots[0].mOffset = 0;
    updateDesc->mUpdateSlots[0].mSize = val.size();
    res = btree->updateSameSizeInPlace(Slice((const u8*)key.data(), key.size()),
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
    -> std::expected<u64, utils::Error> {
  auto* btree = reinterpret_cast<storage::btree::BTreeVI*>(tbl);
  OpCode res;
  cr::CRManager::sInstance->scheduleJobSync(mWorkerId, [&]() {
    if (implicitTx) {
      cr::Worker::my().StartTx(mTxMode, mIsolationLevel);
    }
    SCOPED_DEFER(if (implicitTx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        cr::Worker::my().CommitTx();
      } else {
        cr::Worker::my().AbortTx();
      }
    });

    res = btree->remove(Slice((const u8*)key.data(), key.size()));
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

inline Slice ToSlice(const std::string& src) {
  return Slice((const u8*)src.data(), src.size());
}

} // namespace test
} // namespace leanstore
