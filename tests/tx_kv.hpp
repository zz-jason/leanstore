#pragma once

#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/common/types.h"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/defer.hpp"
#include "leanstore/utils/log.hpp"

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
  static std::unique_ptr<Store> NewLeanStoreMVCC(const std::string& store_dir,
                                                 uint32_t session_limit);

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
  virtual Session* GetSession(lean_wid_t session_id) = 0;
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
  virtual Result<TableRef*> CreateTable(const std::string& tbl_name, bool implicit_tx = false) = 0;

  virtual Result<void> DropTable(const std::string& tbl_name, bool implicit_tx = false) = 0;

  // DML operations
  virtual Result<void> Put(TableRef* tbl, Slice key, Slice val, bool implicit_tx = false) = 0;

  virtual Result<uint64_t> Update(TableRef* tbl, Slice key, Slice val,
                                  bool implicit_tx = false) = 0;

  virtual Result<uint64_t> Get(TableRef* tbl, Slice key, std::string& val,
                               bool implicit_tx = false) = 0;

  virtual Result<uint64_t> Delete(TableRef* tbl, Slice key, bool implicit_tx = false) = 0;
};

class TableRef {
public:
};

class LeanStoreMVCCSession;
class LeanStoreMVCC : public Store {
public:
  std::unique_ptr<leanstore::LeanStore> lean_store_ = nullptr;
  std::unordered_map<uint64_t, LeanStoreMVCCSession> sessions_;

public:
  LeanStoreMVCC(const std::string& store_dir, uint32_t session_limit) {
    lean_store_option* option = lean_store_option_create(store_dir.c_str());
    option->worker_threads_ = session_limit;
    auto res = LeanStore::Open(option);
    lean_store_ = std::move(res.value());
  }

  ~LeanStoreMVCC() override = default;

public:
  Session* GetSession(lean_wid_t session_id) override;
};

class LeanStoreMVCCSession : public Session {
private:
  lean_wid_t worker_id_;
  LeanStoreMVCC* store_;
  TxMode tx_mode_ = TxMode::kShortRunning;
  IsolationLevel isolation_level_ = IsolationLevel::kSnapshotIsolation;

public:
  LeanStoreMVCCSession(lean_wid_t session_id, LeanStoreMVCC* store)
      : worker_id_(session_id),
        store_(store),
        isolation_level_(IsolationLevel::kSnapshotIsolation) {
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
  Result<TableRef*> CreateTable(const std::string& tbl_name, bool implicit_tx = false) override;

  Result<void> DropTable(const std::string& tbl_name, bool implicit_tx = false) override;

  // DML operations
  Result<void> Put(TableRef* tbl, Slice key, Slice val, bool implicit_tx = false) override;

  Result<uint64_t> Get(TableRef* tbl, Slice key, std::string& val,
                       bool implicit_tx = false) override;

  Result<uint64_t> Update(TableRef* tbl, Slice key, Slice val, bool implicit_tx = false) override;

  Result<uint64_t> Delete(TableRef* tbl, Slice key, bool implicit_tx = false) override;
};

class LeanStoreMVCCTableRef : public TableRef {
public:
  leanstore::TransactionKV* tree_;
};

//------------------------------------------------------------------------------
// StoreFactory
//------------------------------------------------------------------------------
inline std::unique_ptr<Store> StoreFactory::NewLeanStoreMVCC(const std::string& store_dir,
                                                             uint32_t session_limit) {
  return std::make_unique<LeanStoreMVCC>(store_dir, session_limit);
}

//------------------------------------------------------------------------------
// LeanStoreMVCC
//------------------------------------------------------------------------------

inline Session* LeanStoreMVCC::GetSession(lean_wid_t session_id) {
  if (sessions_.find(session_id) == sessions_.end()) {
    sessions_.emplace(session_id, LeanStoreMVCCSession(session_id, this));
  }
  auto it = sessions_.find(session_id);
  LEAN_DCHECK(it != sessions_.end());
  return &it->second;
}

//------------------------------------------------------------------------------
// LeanStoreMVCC
//------------------------------------------------------------------------------
inline void LeanStoreMVCCSession::SetIsolationLevel(IsolationLevel level) {
  isolation_level_ = level;
}

inline void LeanStoreMVCCSession::SetTxMode(TxMode tx_mode) {
  tx_mode_ = tx_mode;
}

inline void LeanStoreMVCCSession::StartTx() {
  store_->lean_store_->ExecSync(worker_id_,
                                [&]() { CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_); });
}

inline void LeanStoreMVCCSession::CommitTx() {
  store_->lean_store_->ExecSync(worker_id_, [&]() { CoroEnv::CurTxMgr().CommitTx(); });
}

inline void LeanStoreMVCCSession::AbortTx() {
  store_->lean_store_->ExecSync(worker_id_, [&]() { CoroEnv::CurTxMgr().AbortTx(); });
}

// DDL operations
inline Result<TableRef*> LeanStoreMVCCSession::CreateTable(const std::string& tbl_name,
                                                           bool implicit_tx [[maybe_unused]]) {
  TransactionKV* btree{nullptr};
  store_->lean_store_->ExecSync(worker_id_, [&]() {
    auto res = store_->lean_store_->CreateTransactionKV(tbl_name);
    if (res) {
      btree = res.value();
    }
  });
  if (btree == nullptr) {
    return Error::General("failed to create table");
  }
  return reinterpret_cast<TableRef*>(btree);
}

inline Result<void> LeanStoreMVCCSession::DropTable(const std::string& tbl_name, bool implicit_tx) {
  store_->lean_store_->ExecSync(worker_id_, [&]() {
    if (implicit_tx) {
      CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_);
    }
    store_->lean_store_->DropTransactionKV(tbl_name);
    if (implicit_tx) {
      CoroEnv::CurTxMgr().CommitTx();
    }
  });
  return {};
}

// DML operations
inline Result<void> LeanStoreMVCCSession::Put(TableRef* tbl, Slice key, Slice val,
                                              bool implicit_tx) {
  auto* btree = reinterpret_cast<TransactionKV*>(tbl);
  OpCode res;
  store_->lean_store_->ExecSync(worker_id_, [&]() {
    if (implicit_tx) {
      CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_);
    }
    SCOPED_DEFER(if (implicit_tx) {
      if (res == OpCode::kOK) {
        CoroEnv::CurTxMgr().CommitTx();
      } else {
        CoroEnv::CurTxMgr().AbortTx();
      }
    });

    res = btree->Insert(Slice((const uint8_t*)key.data(), key.size()),
                        Slice((const uint8_t*)val.data(), val.size()));
  });
  if (res != OpCode::kOK) {
    return Error::General("Put failed: " + ToString(res));
  }
  return {};
}

inline Result<uint64_t> LeanStoreMVCCSession::Get(TableRef* tbl, Slice key, std::string& val,
                                                  bool implicit_tx) {
  auto* btree = reinterpret_cast<TransactionKV*>(tbl);
  OpCode res;
  auto copy_value_out = [&](Slice res) {
    val.resize(res.size());
    memcpy(val.data(), res.data(), res.size());
  };

  store_->lean_store_->ExecSync(worker_id_, [&]() {
    if (implicit_tx) {
      CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_);
    }
    SCOPED_DEFER(if (implicit_tx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        CoroEnv::CurTxMgr().CommitTx();
      } else {
        CoroEnv::CurTxMgr().AbortTx();
      }
    });

    res = btree->Lookup(Slice((const uint8_t*)key.data(), key.size()), copy_value_out);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return Error::General("Get failed: " + ToString(res));
}

inline Result<uint64_t> LeanStoreMVCCSession::Update(TableRef* tbl, Slice key, Slice val,
                                                     bool implicit_tx) {
  auto* btree = reinterpret_cast<TransactionKV*>(tbl);
  OpCode res;
  auto update_call_back = [&](MutableSlice to_update) {
    std::memcpy(to_update.Data(), val.data(), val.length());
  };
  store_->lean_store_->ExecSync(worker_id_, [&]() {
    if (implicit_tx) {
      CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_);
    }
    SCOPED_DEFER(if (implicit_tx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        CoroEnv::CurTxMgr().CommitTx();
      } else {
        CoroEnv::CurTxMgr().AbortTx();
      }
    });

    const uint64_t update_desc_buf_size = UpdateDesc::Size(1);
    uint8_t update_desc_buf[update_desc_buf_size];
    auto* update_desc = UpdateDesc::CreateFrom(update_desc_buf);
    update_desc->num_slots_ = 1;
    update_desc->update_slots_[0].offset_ = 0;
    update_desc->update_slots_[0].size_ = val.size();
    res = btree->UpdatePartial(Slice((const uint8_t*)key.data(), key.size()), update_call_back,
                               *update_desc);
  });
  if (res == OpCode::kOK) {
    return 1;
  }
  if (res == OpCode::kNotFound) {
    return 0;
  }
  return Error::General("Update failed: " + ToString(res));
}

inline Result<uint64_t> LeanStoreMVCCSession::Delete(TableRef* tbl, Slice key, bool implicit_tx) {
  auto* btree = reinterpret_cast<TransactionKV*>(tbl);
  OpCode res;
  store_->lean_store_->ExecSync(worker_id_, [&]() {
    if (implicit_tx) {
      CoroEnv::CurTxMgr().StartTx(tx_mode_, isolation_level_);
    }
    SCOPED_DEFER(if (implicit_tx) {
      if (res == OpCode::kOK || res == OpCode::kNotFound) {
        CoroEnv::CurTxMgr().CommitTx();
      } else {
        CoroEnv::CurTxMgr().AbortTx();
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
  return Error::General("Delete failed: " + ToString(res));
}

} // namespace test
} // namespace leanstore
