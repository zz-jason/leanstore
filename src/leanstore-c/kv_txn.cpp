#include "leanstore-c/kv_txn.h"

#include "leanstore-c/leanstore.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/slice.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <utility>

#include <stdint.h>
#include <stdlib.h>

// -----------------------------------------------------------------------------
// Interfaces for Transaction
// -----------------------------------------------------------------------------

void LsStartTx(LeanStoreHandle* handle, uint64_t worker_id) {
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));
  store->ExecSync(worker_id, [&]() { leanstore::cr::WorkerContext::My().StartTx(); });
}

/// Commit a transaction in a leanstore worker
void LsCommitTx(LeanStoreHandle* handle, uint64_t worker_id) {
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));
  store->ExecSync(worker_id, [&]() { leanstore::cr::WorkerContext::My().CommitTx(); });
}

/// Abort a transaction in a leanstore worker
void LsAbortTx(LeanStoreHandle* handle, uint64_t worker_id) {
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));
  store->ExecSync(worker_id, [&]() { leanstore::cr::WorkerContext::My().AbortTx(); });
}

//------------------------------------------------------------------------------
// TxnKv API
//------------------------------------------------------------------------------

struct TxnKvHandle {
  leanstore::LeanStore* store_;
  leanstore::storage::btree::TransactionKV* btree_;
};

TxnKvHandle* CreateTxnKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name) {
  leanstore::storage::btree::TransactionKV* btree{nullptr};
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));

  store->ExecSync(worker_id, [&]() {
    auto res = store->CreateTransactionKV(btree_name);
    if (!res) {
      std::cerr << "create transaction kv failed: " << res.error().ToString() << std::endl;
      return;
    }
    btree = res.value();
  });

  if (btree == nullptr) {
    return nullptr;
  }

  TxnKvHandle* btree_handle = new TxnKvHandle();
  btree_handle->store_ = store;
  btree_handle->btree_ = btree;
  return btree_handle;
}

TxnKvHandle* GetTxnKv(LeanStoreHandle* handle, const char* btree_name) {
  leanstore::storage::btree::TransactionKV* btree{nullptr};
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));
  store->GetTransactionKV(btree_name, &btree);

  if (btree == nullptr) {
    return nullptr;
  }

  TxnKvHandle* btree_handle = new TxnKvHandle();
  btree_handle->store_ = store;
  btree_handle->btree_ = btree;
  return btree_handle;
}

void DestroyTxnKv(TxnKvHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

bool TxnKvInsert(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val) {
  bool succeed{false};
  handle->store_->ExecSync(worker_id, [&]() {
    auto op_code = handle->btree_->Insert(leanstore::Slice(key.data_, key.size_),
                                          leanstore::Slice(val.data_, val.size_));
    succeed = (op_code == leanstore::OpCode::kOK);
  });
  return succeed;
}

bool TxnKvLookup(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, OwnedString** val) {
  bool found{false};
  handle->store_->ExecSync(worker_id, [&]() {
    auto copy_value_out = [&](const leanstore::Slice& val_slice) {
      found = true;

      if ((**val).capacity_ < val_slice.size() + 1) {
        DestroyOwnedString(*val);
        *val = CreateOwnedString(reinterpret_cast<const char*>(val_slice.data()), val_slice.size());
        return;
      }

      // copy data to the buffer
      (**val).size_ = val_slice.size();
      memcpy((**val).data_, val_slice.data(), val_slice.size());
      (**val).data_[val_slice.size()] = '\0';
    };

    // lookup the key
    handle->btree_->Lookup(leanstore::Slice(key.data_, key.size_), std::move(copy_value_out));
  });

  return found;
}

bool TxnKvRemove(TxnKvHandle* handle, uint64_t worker_id, StringSlice key) {
  bool succeed{false};
  handle->store_->ExecSync(worker_id, [&]() {
    auto op_code = handle->btree_->Remove(leanstore::Slice(key.data_, key.size_));
    succeed = (op_code == leanstore::OpCode::kOK);
  });
  return succeed;
}

uint64_t TxnKvNumEntries(TxnKvHandle* handle, uint64_t worker_id) {
  uint64_t ret{0};
  handle->store_->ExecSync(worker_id, [&]() { ret = handle->btree_->CountEntries(); });
  return ret;
}