#include "leanstore-c/kv_basic.h"

#include "btree/core/b_tree_wal_payload.hpp"
#include "leanstore-c/leanstore.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/btree_iter.hpp"
#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/slice.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <utility>

#include <stdint.h>
#include <stdlib.h>

//------------------------------------------------------------------------------
// BasicKv API
//------------------------------------------------------------------------------

struct BasicKvHandle {
  leanstore::LeanStore* store_;
  leanstore::storage::btree::BasicKV* btree_;
};

BasicKvHandle* CreateBasicKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name) {
  leanstore::storage::btree::BasicKV* btree{nullptr};
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));

  store->ExecSync(worker_id, [&]() {
    auto res = store->CreateBasicKv(btree_name);
    if (!res) {
      std::cerr << "create btree failed: " << res.error().ToString() << std::endl;
      return;
    }
    btree = res.value();
  });

  if (btree == nullptr) {
    return nullptr;
  }

  BasicKvHandle* btree_handle = new BasicKvHandle();
  btree_handle->store_ = store;
  btree_handle->btree_ = btree;
  return btree_handle;
}

BasicKvHandle* GetBasicKv(LeanStoreHandle* handle, const char* btree_name) {
  leanstore::storage::btree::BasicKV* btree{nullptr};
  auto* store = reinterpret_cast<leanstore::LeanStore*>(GetLeanStore(handle));
  store->GetBasicKV(btree_name, &btree);

  if (btree == nullptr) {
    return nullptr;
  }

  BasicKvHandle* btree_handle = new BasicKvHandle();
  btree_handle->store_ = store;
  btree_handle->btree_ = btree;
  return btree_handle;
}

void DestroyBasicKv(BasicKvHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

bool BasicKvInsert(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val) {
  bool succeed{false};
  handle->store_->ExecSync(worker_id, [&]() {
    auto op_code = handle->btree_->Insert(leanstore::Slice(key.data_, key.size_),
                                          leanstore::Slice(val.data_, val.size_));
    succeed = (op_code == leanstore::OpCode::kOK);
  });
  return succeed;
}

bool BasicKvLookup(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, OwnedString** val) {
  bool found = false;
  handle->store_->ExecSync(worker_id, [&]() {
    auto copy_value_out = [&](leanstore::Slice val_slice) {
      // set the found flag
      found = true;

      // create a new string if the value is out of the buffer size
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

bool BasicKvRemove(BasicKvHandle* handle, uint64_t worker_id, StringSlice key) {
  bool succeed{false};
  handle->store_->ExecSync(worker_id, [&]() {
    auto op_code = handle->btree_->Remove(leanstore::Slice(key.data_, key.size_));
    succeed = (op_code == leanstore::OpCode::kOK);
  });
  return succeed;
}

uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t worker_id) {
  uint64_t ret{0};
  handle->store_->ExecSync(worker_id, [&]() { ret = handle->btree_->CountEntries(); });
  return ret;
}

//------------------------------------------------------------------------------
// Iterator API for BasicKv
//------------------------------------------------------------------------------

struct BasicKvIterHandle {
  BasicKvIterHandle(std::unique_ptr<leanstore::storage::btree::BTreeIter> iter,
                    leanstore::LeanStore* store)
      : iter_(std::move(iter)),
        store_(store) {
  }

  /// The actual iterator
  std::unique_ptr<leanstore::storage::btree::BTreeIter> iter_;

  /// The leanstore
  leanstore::LeanStore* store_;
};

BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle) {
  BasicKvIterHandle* iter_handle{nullptr};
  iter_handle = new BasicKvIterHandle(handle->btree_->NewBTreeIter(), handle->store_);
  return iter_handle;
}

void DestroyBasicKvIter(BasicKvIterHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

struct BasicKvIterMutHandle {
  BasicKvIterMutHandle(std::unique_ptr<leanstore::storage::btree::BTreeIterMut> iter_mut,
                       leanstore::LeanStore* store)
      : iter_mut_(std::move(iter_mut)),
        store_(store) {
  }

  /// The actual mutable iterator
  std::unique_ptr<leanstore::storage::btree::BTreeIterMut> iter_mut_;

  /// The leanstore
  leanstore::LeanStore* store_;
};

BasicKvIterMutHandle* CreateBasicKvIterMut(const BasicKvHandle* handle) {
  BasicKvIterMutHandle* iter_mut_handle{nullptr};
  iter_mut_handle = new BasicKvIterMutHandle(handle->btree_->NewBTreeIterMut(), handle->store_);
  return iter_mut_handle;
}

void DestroyBasicKvIterMut(BasicKvIterMutHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

BasicKvIterMutHandle* IntoBasicKvIterMut(BasicKvIterHandle* handle) {
  assert(handle != nullptr && handle->iter_ != nullptr);
  auto* iter_mut_handle =
      new BasicKvIterMutHandle(handle->iter_->IntoBtreeIterMut(), handle->store_);
  return iter_mut_handle;
}

BasicKvIterHandle* IntoBasicKvIter(BasicKvIterMutHandle* handle) {
  assert(handle != nullptr && handle->iter_mut_ != nullptr);
  auto* iter_handle = new BasicKvIterHandle(handle->iter_mut_->IntoBtreeIter(), handle->store_);
  return iter_handle;
}

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToFirst(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_->SeekToFirst(); });
}

void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, uint64_t worker_id,
                                        StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iter_->SeekToFirstGreaterEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t worker_id) {
  bool has_next{false};
  handle->store_->ExecSync(worker_id, [&]() { has_next = handle->iter_->HasNext(); });
  return has_next;
}

void BasicKvIterNext(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_->Next(); });
}

void BasicKvIterMutSeekToFirst(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_mut_->SeekToFirst(); });
}

void BasicKvIterMutSeekToFirstGreaterEqual(BasicKvIterMutHandle* handle, uint64_t worker_id,
                                           StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iter_mut_->SeekToFirstGreaterEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterMutHasNext(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  bool has_next{false};
  handle->store_->ExecSync(worker_id, [&]() { has_next = handle->iter_mut_->HasNext(); });
  return has_next;
}

void BasicKvIterMutNext(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_mut_->Next(); });
}

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToLast(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_->SeekToLast(); });
}

void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, uint64_t worker_id,
                                    StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iter_->SeekToLastLessEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t worker_id) {
  bool has_prev{false};
  handle->store_->ExecSync(worker_id, [&]() { has_prev = handle->iter_->HasPrev(); });
  return has_prev;
}

void BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_->Prev(); });
}

void BasicKvIterMutSeekToLast(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_mut_->SeekToLast(); });
}

void BasicKvIterMutSeekToLastLessEqual(BasicKvIterMutHandle* handle, uint64_t worker_id,
                                       StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iter_mut_->SeekToLastLessEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterMutHasPrev(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  bool has_prev{false};
  handle->store_->ExecSync(worker_id, [&]() { has_prev = handle->iter_mut_->HasPrev(); });
  return has_prev;
}

void BasicKvIterMutPrev(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iter_mut_->Prev(); });
}

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

/// Whether the iterator is valid
bool BasicKvIterValid(BasicKvIterHandle* handle) {
  return handle->iter_->Valid();
}

StringSlice BasicKvIterKey(BasicKvIterHandle* handle) {
  handle->iter_->AssembleKey();
  auto key_slice = handle->iter_->Key();
  return {reinterpret_cast<const char*>(key_slice.data()), key_slice.size()};
}

StringSlice BasicKvIterVal(BasicKvIterHandle* handle) {
  auto val_slice = handle->iter_->Val();
  return {reinterpret_cast<const char*>(val_slice.data()), val_slice.size()};
}

bool BasicKvIterMutValid(BasicKvIterMutHandle* handle) {
  return handle->iter_mut_->Valid();
}

StringSlice BasicKvIterMutKey(BasicKvIterMutHandle* handle) {
  handle->iter_mut_->AssembleKey();
  auto key_slice = handle->iter_mut_->Key();
  return {reinterpret_cast<const char*>(key_slice.data()), key_slice.size()};
}

StringSlice BasicKvIterMutVal(BasicKvIterMutHandle* handle) {
  auto val_slice = handle->iter_mut_->Val();
  return {reinterpret_cast<const char*>(val_slice.data()), val_slice.size()};
}

//------------------------------------------------------------------------------
// Interfaces for mutation
//------------------------------------------------------------------------------
void BasicKvIterMutRemove(BasicKvIterMutHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() {
    assert(handle->iter_mut_->Valid() &&
           "Iterator is not valid, cannot remove current key-value pair");
    // wal
    if (handle->store_->store_option_->enable_wal_) {
      handle->iter_mut_->AssembleKey();
      auto key = handle->iter_mut_->Key();
      auto value = handle->iter_mut_->Val();
      auto wal_handler =
          handle->iter_mut_->guarded_leaf_.ReserveWALPayload<leanstore::storage::btree::WalRemove>(
              key.size() + value.size(), key, value);
      wal_handler.SubmitWal();
    }

    // remove
    handle->iter_mut_->RemoveCurrent();

    // merge if needed
    handle->iter_mut_->TryMergeIfNeeded();
  });
}

bool BasicKvIterMutInsert(BasicKvIterMutHandle* handle, uint64_t worker_id, StringSlice key,
                          StringSlice val) {
  auto succeed = true;
  handle->store_->ExecSync(worker_id, [&]() {
    // insert
    auto op_code = handle->iter_mut_->InsertKV(leanstore::Slice(key.data_, key.size_),
                                               leanstore::Slice(val.data_, val.size_));
    if (op_code != leanstore::OpCode::kOK) {
      succeed = false;
      return;
    }

    // wal
    if (handle->store_->store_option_->enable_wal_) {
      handle->iter_mut_->AssembleKey();
      auto key = handle->iter_mut_->Key();
      auto val = handle->iter_mut_->Val();
      auto wal_size = key.size() + val.size();
      handle->iter_mut_->guarded_leaf_.WriteWal<leanstore::storage::btree::WalInsert>(wal_size, key,
                                                                                      val);
    }
  });

  return succeed;
}
