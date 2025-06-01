#include "leanstore-c/kv_basic.h"

#include "leanstore-c/leanstore.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/core/pessimistic_shared_iterator.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/slice.hpp"

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
  BasicKvIterHandle(leanstore::storage::btree::PessimisticSharedIterator iter,
                    leanstore::LeanStore* store)
      : iterator_(std::move(iter)),
        store_(store) {
  }

  /// The actual iterator
  leanstore::storage::btree::PessimisticSharedIterator iterator_;

  /// The leanstore
  leanstore::LeanStore* store_;
};

BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle) {
  BasicKvIterHandle* iterator_handle{nullptr};
  iterator_handle = new BasicKvIterHandle(handle->btree_->GetIterator(), handle->store_);
  return iterator_handle;
}

void DestroyBasicKvIter(BasicKvIterHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToFirst(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iterator_.SeekToFirst(); });
}

void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, uint64_t worker_id,
                                        StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iterator_.SeekToFirstGreaterEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t worker_id) {
  bool has_next{false};
  handle->store_->ExecSync(worker_id, [&]() { has_next = handle->iterator_.HasNext(); });
  return has_next;
}

void BasicKvIterNext(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iterator_.Next(); });
}

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToLast(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iterator_.SeekToLast(); });
}

void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, uint64_t worker_id,
                                    StringSlice key) {
  handle->store_->ExecSync(worker_id, [&]() {
    handle->iterator_.SeekToLastLessEqual(leanstore::Slice(key.data_, key.size_));
  });
}

bool BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t worker_id) {
  bool has_prev{false};
  handle->store_->ExecSync(worker_id, [&]() { has_prev = handle->iterator_.HasPrev(); });
  return has_prev;
}

void BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t worker_id) {
  handle->store_->ExecSync(worker_id, [&]() { handle->iterator_.Prev(); });
}

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

/// Whether the iterator is valid
bool BasicKvIterValid(BasicKvIterHandle* handle) {
  return handle->iterator_.Valid();
}

StringSlice BasicKvIterKey(BasicKvIterHandle* handle) {
  handle->iterator_.AssembleKey();
  auto key_slice = handle->iterator_.Key();
  return {reinterpret_cast<const char*>(key_slice.data()), key_slice.size()};
}

StringSlice BasicKvIterVal(BasicKvIterHandle* handle) {
  auto val_slice = handle->iterator_.Val();
  return {reinterpret_cast<const char*>(val_slice.data()), val_slice.size()};
}
