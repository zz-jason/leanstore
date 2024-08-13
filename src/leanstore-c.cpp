#include "leanstore/leanstore-c.h"

#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Slice.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/core/PessimisticSharedIterator.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
#include <utility>

#include <stdint.h>
#include <stdlib.h>

//------------------------------------------------------------------------------
// String API
//------------------------------------------------------------------------------

String* CreateString(const char* data, uint64_t size) {
  String* str = new String();

  if (data == nullptr || size == 0) {
    str->mData = nullptr;
    str->mSize = 0;
    return str;
  }

  // allocate memory, copy data
  str->mSize = size;
  str->mData = new char[size];
  memcpy(str->mData, data, size);

  return str;
}

void DestroyString(String* str) {
  if (str != nullptr) {
    if (str->mData != nullptr) {
      // release memory
      delete[] str->mData;
    }

    str->mData = nullptr;
    str->mSize = 0;

    // release the string object
    delete str;
  }
}

//------------------------------------------------------------------------------
// LeanStore API
//------------------------------------------------------------------------------

struct LeanStoreHandle {
  std::unique_ptr<leanstore::LeanStore> mStore;
};

LeanStoreHandle* CreateLeanStore(int8_t createFromScratch, const char* storeDir,
                                 uint64_t workerThreads, int8_t enableBulkInsert,
                                 int8_t enableEagerGc) {
  auto res = leanstore::LeanStore::Open(leanstore::StoreOption{
      .mCreateFromScratch = static_cast<bool>(createFromScratch),
      .mStoreDir = storeDir,
      .mWorkerThreads = workerThreads,
      .mEnableBulkInsert = static_cast<bool>(enableBulkInsert),
      .mEnableEagerGc = static_cast<bool>(enableEagerGc),
  });
  if (!res) {
    std::cerr << "open store failed: " << res.error().ToString() << std::endl;
    return nullptr;
  }
  LeanStoreHandle* handle = new LeanStoreHandle();
  handle->mStore = std::move(res.value());
  return handle;
}

void DestroyLeanStore(LeanStoreHandle* handle) {
  delete handle;
}

//------------------------------------------------------------------------------
// BasicKV API
//------------------------------------------------------------------------------

struct BasicKvHandle {
  leanstore::LeanStore* mStore;
  leanstore::storage::btree::BasicKV* mBtree;
};

BasicKvHandle* CreateBasicKV(LeanStoreHandle* handle, uint64_t workerId, const char* btreeName) {
  leanstore::storage::btree::BasicKV* btree{nullptr};
  handle->mStore->ExecSync(workerId, [&]() {
    auto res = handle->mStore->CreateBasicKV(btreeName);
    if (!res) {
      std::cerr << "create btree failed: " << res.error().ToString() << std::endl;
      return;
    }
    btree = res.value();
  });

  if (btree == nullptr) {
    return nullptr;
  }

  // placement new to construct BasicKvHandle
  BasicKvHandle* btreeHandle = new BasicKvHandle();
  btreeHandle->mStore = handle->mStore.get();
  btreeHandle->mBtree = btree;

  return btreeHandle;
}

void DestroyBasicKV(BasicKvHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

uint8_t BasicKvInsert(BasicKvHandle* handle, uint64_t workerId, StringSlice key, StringSlice val) {
  uint8_t succeed{false};
  handle->mStore->ExecSync(workerId, [&]() {
    auto opCode = handle->mBtree->Insert(leanstore::Slice(key.mData, key.mSize),
                                         leanstore::Slice(val.mData, val.mSize));
    succeed = (opCode == leanstore::OpCode::kOK);
  });
  return succeed;
}

String* BasicKvLookup(BasicKvHandle* handle, uint64_t workerId, StringSlice key) {
  String* val{nullptr};
  handle->mStore->ExecSync(workerId, [&]() {
    auto copyValueOut = [&](leanstore::Slice valSlice) {
      val = CreateString(reinterpret_cast<const char*>(valSlice.data()), valSlice.size());
    };
    handle->mBtree->Lookup(leanstore::Slice(key.mData, key.mSize), copyValueOut);
  });
  return val;
}

uint8_t BasicKvRemove(BasicKvHandle* handle, uint64_t workerId, StringSlice key) {
  uint8_t succeed{false};
  handle->mStore->ExecSync(workerId, [&]() {
    auto opCode = handle->mBtree->Remove(leanstore::Slice(key.mData, key.mSize));
    succeed = (opCode == leanstore::OpCode::kOK);
  });
  return succeed;
}

uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t workerId) {
  uint64_t ret{0};
  handle->mStore->ExecSync(workerId, [&]() { ret = handle->mBtree->CountEntries(); });
  return ret;
}

//------------------------------------------------------------------------------
// Iterator API for BasicKV
//------------------------------------------------------------------------------

struct BasicKvIterHandle {
  BasicKvIterHandle(leanstore::storage::btree::PessimisticSharedIterator iter,
                    leanstore::LeanStore* store)
      : mIterator(std::move(iter)),
        mStore(store) {
  }

  //! The actual iterator
  leanstore::storage::btree::PessimisticSharedIterator mIterator;

  //! The leanstore
  leanstore::LeanStore* mStore;
};

BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle) {
  BasicKvIterHandle* iteratorHandle{nullptr};
  iteratorHandle = new BasicKvIterHandle(handle->mBtree->GetIterator(), handle->mStore);
  return iteratorHandle;
}

void DestroyBasicKvIter(BasicKvIterHandle* handle) {
  if (handle != nullptr) {
    delete handle;
  }
}

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToFirst(BasicKvIterHandle* handle, uint64_t workerId) {
  handle->mStore->ExecSync(workerId, [&]() { handle->mIterator.SeekToFirst(); });
}

void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, uint64_t workerId,
                                        StringSlice key) {
  handle->mStore->ExecSync(workerId, [&]() {
    handle->mIterator.SeekToFirstGreaterEqual(leanstore::Slice(key.mData, key.mSize));
  });
}

uint8_t BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t workerId) {
  uint8_t hasNext{false};
  handle->mStore->ExecSync(workerId, [&]() { hasNext = handle->mIterator.HasNext(); });
  return hasNext;
}

void BasicKvIterNext(BasicKvIterHandle* handle, uint64_t workerId) {
  handle->mStore->ExecSync(workerId, [&]() { handle->mIterator.Next(); });
}

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

void BasicKvIterSeekToLast(BasicKvIterHandle* handle, uint64_t workerId) {
  handle->mStore->ExecSync(workerId, [&]() { handle->mIterator.SeekToLast(); });
}

void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, uint64_t workerId, StringSlice key) {
  handle->mStore->ExecSync(workerId, [&]() {
    handle->mIterator.SeekToLastLessEqual(leanstore::Slice(key.mData, key.mSize));
  });
}

uint8_t BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t workerId) {
  uint8_t hasPrev{false};
  handle->mStore->ExecSync(workerId, [&]() { hasPrev = handle->mIterator.HasPrev(); });
  return hasPrev;
}

void BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t workerId) {
  handle->mStore->ExecSync(workerId, [&]() { handle->mIterator.Prev(); });
}

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

//! Whether the iterator is valid
uint8_t BasicKvIterValid(BasicKvIterHandle* handle) {
  return handle->mIterator.Valid();
}

StringSlice BasicKvIterKey(BasicKvIterHandle* handle) {
  handle->mIterator.AssembleKey();
  auto keySlice = handle->mIterator.Key();
  return {reinterpret_cast<const char*>(keySlice.data()), keySlice.size()};
}

StringSlice BasicKvIterVal(BasicKvIterHandle* handle) {
  auto valSlice = handle->mIterator.Val();
  return {reinterpret_cast<const char*>(valSlice.data()), valSlice.size()};
}
