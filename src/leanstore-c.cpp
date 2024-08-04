#include "leanstore/leanstore-c.h"

#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/Slice.hpp"
#include "leanstore/btree/BasicKV.hpp"
#include "leanstore/btree/core/BTreePessimisticSharedIterator.hpp"

#include <iostream>
#include <memory>
#include <utility>

#include <stdint.h>
#include <stdlib.h>

//------------------------------------------------------------------------------
// String API
//------------------------------------------------------------------------------

String CreateString(const char* data, uint64_t size) {
  String str;

  if (data == nullptr || size == 0) {
    str.mData = nullptr;
    str.mSize = 0;
    return str;
  }

  // allocate memory
  str.mData = static_cast<char*>(malloc(size));
  if (str.mData == nullptr) {
    str.mSize = 0;
    return str;
  }

  // copy data
  memcpy(str.mData, data, size);
  str.mSize = size;

  return str;
}

void DestroyString(String* str) {
  if (str != nullptr) {
    if (str->mData == nullptr) {
      return;
    }

    free(str->mData);
    str->mData = nullptr;
    str->mSize = 0;
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

static LeanStoreError ToLeanStoreError(leanstore::OpCode opCode) {
  switch (opCode) {
  case leanstore::OpCode::kOK:
    return kOk;
  case leanstore::OpCode::kNotFound:
    return kKeyNotFound;
  case leanstore::OpCode::kDuplicated:
    return kKeyDuplicated;
  case leanstore::OpCode::kAbortTx:
    return kTransactionConflict;
  case leanstore::OpCode::kSpaceNotEnough:
    return kSpaceNotEnough;
  case leanstore::OpCode::kOther:
  default:
    return kUnknownError;
  }
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

LeanStoreError BasicKvInsert(BasicKvHandle* handle, uint64_t workerId, StringSlice key,
                             StringSlice val) {
  leanstore::OpCode opCode{leanstore::OpCode::kOK};
  handle->mStore->ExecSync(workerId, [&]() {
    opCode = handle->mBtree->Insert(leanstore::Slice(key.mData, key.mSize),
                                    leanstore::Slice(val.mData, val.mSize));
  });
  return ToLeanStoreError(opCode);
}

LeanStoreError BasicKvLookup(BasicKvHandle* handle, uint64_t workerId, StringSlice key,
                             String* val) {
  leanstore::OpCode opCode{leanstore::OpCode::kOK};
  handle->mStore->ExecSync(workerId, [&]() {
    auto copyValueOut = [&](leanstore::Slice valSlice) {
      DestroyString(val); // release old content
      *val = CreateString(reinterpret_cast<const char*>(valSlice.data()), valSlice.size());
    };
    opCode = handle->mBtree->Lookup(leanstore::Slice(key.mData, key.mSize), copyValueOut);
  });
  return ToLeanStoreError(opCode);
}

LeanStoreError BasicKvRemove(BasicKvHandle* handle, uint64_t workerId, StringSlice key) {
  leanstore::OpCode opCode{leanstore::OpCode::kOK};
  handle->mStore->ExecSync(
      workerId, [&]() { opCode = handle->mBtree->Remove(leanstore::Slice(key.mData, key.mSize)); });
  return ToLeanStoreError(opCode);
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
  BasicKvIterHandle(leanstore::storage::btree::BTreePessimisticSharedIterator iter,
                    leanstore::LeanStore* store)
      : mIterator(std::move(iter)),
        mStore(store) {
  }

  //! The actual iterator
  leanstore::storage::btree::BTreePessimisticSharedIterator mIterator;

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

uint8_t BasicKvIterSeek(BasicKvIterHandle* handle, uint64_t workerId, StringSlice key) {
  uint8_t succeed{false};
  handle->mStore->ExecSync(workerId, [&]() {
    succeed = handle->mIterator.Seek(leanstore::Slice(key.mData, key.mSize));
  });
  return succeed;
}

uint8_t BasicKvIterSeekToFirstKey(BasicKvIterHandle* handle, uint64_t workerId) {
  StringSlice smallestKey{reinterpret_cast<char*>(0), 0};
  return BasicKvIterSeek(handle, workerId, smallestKey);
}

static void AbortOnNotSupported(const char* apiName) {
  std::cerr << apiName << " is unsupported" << std::endl;
  std::abort();
}

uint8_t BasicKvIterSeekToLastKey(BasicKvIterHandle* handle [[maybe_unused]],
                                 uint64_t workerId [[maybe_unused]]) {
  // TODO: abort on not supported
  AbortOnNotSupported("BasicKvIterSeekToLastKey");
  return false;
}

uint8_t BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t workerId) {
  uint8_t hasNext{false};
  handle->mStore->ExecSync(workerId, [&]() { hasNext = handle->mIterator.HasNext(); });
  return hasNext;
}

uint8_t BasicKvIterNext(BasicKvIterHandle* handle, uint64_t workerId) {
  uint8_t hasNext{false};
  handle->mStore->ExecSync(workerId, [&]() { hasNext = handle->mIterator.Next(); });
  return hasNext;
}

uint8_t BasicKvIterHasPrev(BasicKvIterHandle* handle [[maybe_unused]],
                           uint64_t workerId [[maybe_unused]]) {
  AbortOnNotSupported("BasicKvIterHasPrev");
  return false;
}

uint8_t BasicKvIterPrev(BasicKvIterHandle* handle [[maybe_unused]],
                        uint64_t workerId [[maybe_unused]]) {
  AbortOnNotSupported("BasicKvIterPrev");
  return false;
}

StringSlice BasicKvIterKey(BasicKvIterHandle* handle) {
  handle->mIterator.AssembleKey();
  auto keySlice = handle->mIterator.key();
  return {reinterpret_cast<const char*>(keySlice.data()), keySlice.size()};
}

StringSlice BasicKvIterVal(BasicKvIterHandle* handle) {
  auto valSlice = handle->mIterator.value();
  return {reinterpret_cast<const char*>(valSlice.data()), valSlice.size()};
}
