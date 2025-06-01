#include "leanstore-c/kv_basic.h"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/store_option.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  struct StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/BasicKvExample");
  option->create_from_scratch_ = 1;
  option->worker_threads_ = 2;
  option->enable_bulk_insert_ = 0;
  option->enable_eager_gc_ = 1;
  LeanStoreHandle* store_handle = CreateLeanStore(option);
  BasicKvHandle* kv_handle = CreateBasicKv(store_handle, 0, "testTree1");
  if (kv_handle == NULL) {
    DestroyStoreOption(option);
    printf("create basic kv failed\n");
    return -1;
  }

  // key-value pair 1
  StringSlice key_slice;
  key_slice.data_ = "Hello";
  key_slice.size_ = strlen(key_slice.data_);

  StringSlice val_slice;
  val_slice.data_ = "World";
  val_slice.size_ = strlen(val_slice.data_);

  // key-value pair 2
  StringSlice key_slice2;
  key_slice2.data_ = "Hello2";
  key_slice2.size_ = strlen(key_slice2.data_);

  StringSlice val_slice2;
  val_slice2.data_ = "World2";
  val_slice2.size_ = strlen(val_slice2.data_);

  {
    // insert a key value
    if (!BasicKvInsert(kv_handle, 0, key_slice, val_slice)) {
      printf("insert value failed, key=%.*s, val=%.*s\n", (int)key_slice.size_, key_slice.data_,
             (int)val_slice.size_, val_slice.data_);
      return -1;
    }
  }

  // lookup a key
  {
    OwnedString* val = CreateOwnedString(NULL, 0);
    bool found = BasicKvLookup(kv_handle, 1, key_slice, &val);
    if (!found) {
      printf("lookup value failed, value may not exist, key=%.*s\n", (int)key_slice.size_,
             key_slice.data_);
      DestroyOwnedString(val);
      return -1;
    }
    printf("%.*s, %.*s\n", (int)key_slice.size_, key_slice.data_, (int)val->size_, val->data_);
    DestroyOwnedString(val);
  }

  // insert more key-values
  {
    if (!BasicKvInsert(kv_handle, 0, key_slice2, val_slice2)) {
      printf("insert value failed, key=%.*s, val=%.*s\n", (int)key_slice2.size_, key_slice2.data_,
             (int)val_slice2.size_, val_slice2.data_);
      return -1;
    }
  }

  // assending iteration
  {
    BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle);
    if (iter_handle == NULL) {
      printf("create iterator failed\n");
      return -1;
    }

    for (BasicKvIterSeekToFirst(iter_handle, 0); BasicKvIterValid(iter_handle);
         BasicKvIterNext(iter_handle, 0)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      printf("%.*s, %.*s\n", (int)key.size_, key.data_, (int)val.size_, val.data_);
    }

    // destroy the iterator
    DestroyBasicKvIter(iter_handle);
  }

  // descending iteration
  {
    BasicKvIterHandle* iter_handle = CreateBasicKvIter(kv_handle);
    if (iter_handle == NULL) {
      printf("create iterator failed\n");
      return -1;
    }

    for (BasicKvIterSeekToLast(iter_handle, 0); BasicKvIterValid(iter_handle);
         BasicKvIterPrev(iter_handle, 0)) {
      StringSlice key = BasicKvIterKey(iter_handle);
      StringSlice val = BasicKvIterVal(iter_handle);
      printf("%.*s, %.*s\n", (int)key.size_, key.data_, (int)val.size_, val.data_);
    }

    // destroy the iterator
    DestroyBasicKvIter(iter_handle);
  }

  // remove key-values
  {
    if (!BasicKvRemove(kv_handle, 0, key_slice)) {
      printf("remove value failed, key=%.*s\n", (int)key_slice.size_, key_slice.data_);
      return -1;
    }

    if (!BasicKvRemove(kv_handle, 0, key_slice2)) {
      printf("remove value failed, key=%.*s\n", (int)key_slice2.size_, key_slice2.data_);
      return -1;
    }
  }

  // cleanup the basic kv handle
  DestroyBasicKv(kv_handle);

  // cleanup the store handle
  DestroyLeanStore(store_handle);
}