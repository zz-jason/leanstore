#include "leanstore-c/StoreOption.h"
#include "leanstore-c/leanstore-c.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  struct StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/BasicKvExample");
  option->mCreateFromScratch = 1;
  option->mWorkerThreads = 2;
  option->mEnableBulkInsert = 0;
  option->mEnableEagerGc = 1;
  LeanStoreHandle* storeHandle = CreateLeanStore(option);
  BasicKvHandle* kvHandle = CreateBasicKV(storeHandle, 0, "testTree1");
  if (kvHandle == NULL) {
    DestroyStoreOption(option);
    printf("create basic kv failed\n");
    return -1;
  }

  // key-value pair 1
  StringSlice keySlice;
  keySlice.mData = "Hello";
  keySlice.mSize = strlen(keySlice.mData);

  StringSlice valSlice;
  valSlice.mData = "World";
  valSlice.mSize = strlen(valSlice.mData);

  // key-value pair 2
  StringSlice keySlice2;
  keySlice2.mData = "Hello2";
  keySlice2.mSize = strlen(keySlice2.mData);

  StringSlice valSlice2;
  valSlice2.mData = "World2";
  valSlice2.mSize = strlen(valSlice2.mData);

  {
    // insert a key value
    if (!BasicKvInsert(kvHandle, 0, keySlice, valSlice)) {
      printf("insert value failed, key=%.*s, val=%.*s\n", (int)keySlice.mSize, keySlice.mData,
             (int)valSlice.mSize, valSlice.mData);
      return -1;
    }
  }

  // lookup a key
  {
    String* valStr = BasicKvLookup(kvHandle, 1, keySlice);
    if (valStr == NULL) {
      printf("lookup value failed, value may not exist, key=%.*s\n", (int)keySlice.mSize,
             keySlice.mData);
      return -1;
    }
    printf("%.*s, %.*s\n", (int)keySlice.mSize, keySlice.mData, (int)valStr->mSize, valStr->mData);

    // cleanup the value string
    DestroyString(valStr);
  }

  // insert more key-values
  {
    if (!BasicKvInsert(kvHandle, 0, keySlice2, valSlice2)) {
      printf("insert value failed, key=%.*s, val=%.*s\n", (int)keySlice2.mSize, keySlice2.mData,
             (int)valSlice2.mSize, valSlice2.mData);
      return -1;
    }
  }

  // assending iteration
  {
    BasicKvIterHandle* iterHandle = CreateBasicKvIter(kvHandle);
    if (iterHandle == NULL) {
      printf("create iterator failed\n");
      return -1;
    }

    for (BasicKvIterSeekToFirst(iterHandle, 0); BasicKvIterValid(iterHandle);
         BasicKvIterNext(iterHandle, 0)) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      printf("%.*s, %.*s\n", (int)key.mSize, key.mData, (int)val.mSize, val.mData);
    }

    // destroy the iterator
    DestroyBasicKvIter(iterHandle);
  }

  // descending iteration
  {
    BasicKvIterHandle* iterHandle = CreateBasicKvIter(kvHandle);
    if (iterHandle == NULL) {
      printf("create iterator failed\n");
      return -1;
    }

    for (BasicKvIterSeekToLast(iterHandle, 0); BasicKvIterValid(iterHandle);
         BasicKvIterPrev(iterHandle, 0)) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      printf("%.*s, %.*s\n", (int)key.mSize, key.mData, (int)val.mSize, val.mData);
    }

    // destroy the iterator
    DestroyBasicKvIter(iterHandle);
  }

  // remove key-values
  {
    if (!BasicKvRemove(kvHandle, 0, keySlice)) {
      printf("remove value failed, key=%.*s\n", (int)keySlice.mSize, keySlice.mData);
      return -1;
    }

    if (!BasicKvRemove(kvHandle, 0, keySlice2)) {
      printf("remove value failed, key=%.*s\n", (int)keySlice2.mSize, keySlice2.mData);
      return -1;
    }
  }

  // cleanup the basic kv handle
  DestroyBasicKV(kvHandle);

  // cleanup the store handle
  DestroyLeanStore(storeHandle);
}