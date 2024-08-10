#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "leanstore/leanstore-c.h"

int main() {
  LeanStoreHandle* storeHandle =
      CreateLeanStore(1, "/tmp/leanstore/examples/BasicKvExample", 2, 0, 1);
  BasicKvHandle* kvHandle = CreateBasicKV(storeHandle, 0, "testTree1");

  StringSlice keySlice;
  keySlice.mData = "Hello";
  keySlice.mSize = strlen(keySlice.mData);

  StringSlice valSlice;
  valSlice.mData = "World";
  valSlice.mSize = strlen(valSlice.mData);

  {
    // insert a key value
    LeanStoreError error = BasicKvInsert(kvHandle, 0, keySlice, valSlice);
    if (error != kOk) {
      printf("insert value failed: %d\n", error);
      return error;
    }
  }

  // lookup a key
  {
    String valStr = CreateString(NULL, 0);
    LeanStoreError error = BasicKvLookup(kvHandle, 1, keySlice, &valStr);
    if (error != 0) {
      printf("lookup value failed: %d\n", error);
      return error;
    }
    printf("%.*s, %.*s\n", (int)keySlice.mSize, keySlice.mData, (int)valStr.mSize, valStr.mData);

    // cleanup the value string
    DestroyString(&valStr);
  }

  // insert more key-values
  {
    StringSlice keySlice2;
    keySlice2.mData = "Hello2";
    keySlice2.mSize = strlen(keySlice2.mData);

    StringSlice valSlice2;
    valSlice2.mData = "World2";
    valSlice2.mSize = strlen(valSlice2.mData);
    LeanStoreError error = BasicKvInsert(kvHandle, 0, keySlice2, valSlice2);
    if (error != kOk) {
      printf("insert value failed: %d\n", error);
      return error;
    }
  }

  // assending iteration
  {
    BasicKvIterHandle* iterHandle = CreateBasicKvIter(kvHandle);
    if (iterHandle == NULL) {
      printf("create iterator failed\n");
      return -1;
    }

    uint8_t succeed = BasicKvIterSeekForFirst(iterHandle, 0);
    while (succeed) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      printf("%.*s, %.*s\n", (int)key.mSize, key.mData, (int)val.mSize, val.mData);

      succeed = BasicKvIterNext(iterHandle, 0);
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

    uint8_t succeed = BasicKvIterSeekForLast(iterHandle, 0);
    while (succeed) {
      StringSlice key = BasicKvIterKey(iterHandle);
      StringSlice val = BasicKvIterVal(iterHandle);
      printf("%.*s, %.*s\n", (int)key.mSize, key.mData, (int)val.mSize, val.mData);

      succeed = BasicKvIterPrev(iterHandle, 0);
    }

    // destroy the iterator
    DestroyBasicKvIter(iterHandle);
  }

  // cleanup the basic kv handle
  DestroyBasicKV(kvHandle);

  // cleanup the store handle
  DestroyLeanStore(storeHandle);
}