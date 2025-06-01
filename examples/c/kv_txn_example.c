#include "leanstore-c/kv_txn.h"
#include "leanstore-c/leanstore.h"
#include "leanstore-c/store_option.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  // Create LeanStoreHandle based on StoreOption
  // NOTE: StoreOption and LeanStoreHandle should be destroyed by the caller after use
  struct StoreOption* option = CreateStoreOption("/tmp/leanstore/examples/TxnKvExample");
  option->create_from_scratch_ = 1;
  option->worker_threads_ = 2;
  option->enable_bulk_insert_ = 0;
  option->enable_eager_gc_ = 1;
  LeanStoreHandle* store_handle = CreateLeanStore(option);

  // Create a transaction key-value store
  // NOTE: TxnKvHandle should be destroyed by the caller after use
  TxnKvHandle* kv_handle = CreateTxnKv(store_handle, 0, "test_txn_btree_1");
  if (kv_handle == NULL) {
    DestroyStoreOption(option);
    printf("create basic kv failed\n");
    return -1;
  }

  StringSlice key_slice;
  key_slice.data_ = "Hello";
  key_slice.size_ = strlen(key_slice.data_);

  StringSlice val_slice;
  val_slice.data_ = "World";
  val_slice.size_ = strlen(val_slice.data_);

  // Insert a key-value pair
  {
    LsStartTx(store_handle, 0);
    if (!TxnKvInsert(kv_handle, 0, key_slice, val_slice)) {
      printf("insert value failed, key=%.*s, val=%.*s\n", (int)key_slice.size_, key_slice.data_,
             (int)val_slice.size_, val_slice.data_);

      LsAbortTx(store_handle, 0);
      DestroyTxnKv(kv_handle);
      DestroyLeanStore(store_handle);
      DestroyStoreOption(option);
      return -1;
    }
    LsCommitTx(store_handle, 0);
    printf("Inserted key: %.*s, value: %.*s\n", (int)key_slice.size_, key_slice.data_,
           (int)val_slice.size_, val_slice.data_);
  }

  // Lookup a key
  {
    // NOTE: OwnedString should be destroyed by the caller after use
    OwnedString* val = CreateOwnedString(NULL, 0);
    LsStartTx(store_handle, 1);
    bool found = TxnKvLookup(kv_handle, 1, key_slice, &val);
    if (!found) {
      printf("lookup value failed, value may not exist, key=%.*s\n", (int)key_slice.size_,
             key_slice.data_);
      LsAbortTx(store_handle, 1);
      DestroyOwnedString(val);
      DestroyTxnKv(kv_handle);
      DestroyLeanStore(store_handle);
      DestroyStoreOption(option);
      return -1;
    }
    LsCommitTx(store_handle, 1);
    printf("Found value: %.*s\n", (int)val->size_, val->data_);
    DestroyOwnedString(val);
  }

  // Delete the key-value pair
  {
    LsStartTx(store_handle, 0);
    if (!TxnKvRemove(kv_handle, 0, key_slice)) {
      printf("remove value failed, key=%.*s\n", (int)key_slice.size_, key_slice.data_);
      LsAbortTx(store_handle, 0);
      DestroyTxnKv(kv_handle);
      DestroyLeanStore(store_handle);
      DestroyStoreOption(option);
      return -1;
    }
    LsCommitTx(store_handle, 0);
    printf("Removed key: %.*s\n", (int)key_slice.size_, key_slice.data_);
  }

  // get another handle
  {
    TxnKvHandle* kv_handle2 = GetTxnKv(store_handle, "test_txn_btree_1");
    if (kv_handle2 == NULL) {
      printf("get txn kv handle failed\n");
      DestroyTxnKv(kv_handle);
      DestroyLeanStore(store_handle);
      DestroyStoreOption(option);
      return -1;
    }
    printf("Got another TxnKvHandle for the same btree.\n");

    // Check the number of entries
    uint64_t num_entries = TxnKvNumEntries(kv_handle2, 0);
    printf("Number of entries in the TxnKv: %lu\n", num_entries);

    DestroyTxnKv(kv_handle2);
  }

  // clean up and exit
  DestroyTxnKv(kv_handle);
  DestroyLeanStore(store_handle);
  DestroyStoreOption(option);
  return 0;
}