#ifndef LEANSTORE_C_KV_TXN_H
#define LEANSTORE_C_KV_TXN_H

#include "leanstore-c/leanstore.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//------------------------------------------------------------------------------
// TxnKv API
//------------------------------------------------------------------------------

typedef struct TxnKvHandle TxnKvHandle;

/// Create a basic key-value store in a leanstore instance at workerId
/// @return the basic key-value store handle, or nullptr if the creation fails. The handle should be
///         destroyed by the caller with DestroyTxnKv()
TxnKvHandle* CreateTxnKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name);

/// Destroy the basic key-value store handle
void DestroyTxnKv(TxnKvHandle* handle);

/// Insert a key-value pair into a basic key-value store at workerId
/// @return true if the insert is successful, false otherwise
bool TxnKvInsert(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val);

/// Lookup a key in a basic key-value store at workerId
/// @return whether the value exists, The input val is untouched if the key is not found
bool TxnKvLookup(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, String** val);

/// Remove a key in a basic key-value store at workerId
/// @return true if the key is found and removed, false otherwise
bool TxnKvRemove(TxnKvHandle* handle, uint64_t worker_id, StringSlice key);

/// Get the size of a basic key-value store at workerId
/// @return the number of entries in the basic key-value store
uint64_t TxnKvNumEntries(TxnKvHandle* handle, uint64_t worker_id);

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_KV_TXN_H