#ifndef LEANSTORE_C_KV_TXN_H
#define LEANSTORE_C_KV_TXN_H

#include "leanstore-c/leanstore.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// -----------------------------------------------------------------------------
// Interfaces for Transaction
// -----------------------------------------------------------------------------

/// Start a transaction in a leanstore worker
/// Operations on TxnKvHandle should be inside a transaction
void LsStartTx(LeanStoreHandle* handle, uint64_t worker_id);

/// Commit a transaction in a leanstore worker
void LsCommitTx(LeanStoreHandle* handle, uint64_t worker_id);

/// Abort a transaction in a leanstore worker
void LsAbortTx(LeanStoreHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// TxnKv API
//------------------------------------------------------------------------------

/// The TxnKvHandle is an opaque handle to a transaction key-value store. The
/// handle should be destroyed by the caller after use.
typedef struct TxnKvHandle TxnKvHandle;

/// Create a transaction key-value store in a leanstore instance at workerId
/// @return the transaction key-value store handle, or nullptr if the creation
///         fails. The handle should be destroyed by the caller with DestroyTxnKv()
TxnKvHandle* CreateTxnKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name);

/// Get the transaction key-value handle for btree name
/// @return nullptr if not found
TxnKvHandle* GetTxnKv(LeanStoreHandle* handle, const char* btree_name);

/// Destroy the transaction key-value store handle
void DestroyTxnKv(TxnKvHandle* handle);

/// Insert a key-value pair into a transaction key-value store at workerId
/// @return true if the insert is successful, false otherwise
bool TxnKvInsert(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val);

/// Lookup a key in a transaction key-value store at workerId
/// @return whether the value exists, The input val is untouched if the key is not found
bool TxnKvLookup(TxnKvHandle* handle, uint64_t worker_id, StringSlice key, OwnedString** val);

/// Remove a key in a transaction key-value store at workerId
/// @return true if the key is found and removed, false otherwise
bool TxnKvRemove(TxnKvHandle* handle, uint64_t worker_id, StringSlice key);

/// Get the size of a transaction key-value store at workerId
/// @return the number of entries in the transaction key-value store
uint64_t TxnKvNumEntries(TxnKvHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Iterator API for TxnKv
//------------------------------------------------------------------------------

/// The TxnKvIterHandle is an opaque handle to an iterator for a transaction key-value store. The
/// iterator should be destroyed by the caller after use.
typedef struct TxnKvIterHandle TxnKvIterHandle;

/// Create an iterator for a transaction key-value store at workerId
/// @return the iterator handle, or nullptr if the creation fails. The handle should be destroyed by
///         the caller with DestroyTxnKvIter()
TxnKvIterHandle* CreateTxnKvIter(const TxnKvHandle* handle);

/// Destroy an iterator for a transaction key-value store at workerId
void DestroyTxnKvIter(TxnKvIterHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

/// Seek to the first key of the transaction key-value store at workerId
void TxnKvIterSeekToFirst(TxnKvIterHandle* handle, uint64_t worker_id);

/// Seek to the first key that >= the given key
void TxnKvIterSeekToFirstGreaterEqual(TxnKvIterHandle* handle, uint64_t worker_id, StringSlice key);

/// Whether the iterator has a next key in a transaction key-value store at workerId
/// @return true if the next key exists, false otherwise
bool TxnKvIterHasNext(TxnKvIterHandle* handle, uint64_t worker_id);

/// Iterate to the next key in a transaction key-value store at workerId
void TxnKvIterNext(TxnKvIterHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

/// Seek to the last key of the transaction key-value store at workerId
void TxnKvIterSeekToLast(TxnKvIterHandle* handle, uint64_t worker_id);

/// Seek to the last key that <= the given key
void TxnKvIterSeekToLastLessEqual(TxnKvIterHandle* handle, uint64_t worker_id, StringSlice key);

/// Whether the iterator has a previous key in a transaction key-value store at workerId
/// @return true if the previous key exists, false otherwise
bool TxnKvIterHasPrev(TxnKvIterHandle* handle, uint64_t worker_id);

/// Iterate to the previous key in a transaction key-value store at workerId
void TxnKvIterPrev(TxnKvIterHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

/// Whether the iterator is valid
bool TxnKvIterValid(TxnKvIterHandle* handle);

/// Get the key of the current iterator position in a transaction key-value store at workerId
/// @return the read-only key slice
StringSlice TxnKvIterKey(TxnKvIterHandle* handle);

/// Get the value of the current iterator position in a transaction key-value store at workerId
/// @return the read-only value slice
StringSlice TxnKvIterVal(TxnKvIterHandle* handle);

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_KV_TXN_H