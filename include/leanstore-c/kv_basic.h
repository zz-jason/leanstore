#ifndef LEANSTORE_C_KV_BASIC_H
#define LEANSTORE_C_KV_BASIC_H

#include "leanstore-c/leanstore.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//------------------------------------------------------------------------------
// BasicKv API
//------------------------------------------------------------------------------

typedef struct BasicKvHandle BasicKvHandle;

/// Create a basic key-value store in a leanstore instance at workerId
/// @return the basic key-value store handle, or nullptr if the creation fails. The handle should be
///         destroyed by the caller with DestroyBasicKv()
BasicKvHandle* CreateBasicKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name);

/// Destroy the basic key-value store handle
void DestroyBasicKv(BasicKvHandle* handle);

/// Insert a key-value pair into a basic key-value store at workerId
/// @return true if the insert is successful, false otherwise
bool BasicKvInsert(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val);

/// Lookup a key in a basic key-value store at workerId
/// @return whether the value exists, The input val is untouched if the key is not found
bool BasicKvLookup(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, String** val);

/// Remove a key in a basic key-value store at workerId
/// @return true if the key is found and removed, false otherwise
bool BasicKvRemove(BasicKvHandle* handle, uint64_t worker_id, StringSlice key);

/// Get the size of a basic key-value store at workerId
/// @return the number of entries in the basic key-value store
uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Iterator API for BasicKv
//------------------------------------------------------------------------------

/// The BasicKvIterHandle is an opaque handle to an iterator for a basic key-value store. The
/// iterator should be destroyed by the caller after use.
typedef struct BasicKvIterHandle BasicKvIterHandle;

/// Create an iterator for a basic key-value store at workerId
/// @return the iterator handle, or nullptr if the creation fails. The handle should be destroyed by
///         the caller with DestroyBasicKvIter()
BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle);

/// Destroy an iterator for a basic key-value store at workerId
void DestroyBasicKvIter(BasicKvIterHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

/// Seek to the first key of the basic key-value store at workerId
void BasicKvIterSeekToFirst(BasicKvIterHandle* handle, uint64_t worker_id);

/// Seek to the first key that >= the given key
void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, uint64_t worker_id,
                                        StringSlice key);

/// Whether the iterator has a next key in a basic key-value store at workerId
/// @return true if the next key exists, false otherwise
bool BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t worker_id);

/// Iterate to the next key in a basic key-value store at workerId
void BasicKvIterNext(BasicKvIterHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

/// Seek to the last key of the basic key-value store at workerId
void BasicKvIterSeekToLast(BasicKvIterHandle* handle, uint64_t worker_id);

/// Seek to the last key that <= the given key
void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, uint64_t worker_id, StringSlice key);

/// Whether the iterator has a previous key in a basic key-value store at workerId
/// @return true if the previous key exists, false otherwise
bool BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t worker_id);

/// Iterate to the previous key in a basic key-value store at workerId
void BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

/// Whether the iterator is valid
bool BasicKvIterValid(BasicKvIterHandle* handle);

/// Get the key of the current iterator position in a basic key-value store at workerId
/// @return the read-only key slice
StringSlice BasicKvIterKey(BasicKvIterHandle* handle);

/// Get the value of the current iterator position in a basic key-value store at workerId
/// @return the read-only value slice
StringSlice BasicKvIterVal(BasicKvIterHandle* handle);

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_KV_BASIC_H