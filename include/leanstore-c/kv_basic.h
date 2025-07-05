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
/// @return the basic key-value store handle, or nullptr if the creation fails.
///         The handle should be destroyed by the caller with DestroyBasicKv()
BasicKvHandle* CreateBasicKv(LeanStoreHandle* handle, uint64_t worker_id, const char* btree_name);

/// Get the basic key-value handle for btree name
/// @return nullptr if not found
BasicKvHandle* GetBasicKv(LeanStoreHandle* handle, const char* btree_name);

/// Destroy the basic key-value store handle
void DestroyBasicKv(BasicKvHandle* handle);

/// Insert a key-value pair into a basic key-value store at workerId
/// @return true if the insert is successful, false otherwise
bool BasicKvInsert(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, StringSlice val);

/// Lookup a key in a basic key-value store at workerId
/// @return whether the value exists, The input val is untouched if the key is not found
bool BasicKvLookup(BasicKvHandle* handle, uint64_t worker_id, StringSlice key, OwnedString** val);

/// Remove a key in a basic key-value store at workerId
/// @return true if the key is found and removed, false otherwise
bool BasicKvRemove(BasicKvHandle* handle, uint64_t worker_id, StringSlice key);

/// Get the size of a basic key-value store at workerId
/// @return the number of entries in the basic key-value store
uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t worker_id);

//------------------------------------------------------------------------------
// Iterator API for BasicKv
//------------------------------------------------------------------------------

/// The BasicKvIterHandle is an opaque handle to an iterator for a basic
/// key-value store. The iterator should be destroyed by the caller after use.
typedef struct BasicKvIterHandle BasicKvIterHandle;
BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle, uint64_t worker_id);
void DestroyBasicKvIter(BasicKvIterHandle* handle);

/// The BasicKvIterMutHandle is an opaque handle to a mutable iterator for a
/// basic key-value store.  It allows modification of the key-value store while
/// iterating. The iterator should be destroyed by the caller after use.
typedef struct BasicKvIterMutHandle BasicKvIterMutHandle;
BasicKvIterMutHandle* CreateBasicKvIterMut(const BasicKvHandle* handle, uint64_t worker_id);
void DestroyBasicKvIterMut(BasicKvIterMutHandle* handle);

/// Convert a BasicKvIterHandle to a BasicKvIterMutHandle and vice versa. The
/// original handle is invalidated after the conversion, should be destroyed by
/// the caller with either DestroyBasicKvIter() or DestroyBasicKvIterMut().
BasicKvIterMutHandle* IntoBasicKvIterMut(BasicKvIterHandle* handle);
BasicKvIterHandle* IntoBasicKvIter(BasicKvIterMutHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

/// Seek to the first key of the basic key-value store at workerId
void BasicKvIterSeekToFirst(BasicKvIterHandle* handle);

/// Seek to the first key that >= the given key
void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, StringSlice key);

/// Whether the iterator has a next key in a basic key-value store at workerId
/// @return true if the next key exists, false otherwise
bool BasicKvIterHasNext(BasicKvIterHandle* handle);

/// Iterate to the next key in a basic key-value store at workerId
void BasicKvIterNext(BasicKvIterHandle* handle);

void BasicKvIterMutSeekToFirst(BasicKvIterMutHandle* handle);
void BasicKvIterMutSeekToFirstGreaterEqual(BasicKvIterMutHandle* handle, StringSlice key);
bool BasicKvIterMutHasNext(BasicKvIterMutHandle* handle);
void BasicKvIterMutNext(BasicKvIterMutHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

/// Seek to the last key of the basic key-value store at workerId
void BasicKvIterSeekToLast(BasicKvIterHandle* handle);

/// Seek to the last key that <= the given key
void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, StringSlice key);

/// Whether the iterator has a previous key in a basic key-value store at workerId
/// @return true if the previous key exists, false otherwise
bool BasicKvIterHasPrev(BasicKvIterHandle* handle);

/// Iterate to the previous key in a basic key-value store at workerId
void BasicKvIterPrev(BasicKvIterHandle* handle);

void BasicKvIterMutSeekToLast(BasicKvIterMutHandle* handle);
void BasicKvIterMutSeekToLastLessEqual(BasicKvIterMutHandle* handle, StringSlice key);
bool BasicKvIterMutHasPrev(BasicKvIterMutHandle* handle);
void BasicKvIterMutPrev(BasicKvIterMutHandle* handle);

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

bool BasicKvIterMutValid(BasicKvIterMutHandle* handle);
StringSlice BasicKvIterMutKey(BasicKvIterMutHandle* handle);
StringSlice BasicKvIterMutVal(BasicKvIterMutHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for mutation
//------------------------------------------------------------------------------

/// Remove the current key-value pair in a basic key-value store at workerId
void BasicKvIterMutRemove(BasicKvIterMutHandle* handle);

/// Insert a key-value pair in a basic key-value store at workerId
bool BasicKvIterMutInsert(BasicKvIterMutHandle* handle, StringSlice key, StringSlice val);

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_KV_BASIC_H