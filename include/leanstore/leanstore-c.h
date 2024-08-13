#ifndef LEANSTORE_C_H
#define LEANSTORE_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//------------------------------------------------------------------------------
// String API
//------------------------------------------------------------------------------

//! String is a data structure that holds an owned bytes buffer
typedef struct String {
  //! The owned data pointer
  char* mData;

  //! The size of the data
  uint64_t mSize;
} String;

//! Creates a new string, copying the data from the given buffer to the new string
//! @param data the data buffer
//! @param size the size of the data buffer
//! @return the new string, which should be destroyed by the caller with DestroyString()
String* CreateString(const char* data, uint64_t size);

//! Destroys a string
void DestroyString(String* str);

//------------------------------------------------------------------------------
// StringSlice API
//------------------------------------------------------------------------------

//! StringSlice is a read-only data structure that holds a slice of a bytes buffer
typedef struct StringSlice {
  //! The read-only data pointer
  const char* mData;

  //! The size of the data
  uint64_t mSize;
} StringSlice;

//------------------------------------------------------------------------------
// LeanStore API
//------------------------------------------------------------------------------

typedef struct LeanStoreHandle LeanStoreHandle;

//! Create and init a leanstore instance
LeanStoreHandle* CreateLeanStore(int8_t createFromScratch, const char* storeDir,
                                 uint64_t workerThreads, int8_t enableBulkInsert,
                                 int8_t enableEagerGc);

//! Deinit and destroy a leanstore instance
void DestroyLeanStore(LeanStoreHandle* handle);

//------------------------------------------------------------------------------
// BasicKV API
//------------------------------------------------------------------------------

typedef struct BasicKvHandle BasicKvHandle;

//! Create a basic key-value store in a leanstore instance at workerId
//! @return the basic key-value store handle, or nullptr if the creation fails. The handle should be
//!         destroyed by the caller with DestroyBasicKV()
BasicKvHandle* CreateBasicKV(LeanStoreHandle* handle, uint64_t workerId, const char* btreeName);

//! Destroy the basic key-value store handle
void DestroyBasicKV(BasicKvHandle* handle);

//! Insert a key-value pair into a basic key-value store at workerId
//! @return true if the insert is successful, false otherwise
uint8_t BasicKvInsert(BasicKvHandle* handle, uint64_t workerId, StringSlice key, StringSlice val);

//! Lookup a key in a basic key-value store at workerId
//! NOTE: The caller should destroy the val after use via DestroyString()
//! @return the value if the key exists, nullptr otherwise
String* BasicKvLookup(BasicKvHandle* handle, uint64_t workerId, StringSlice key);

//! Remove a key in a basic key-value store at workerId
//! @return true if the key is found and removed, false otherwise
uint8_t BasicKvRemove(BasicKvHandle* handle, uint64_t workerId, StringSlice key);

//! Get the size of a basic key-value store at workerId
//! @return the number of entries in the basic key-value store
uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t workerId);

//------------------------------------------------------------------------------
// Iterator API for BasicKV
//------------------------------------------------------------------------------

//! The BasicKvIterHandle is an opaque handle to an iterator for a basic key-value store. The
//! iterator should be destroyed by the caller after use.
typedef struct BasicKvIterHandle BasicKvIterHandle;

//! Create an iterator for a basic key-value store at workerId
//! @return the iterator handle, or nullptr if the creation fails. The handle should be destroyed by
//!         the caller with DestroyBasicKvIter()
BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle);

//! Destroy an iterator for a basic key-value store at workerId
void DestroyBasicKvIter(BasicKvIterHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for ascending iteration
//------------------------------------------------------------------------------

//! Seek to the first key of the basic key-value store at workerId
void BasicKvIterSeekToFirst(BasicKvIterHandle* handle, uint64_t workerId);

//! Seek to the first key that >= the given key
void BasicKvIterSeekToFirstGreaterEqual(BasicKvIterHandle* handle, uint64_t workerId,
                                        StringSlice key);

//! Whether the iterator has a next key in a basic key-value store at workerId
//! @return true if the next key exists, false otherwise
uint8_t BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t workerId);

//! Iterate to the next key in a basic key-value store at workerId
void BasicKvIterNext(BasicKvIterHandle* handle, uint64_t workerId);

//------------------------------------------------------------------------------
// Interfaces for descending iteration
//------------------------------------------------------------------------------

//! Seek to the last key of the basic key-value store at workerId
void BasicKvIterSeekToLast(BasicKvIterHandle* handle, uint64_t workerId);

//! Seek to the last key that <= the given key
void BasicKvIterSeekToLastLessEqual(BasicKvIterHandle* handle, uint64_t workerId, StringSlice key);

//! Whether the iterator has a previous key in a basic key-value store at workerId
//! @return true if the previous key exists, false otherwise
uint8_t BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t workerId);

//! Iterate to the previous key in a basic key-value store at workerId
void BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t workerId);

//------------------------------------------------------------------------------
// Interfaces for accessing the current iterator position
//------------------------------------------------------------------------------

//! Whether the iterator is valid
uint8_t BasicKvIterValid(BasicKvIterHandle* handle);

//! Get the key of the current iterator position in a basic key-value store at workerId
//! @return the read-only key slice
StringSlice BasicKvIterKey(BasicKvIterHandle* handle);

//! Get the value of the current iterator position in a basic key-value store at workerId
//! @return the read-only value slice
StringSlice BasicKvIterVal(BasicKvIterHandle* handle);

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_H