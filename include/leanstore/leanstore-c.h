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

//! Creates a new string with the given bytes buffer
String CreateString(const char* data, uint64_t size);

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

//! LeanStore error codes
typedef enum LeanStoreError {
  //! No error
  kOk = 0,

  //! Unknown error
  kUnknownError = 1001,

  //! Key already exists
  kKeyDuplicated = 2000,

  //! Key not found
  kKeyNotFound = 2001,

  kTransactionConflict = 3000,

  //! Space not enough
  kSpaceNotEnough = 4000,
} LeanStoreError;

//------------------------------------------------------------------------------
// BasicKV API
//------------------------------------------------------------------------------

typedef struct BasicKvHandle BasicKvHandle;

//! Create a basic key-value store in a leanstore instance at workerId
//! The handle should be destroyed by the caller
BasicKvHandle* CreateBasicKV(LeanStoreHandle* handle, uint64_t workerId, const char* btreeName);

//! Destroy the basic key-value store handle
void DestroyBasicKV(BasicKvHandle* handle);

//! Insert a key-value pair into a basic key-value store at workerId
LeanStoreError BasicKvInsert(BasicKvHandle* handle, uint64_t workerId, StringSlice key,
                             StringSlice val);

//! Lookup a key in a basic key-value store at workerId
//! NOTE:
//!   1. The old content hold by val will be released and overwritten by the new content
//!   2. The caller should destroy the val after use
LeanStoreError BasicKvLookup(BasicKvHandle* handle, uint64_t workerId, StringSlice key,
                             String* val);

//! Remove a key in a basic key-value store at workerId
LeanStoreError BasicKvRemove(BasicKvHandle* handle, uint64_t workerId, StringSlice key);

//! Get the size of a basic key-value store at workerId
uint64_t BasicKvNumEntries(BasicKvHandle* handle, uint64_t workerId);

//------------------------------------------------------------------------------
// Iterator API for BasicKV
//------------------------------------------------------------------------------

//! The BasicKvIterHandle is an opaque handle to an iterator for a basic key-value store. The
//! iterator should be destroyed by the caller after use.
typedef struct BasicKvIterHandle BasicKvIterHandle;

//! Create an iterator for a basic key-value store at workerId
BasicKvIterHandle* CreateBasicKvIter(const BasicKvHandle* handle);

//! Destroy an iterator for a basic key-value store at workerId
void DestroyBasicKvIter(BasicKvIterHandle* handle);

//! Seek for the first key that is not less than the given key
//! @return true if the key is found, false otherwise
uint8_t BasicKvIterSeek(BasicKvIterHandle* handle, uint64_t workerId, StringSlice key);

//! Seek to the first key of the basic key-value store at workerId
//! @return true if the begin key exists, false otherwise
uint8_t BasicKvIterSeekToFirstKey(BasicKvIterHandle* handle, uint64_t workerId);

//! Seek to the last key of the basic key-value store at workerId
//! @return true if the end key exists, false otherwise
uint8_t BasicKvIterSeekToLastKey(BasicKvIterHandle* handle, uint64_t workerId);

//! Whether the iterator has a next key in a basic key-value store at workerId
//! @return true if the next key exists, false otherwise
uint8_t BasicKvIterHasNext(BasicKvIterHandle* handle, uint64_t workerId);

//! Seek to the next key in a basic key-value store at workerId
//! @return true if the next key exists, false otherwise
uint8_t BasicKvIterNext(BasicKvIterHandle* handle, uint64_t workerId);

//! Whether the iterator has a previous key in a basic key-value store at workerId
//! @return true if the previous key exists, false otherwise
uint8_t BasicKvIterHasPrev(BasicKvIterHandle* handle, uint64_t workerId);

//! Seek to the previous key in a basic key-value store at workerId
//! @return true if the previous key exists, false otherwise
uint8_t BasicKvIterPrev(BasicKvIterHandle* handle, uint64_t workerId);

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