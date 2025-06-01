#ifndef LEANSTORE_C_H
#define LEANSTORE_C_H

#include "leanstore-c/store_option.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//------------------------------------------------------------------------------
// OwnedString API
//------------------------------------------------------------------------------

/// OwnedString is a data structure that holds an owned bytes buffer
typedef struct OwnedString {
  /// The owned data pointer
  char* data_;

  /// The size of the data
  uint64_t size_;

  /// The capacity of the data
  uint64_t capacity_;
} OwnedString;

/// Creates a new string, copying the data from the given buffer to the new string
/// @param data the data buffer
/// @param size the size of the data buffer
/// @return the new string, which should be destroyed by the caller with DestroyOwnedString()
OwnedString* CreateOwnedString(const char* data, uint64_t size);

/// Destroys a string
void DestroyOwnedString(OwnedString* str);

//------------------------------------------------------------------------------
// StringSlice API
//------------------------------------------------------------------------------

/// StringSlice is a read-only data structure that holds a slice of a bytes buffer
typedef struct StringSlice {
  /// The read-only data pointer
  const char* data_;

  /// The size of the data
  uint64_t size_;
} StringSlice;

//------------------------------------------------------------------------------
// LeanStore API
//------------------------------------------------------------------------------

typedef struct LeanStoreHandle LeanStoreHandle;

/// Create and init a leanstore instance
LeanStoreHandle* CreateLeanStore(StoreOption* option);

/// Deinit and destroy a leanstore instance
void DestroyLeanStore(LeanStoreHandle* handle);

/// Get the LeanStore instance, return NULL if the handle is invalid
void* GetLeanStore(LeanStoreHandle* handle);

//------------------------------------------------------------------------------
// Interfaces for metrics
//------------------------------------------------------------------------------

/// Start the global http metrics exposer
void StartMetricsHttpExposer(int32_t port);

/// Stop the global http metrics exposer
void StopMetricsHttpExposer();

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_C_H