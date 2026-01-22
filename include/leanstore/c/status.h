#ifndef LEANSTORE_COMMON_STATUS_H
#define LEANSTORE_COMMON_STATUS_H

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

/// Status codes returned by LeanStore operations
typedef enum lean_status {
  LEAN_STATUS_OK = 0,       // Operation completed successfully
  LEAN_ERR_NOT_FOUND,       // Requested key was not found
  LEAN_ERR_DUPLICATED,      // Key already exists (for unique constraints)
  LEAN_ERR_CONFLICT,        // Transaction conflict occurred
  LEAN_ERR_OPEN_STORE,      // Failed to open/initialize store
  LEAN_ERR_CTEATE_BTREE,    // Failed to create B-tree
  LEAN_ERR_UNSUPPORTED,     // Operation not supported
  LEAN_ERR_CREATE_TABLE,    // Failed to create a table
  LEAN_ERR_TABLE_NOT_FOUND, // Table not found
} lean_status;

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_COMMON_STATUS_H
