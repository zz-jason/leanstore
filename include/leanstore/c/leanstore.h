#ifndef LEANSTORE_C_LEANSTORE_H
#define LEANSTORE_C_LEANSTORE_H

#include "leanstore/common/status.h"
#include "leanstore/common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

/// Forward declarations for opaque types
struct lean_str;          // Dynamic string with automatic memory management
struct lean_str_view;     // Read-only view of a string slice
struct lean_store_option; // Configuration options for store initialization
struct lean_store;        // Main database store instance
struct lean_session;      // Database session for transaction management
struct lean_btree;        // B-tree index structure for key-value storage
struct lean_cursor;       // Iterator for traversing B-tree entries
struct lean_table;        // Logical table abstraction
struct lean_row;          // Row of typed datums
struct lean_table_cursor; // Cursor for logical tables

/// Dynamic string with automatic memory management
typedef struct lean_str {
  const char* data;  // Pointer to string data
  uint64_t size;     // Current string length
  uint64_t capacity; // Allocated buffer capacity

} lean_str;

/// Read-only view of a string slice (does not own memory)
typedef struct lean_str_view {
  const char* data; // Pointer to string data
  uint64_t size;    // String length

} lean_str_view;

/// String manipulation functions

/// Initialize string with given capacity
void lean_str_init(lean_str* s, uint64_t size);

/// Free string memory
void lean_str_deinit(lean_str* s);

/// Reserve memory for target capacity
void lean_str_reserve(lean_str* s, uint64_t target_capacity);

/// Assign data to string
void lean_str_assign(lean_str* s, const char* data, uint64_t size);

/// Append data to string
void lean_str_append(lean_str* s, const char* data, uint64_t size);

/// Append string view to string
void lean_str_append_view(lean_str* s, struct lean_str_view str_view);

/// Store management functions

/// Open database store
lean_status lean_open_store(struct lean_store_option* option, struct lean_store** store);

/// Table schema definitions
typedef enum lean_column_type {
  LEAN_COLUMN_TYPE_BOOL = 0,
  LEAN_COLUMN_TYPE_INT32,
  LEAN_COLUMN_TYPE_INT64,
  LEAN_COLUMN_TYPE_UINT64,
  LEAN_COLUMN_TYPE_FLOAT32,
  LEAN_COLUMN_TYPE_FLOAT64,
  LEAN_COLUMN_TYPE_BINARY,
  LEAN_COLUMN_TYPE_STRING,
} lean_column_type;

typedef struct lean_table_column_def {
  struct lean_str_view name;
  lean_column_type type;
  bool nullable;
  /// For fixed length types (CHAR, BINARY, etc). Zero implies variable sized.
  uint32_t fixed_length;
} lean_table_column_def;

typedef struct lean_table_def {
  struct lean_str_view name;
  const struct lean_table_column_def* columns;
  uint32_t num_columns;
  /// pk_cols[i] is the column position in the table schema.
  const uint32_t* pk_cols;
  uint32_t pk_cols_count;
  lean_btree_type primary_index_type;
  struct lean_btree_config primary_index_config;
} lean_table_def;

/// Datum representation for a single column. The type is defined by the table schema.
typedef union lean_datum {
  bool b;
  int32_t i32;
  int64_t i64;
  uint64_t u64;
  float f32;
  double f64;
  struct lean_str_view str; /// used for binary/string values
} lean_datum;

/// Row consisting of typed datums.
typedef struct lean_row {
  lean_datum* columns;
  bool* nulls; // array of length num_columns, true if null
  uint32_t num_columns;
} lean_row;

/// Main database store instance with connection management
typedef struct lean_store {
  /// Create new session (blocking)
  struct lean_session* (*connect)(struct lean_store* store);

  /// Try to create session (non-blocking)
  struct lean_session* (*try_connect)(struct lean_store* store);

  /// Close store and free resources
  void (*close)(struct lean_store* store);

} lean_store;

/// Database session for transaction and B-tree management
typedef struct lean_session {
  /// Begin new transaction
  void (*start_tx)(struct lean_session* session);

  /// Commit current transaction
  void (*commit_tx)(struct lean_session* session);

  /// Abort current transaction
  void (*abort_tx)(struct lean_session* session);

  /// Create new B-tree index
  lean_status (*create_btree)(struct lean_session* session,
                              const char* btree_name,
                              lean_btree_type btree_type);

  /// Delete B-tree index
  void (*drop_btree)(struct lean_session* session, const char* btree_name);

  /// Get B-tree by name
  struct lean_btree* (*get_btree)(struct lean_session* session, const char* btree_name);

  /// Create a logical table
  lean_status (*create_table)(struct lean_session* session, const struct lean_table_def* table_def);

  /// Drop a logical table
  lean_status (*drop_table)(struct lean_session* session, const char* table_name);

  /// Get table by name
  struct lean_table* (*get_table)(struct lean_session* session, const char* table_name);

  /// Close session
  void (*close)(struct lean_session* session);

} lean_session;

/// B-tree index for key-value storage operations
typedef struct lean_btree {
  /// Insert key-value pair
  lean_status (*insert)(struct lean_btree* btree, lean_str_view key, lean_str_view value);

  /// Remove key and its value
  lean_status (*remove)(struct lean_btree* btree, lean_str_view key);

  /// Find value by key
  lean_status (*lookup)(struct lean_btree* btree, lean_str_view key, lean_str* value);

  /// Create cursor for iteration
  struct lean_cursor* (*open_cursor)(struct lean_btree* btree);

  /// Close B-tree handle
  void (*close)(struct lean_btree* btree);

} lean_btree;

/// Logical table for row insert/update/lookup
typedef struct lean_table {
  /// Insert a row using typed datums.
  lean_status (*insert)(struct lean_table* table, const struct lean_row* row);

  /// Remove a row by primary key columns in the row.
  lean_status (*remove)(struct lean_table* table, const struct lean_row* row);

  /// Lookup a row by primary key columns in the row, decoding into out_row.
  lean_status (*lookup)(struct lean_table* table, const struct lean_row* row,
                        struct lean_row* out_row);

  /// Open iterator over table rows
  struct lean_table_cursor* (*open_cursor)(struct lean_table* table);

  /// Close the table handle
  void (*close)(struct lean_table* table);

} lean_table;

/// Iterator for traversing B-tree entries
typedef struct lean_cursor {
  /// Move to first entry
  bool (*seek_to_first)(struct lean_cursor* cursor);

  /// Move to first entry >= key
  bool (*seek_to_first_ge)(struct lean_cursor* cursor, lean_str_view key);

  /// Move to next entry
  bool (*next)(struct lean_cursor* cursor);

  /// Move to last entry
  bool (*seek_to_last)(struct lean_cursor* cursor);

  /// Move to last entry <= key
  bool (*seek_to_last_le)(struct lean_cursor* cursor, lean_str_view key);

  /// Move to previous entry
  bool (*prev)(struct lean_cursor* cursor);

  /// Check if cursor is valid
  bool (*is_valid)(struct lean_cursor* cursor);

  /// Get current key
  void (*current_key)(struct lean_cursor* cursor, lean_str* key);

  /// Get current value
  void (*current_value)(struct lean_cursor* cursor, lean_str* value);

  /// Remove current entry
  lean_status (*remove_current)(struct lean_cursor* cursor);

  /// Update current value
  lean_status (*update_current)(struct lean_cursor* cursor, lean_str_view new_value);

  /// Close cursor
  void (*close)(struct lean_cursor* cursor);

} lean_cursor;

/// Iterator for traversing table rows with decoded datums
typedef struct lean_table_cursor {
  /// Move to first entry
  bool (*seek_to_first)(struct lean_table_cursor* cursor);

  /// Move to first entry >= key
  bool (*seek_to_first_ge)(struct lean_table_cursor* cursor, const struct lean_row* key_row);

  /// Move to next entry
  bool (*next)(struct lean_table_cursor* cursor);

  /// Move to last entry
  bool (*seek_to_last)(struct lean_table_cursor* cursor);

  /// Move to last entry <= key
  bool (*seek_to_last_le)(struct lean_table_cursor* cursor, const struct lean_row* key_row);

  /// Move to previous entry
  bool (*prev)(struct lean_table_cursor* cursor);

  /// Check if cursor is valid
  bool (*is_valid)(struct lean_table_cursor* cursor);

  /// Get current row
  void (*current_row)(struct lean_table_cursor* cursor, struct lean_row* row);

  /// Remove current entry
  lean_status (*remove_current)(struct lean_table_cursor* cursor);

  /// Update current entry with new row values
  lean_status (*update_current)(struct lean_table_cursor* cursor, const struct lean_row* row);

  /// Close cursor
  void (*close)(struct lean_table_cursor* cursor);

} lean_table_cursor;

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif
