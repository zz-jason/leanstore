#ifndef LEANSTORE_C_LEANSTORE_H
#define LEANSTORE_C_LEANSTORE_H

#include "leanstore/common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

struct lean_str;
struct lean_str_view;
struct lean_store_option;
struct lean_store;
struct lean_session;
struct lean_btree;
struct lean_cursor;

/// lean_status
typedef enum lean_status {
  LEAN_STATUS_OK = 0,
  LEAN_ERR_NOT_FOUND,
  LEAN_ERR_DUPLICATED,
  LEAN_ERR_CONFLICT,
  LEAN_ERR_OPEN_STORE,
  LEAN_ERR_CTEATE_BTREE,
  LEAN_ERR_UNSUPPORTED,
} lean_status;

/// lean_str
typedef struct lean_str {
  const char* data;
  uint64_t size;
  uint64_t capacity;
} lean_str;

/// lean_str_view
typedef struct lean_str_view {
  const char* data;
  uint64_t size;
} lean_str_view;

/// lean_str related APIs
void lean_str_init(lean_str* s, uint64_t size);
void lean_str_deinit(lean_str* s);
void lean_str_reserve(lean_str* s, uint64_t target_capacity);
void lean_str_assign(lean_str* s, const char* data, uint64_t size);
void lean_str_append(lean_str* s, const char* data, uint64_t size);
void lean_str_append_view(lean_str* s, struct lean_str_view str_view);

/// Opens a store
lean_status lean_open_store(struct lean_store_option* option, struct lean_store** store);

/// lean_store
typedef struct lean_store {
  struct lean_session* (*connect)(struct lean_store* store);
  struct lean_session* (*try_connect)(struct lean_store* store);
  void (*close)(struct lean_store* store);
} lean_store;

/// lean_session
typedef struct lean_session {
  void (*start_tx)(struct lean_session* session);
  void (*commit_tx)(struct lean_session* session);
  void (*abort_tx)(struct lean_session* session);
  lean_status (*create_btree)(struct lean_session* session, const char* btree_name,
                              lean_btree_type btree_type);
  void (*drop_btree)(struct lean_session* session, const char* btree_name);
  struct lean_btree* (*get_btree)(struct lean_session* session, const char* btree_name);
  void (*close)(struct lean_session* session);
} lean_session;

/// lean_btree
typedef struct lean_btree {
  lean_status (*insert)(struct lean_btree* btree, lean_str_view key, lean_str_view value);
  lean_status (*remove)(struct lean_btree* btree, lean_str_view key);
  lean_status (*lookup)(struct lean_btree* btree, lean_str_view key, lean_str* value);
  struct lean_cursor* (*open_cursor)(struct lean_btree* btree);
  void (*close)(struct lean_btree* btree);
} lean_btree;

/// lean_cursor
typedef struct lean_cursor {
  bool (*seek_to_first)(struct lean_cursor* cursor);
  bool (*seek_to_first_ge)(struct lean_cursor* cursor, lean_str_view key);
  bool (*next)(struct lean_cursor* cursor);
  bool (*seek_to_last)(struct lean_cursor* cursor);
  bool (*seek_to_last_le)(struct lean_cursor* cursor, lean_str_view key);
  bool (*prev)(struct lean_cursor* cursor);
  bool (*is_valid)(struct lean_cursor* cursor);
  void (*current_key)(struct lean_cursor* cursor, lean_str* key);
  void (*current_value)(struct lean_cursor* cursor, lean_str* value);
  lean_status (*remove_current)(struct lean_cursor* cursor);
  lean_status (*update_current)(struct lean_cursor* cursor, lean_str_view new_value);
  void (*close)(struct lean_cursor* cursor);
} lean_cursor;

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif