#include "leanstore/c/leanstore.h"
#include "leanstore/c/types.h"

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  // prepare store options
  struct lean_store_option* option =
      lean_store_option_create("/tmp/leanstore/examples/BasicKvExample");
  option->create_from_scratch_ = 1;
  option->worker_threads_ = 2;
  option->max_concurrent_transaction_per_worker_ = 4;
  option->enable_bulk_insert_ = 0;
  option->enable_eager_gc_ = 1;

  // open store, moves the ownership of option to it
  struct lean_store* store = NULL;
  lean_status status = lean_open_store(option, &store);
  if (status != LEAN_STATUS_OK) {
    printf("open store failed\n");
    lean_store_option_destroy(option);
    exit(-1);
  }

  // connect to leanstore
  struct lean_session* session_0 = store->connect(store);
  struct lean_session* session_1 = store->connect(store);
  if (session_0 == NULL || session_1 == NULL) {
    printf("connect to store failed\n");

    // sessions need to be closed explicitly
    if (session_0 != NULL) {
      session_0->close(session_0);
    }
    if (session_1 != NULL) {
      session_1->close(session_1);
    }

    // store also needs to be closed explicitly
    store->close(store);
    lean_store_option_destroy(option);
    exit(-1);
  }

  // create a basic btree key-value store
  const char* btree_name = "test_btree";
  status = session_0->create_btree(session_0, btree_name, LEAN_BTREE_TYPE_ATOMIC);
  assert(status == LEAN_STATUS_OK);

  // key-value pair 1
  lean_str_view key1 = {.data = "Hello", .size = strlen("Hello")};
  lean_str_view val1 = {.data = "World", .size = strlen("World")};

  // key-value pair 2
  lean_str_view key2 = {.data = "Hello2", .size = strlen("Hello2")};
  lean_str_view val2 = {.data = "World2", .size = strlen("World2")};

  // insert a key value in session 0
  struct lean_btree* btree_s0 = session_0->get_btree(session_0, btree_name);
  assert(btree_s0 != NULL);
  status = btree_s0->insert(btree_s0, key1, val1);
  assert(status == LEAN_STATUS_OK);

  // lookup in session 1
  struct lean_str val_lookup;
  lean_str_init(&val_lookup, 8);
  struct lean_btree* btree_s1 = session_1->get_btree(session_1, btree_name);
  assert(btree_s1 != NULL);
  status = btree_s1->lookup(btree_s1, key1, &val_lookup);
  assert(status == LEAN_STATUS_OK);
  printf("%.*s, %.*s\n", (int)key1.size, key1.data, (int)val_lookup.size, val_lookup.data);
  lean_str_deinit(&val_lookup);

  // insert more key-values in session 0
  status = btree_s0->insert(btree_s0, key2, val2);
  assert(status == LEAN_STATUS_OK);

  // assending iteration in session 1
  struct lean_cursor* cursor_s1 = btree_s1->open_cursor(btree_s1);
  assert(cursor_s1 != NULL);
  for (cursor_s1->seek_to_first(cursor_s1); cursor_s1->is_valid(cursor_s1);
       cursor_s1->next(cursor_s1)) {
    lean_str key, value;
    lean_str_init(&key, 8);
    lean_str_init(&value, 8);

    cursor_s1->current_key(cursor_s1, &key);
    cursor_s1->current_value(cursor_s1, &value);

    printf("%.*s, %.*s\n", (int)key.size, key.data, (int)value.size, value.data);

    lean_str_deinit(&key);
    lean_str_deinit(&value);
  }
  cursor_s1->close(cursor_s1);

  // descending iteration in session 0
  struct lean_cursor* cursor_s0 = btree_s0->open_cursor(btree_s0);
  assert(cursor_s0 != NULL);
  for (cursor_s0->seek_to_last(cursor_s0); cursor_s0->is_valid(cursor_s0);
       cursor_s0->prev(cursor_s0)) {
    lean_str key, value;
    lean_str_init(&key, 8);
    lean_str_init(&value, 8);
    cursor_s0->current_key(cursor_s0, &key);
    cursor_s0->current_value(cursor_s0, &value);
    printf("%.*s, %.*s\n", (int)key.size, key.data, (int)value.size, value.data);
    lean_str_deinit(&key);
    lean_str_deinit(&value);
  }
  cursor_s0->close(cursor_s0);

  // remove key-values in session 1
  status = btree_s1->remove(btree_s1, key1);
  assert(status == LEAN_STATUS_OK);
  status = btree_s1->remove(btree_s1, key2);
  assert(status == LEAN_STATUS_OK);

  // cleanup the opened btrees
  btree_s0->close(btree_s0);
  btree_s1->close(btree_s1);

  // cleanup opened sessions
  session_0->close(session_0);
  session_1->close(session_1);

  // cleanup store
  store->close(store);
}
