#include "leanstore/c/leanstore.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

void lean_str_init(lean_str* s, uint64_t capacity) {
  s->size = 0;
  s->capacity = capacity;
  if (capacity == 0) {
    s->data = nullptr;
    return;
  }
  s->data = (char*)malloc(capacity);
}

void lean_str_deinit(lean_str* s) {
  if (s->data != nullptr) {
    free((void*)s->data);
    s->data = nullptr;
  }
  s->size = 0;
  s->capacity = 0;
}

void lean_str_reserve(lean_str* s, uint64_t target_capacity) {
  if (target_capacity <= s->capacity) {
    return;
  }

  uint64_t new_capacity = target_capacity > 2 * s->capacity ? target_capacity : 2 * s->capacity;
  void* new_data = realloc((void*)s->data, new_capacity);
  if (new_data) {
    s->data = (char*)new_data;
    s->capacity = new_capacity;
  } else {
    lean_str_deinit(s);
    perror("realloc failed, memory exhausted?");
    exit(EXIT_FAILURE);
  }
}

void lean_str_assign(lean_str* s, const char* data, uint64_t size) {
  lean_str_reserve(s, size);

  memcpy((void*)s->data, (const void*)data, size);
  s->size = size;
}

void lean_str_append(lean_str* s, const char* data, uint64_t size) {
  lean_str_reserve(s, s->size + size);

  memcpy((void*)(s->data + s->size), (const void*)data, size);
  s->size += size;
}

void lean_str_append_view(lean_str* s, lean_str_view str_view) {
  lean_str_append(s, str_view.data, str_view.size);
}
