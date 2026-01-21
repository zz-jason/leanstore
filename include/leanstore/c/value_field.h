#ifndef LEANSTORE_COMMON_VALUE_FIELD_H
#define LEANSTORE_COMMON_VALUE_FIELD_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

struct lean_field_info;
struct lean_field_update_info;

typedef struct lean_field_info {
  uint16_t offset_; // field offset in the value
  uint16_t size_;   // field size

} lean_field_info;

typedef struct lean_field_update_info {
  struct lean_field_info field_; // field to update
  const char* new_value_; // value to be updated in the field, length must be equal to field_.size_

} lean_field_update_info;

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif