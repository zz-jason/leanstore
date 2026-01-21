#pragma once

#include "leanstore/c/value_field.h"

#include <cstring>
#include <memory>
#include <vector>
namespace leanstore {

class FieldsToUpdate {
public:
  class Builder;

  FieldsToUpdate(std::vector<lean_field_update_info>&& updates) : updates_(std::move(updates)) {
  }

  static void ApplyUpdates(char* record, const FieldsToUpdate& updates) {
    for (const auto& update : updates.updates_) {
      std::memcpy(record + update.field_.offset_, update.new_value_, update.field_.size_);
    }
  }

private:
  std::vector<lean_field_update_info> updates_;
}; // FieldsToUpdate

/**
Usage example:

auto fields_to_update = FieldsToUpdate::Builder().AddField(0, 4, &new_val).Build();

 */

class FieldsToUpdate::Builder {
public:
  Builder();
  ~Builder();

  Builder& AddField(uint16_t offset, uint16_t size, const void* new_value) {
    updates_.push_back({{offset, size}, (const char*)new_value});
    return *this;
  }

  std::unique_ptr<FieldsToUpdate> Build() {
    return std::make_unique<FieldsToUpdate>(std::move(updates_));
  }

private:
  std::vector<lean_field_update_info> updates_;

}; // FieldsToUpdate::Builder

} // namespace leanstore