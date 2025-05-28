#pragma once

#include "leanstore/slice.hpp"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace leanstore::utils {

using JsonValue = rapidjson::GenericValue<rapidjson::UTF8<>>;

inline std::string JsonToStr(rapidjson::Value* obj) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  obj->Accept(writer);
  return buffer.GetString();
}

template <typename... Args>
inline void AddMemberToJson(JsonValue* obj, JsonValue::AllocatorType& allocator, const char* name,
                            Args&&... args) {
  JsonValue member_name(name, allocator);
  JsonValue member_val(std::forward<Args>(args)...);
  obj->AddMember(member_name, member_val, allocator);
}

template <typename... Args>
inline void AddMemberToJson(JsonValue* obj, JsonValue::AllocatorType& allocator, const char* name,
                            Slice val) {
  JsonValue member_name(name, allocator);
  JsonValue member_val(reinterpret_cast<const char*>(val.data()), val.size(), allocator);
  obj->AddMember(member_name, member_val, allocator);
}

} // namespace leanstore::utils