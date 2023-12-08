#pragma once

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace leanstore {
namespace utils {

using JsonValue = rapidjson::GenericValue<rapidjson::UTF8<>>;

inline std::string JsonToStr(rapidjson::Value* obj) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  obj->Accept(writer);
  return buffer.GetString();
}

template <typename... Args>
inline void AddMemberToJson(JsonValue* obj, JsonValue::AllocatorType& allocator,
                            const char* name, Args&&... args) {
  JsonValue memberName(name, allocator);
  JsonValue memberVal(std::forward<Args>(args)...);
  obj->AddMember(memberName, memberVal, allocator);
}

template <typename... Args>
inline void AddMemberToJson(JsonValue* obj, JsonValue::AllocatorType& allocator,
                            const char* name, Slice val) {
  JsonValue memberName(name, allocator);
  JsonValue memberVal(reinterpret_cast<const char*>(val.data()), val.size(),
                      allocator);
  obj->AddMember(memberName, memberVal, allocator);
}

} // namespace utils
} // namespace leanstore