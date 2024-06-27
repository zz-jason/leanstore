#pragma once

#include <rapidjson/document.h>

//! forward declaration
namespace leanstore::storage {
class BufferFrame;
} // namespace leanstore::storage

namespace leanstore::utils {

//! Convert the object to a JSON document.
template <typename ObjType>
void ToJson(const ObjType* obj [[maybe_unused]], rapidjson::Document* doc [[maybe_unused]]) {
}

//! Convert the object to a JSON document.
template <typename ObjType, typename JsonType, typename JsonAllocator>
void ToJson(ObjType* obj [[maybe_unused]], JsonType* doc [[maybe_unused]],
            JsonAllocator* allocator [[maybe_unused]]) {
}

//! Convert the object to a JSON string.
template <typename ObjType>
inline std::string ToJsonString(const ObjType* entry [[maybe_unused]]) {
  return "";
}

} // namespace leanstore::utils