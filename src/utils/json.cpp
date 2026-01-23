#include "utils/json.hpp"

#include "leanstore/base/error.hpp"
#include "leanstore/base/optional.hpp"
#include "leanstore/base/result.hpp"

#include <cassert>
#include <format>
#include <functional>
#include <memory>
#include <optional>

// Include rapidjson headers only in implementation
#define RAPIDJSON_NAMESPACE leanstore::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN namespace leanstore::rapidjson {
#define RAPIDJSON_NAMESPACE_END }

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#undef RAPIDJSON_NAMESPACE_END
#undef RAPIDJSON_NAMESPACE_BEGIN
#undef RAPIDJSON_NAMESPACE

namespace leanstore::utils {

//==============================================================================
// JsonObj::Impl - Implementation struct
//==============================================================================

struct JsonObj::Impl {
  rapidjson::Document doc_;

  Impl() {
    doc_.SetObject();
  }
};

//==============================================================================
// JsonArray::Impl - Implementation struct
//==============================================================================

struct JsonArray::Impl {
  rapidjson::Document doc_;

  Impl() {
    doc_.SetArray();
  }
};

//==============================================================================
// JsonObj - Special member functions
//==============================================================================

JsonObj::JsonObj() : impl_(std::make_unique<Impl>()) {
}

JsonObj::~JsonObj() = default;

JsonObj::JsonObj(JsonObj&& other) noexcept = default;

JsonObj& JsonObj::operator=(JsonObj&& other) noexcept {
  if (this != &other) {
    impl_ = std::make_unique<Impl>();
    impl_->doc_.Swap(other.impl_->doc_);
  }
  return *this;
}

//==============================================================================
// JsonObj - Serialization
//==============================================================================

std::string JsonObj::Serialize() const {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  impl_->doc_.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetSize());
}

Result<void> JsonObj::Deserialize(std::string_view json) {
  impl_->doc_.Parse(json.data(), json.size());
  if (impl_->doc_.HasParseError()) {
    return Error::General(std::format("Failed to parse JSON: {}", json));
  }
  return {};
}

//==============================================================================
// JsonObj - Add methods
//==============================================================================

void JsonObj::AddBool(std::string_view key, bool value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

void JsonObj::AddInt64(std::string_view key, int64_t value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

void JsonObj::AddUint64(std::string_view key, uint64_t value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

void JsonObj::AddString(std::string_view key, std::string_view value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.data(), value.size(), impl_->doc_.GetAllocator());
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

void JsonObj::AddJsonObj(std::string_view key, const JsonObj& value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.impl_->doc_, impl_->doc_.GetAllocator());
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

void JsonObj::AddJsonArray(std::string_view key, const JsonArray& value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), impl_->doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.impl_->doc_, impl_->doc_.GetAllocator());
  impl_->doc_.AddMember(key_copy, value_copy, impl_->doc_.GetAllocator());
}

//==============================================================================
// JsonObj - Get methods
//==============================================================================

const rapidjson::Value* JsonObj::GetJsonValue(std::string_view key) const {
  assert(impl_->doc_.IsObject() && "JsonObj must be an object");
  const auto& obj = impl_->doc_.GetObject();
  if (!obj.HasMember(key.data())) {
    return nullptr;
  }
  return &obj[key.data()];
}

Optional<bool> JsonObj::GetBool(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsBool()) {
    return std::nullopt;
  }
  return value->GetBool();
}

Optional<int64_t> JsonObj::GetInt64(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsInt64()) {
    return std::nullopt;
  }
  return value->GetInt64();
}

Optional<uint64_t> JsonObj::GetUint64(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsUint64()) {
    return std::nullopt;
  }
  return value->GetUint64();
}

Optional<std::string_view> JsonObj::GetString(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsString()) {
    return std::nullopt;
  }
  return std::string_view(value->GetString(), value->GetStringLength());
}

Optional<JsonObj> JsonObj::GetJsonObj(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsObject()) {
    return std::nullopt;
  }

  JsonObj json_obj;
  json_obj.impl_->doc_.CopyFrom(*value, json_obj.impl_->doc_.GetAllocator());
  return json_obj;
}

Optional<JsonArray> JsonObj::GetJsonArray(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsArray()) {
    return std::nullopt;
  }

  JsonArray json_array;
  json_array.impl_->doc_.CopyFrom(*value, json_array.impl_->doc_.GetAllocator());
  return json_array;
}

void JsonObj::Foreach(
    const std::function<void(std::string_view key, const JsonValue& value)>& fn) const {
  const auto& obj = impl_->doc_.GetObject();
  for (const auto& member : obj) {
    fn(member.name.GetString(), member.value);
  }
}

bool JsonObj::HasMember(std::string_view key) const {
  const auto& obj = impl_->doc_.GetObject();
  return obj.HasMember(key.data());
}

//==============================================================================
// JsonArray - Special member functions
//==============================================================================

JsonArray::JsonArray() : impl_(std::make_unique<Impl>()) {
}

JsonArray::~JsonArray() = default;

JsonArray::JsonArray(JsonArray&& other) noexcept = default;

JsonArray& JsonArray::operator=(JsonArray&& other) noexcept {
  if (this != &other) {
    impl_ = std::make_unique<Impl>();
    impl_->doc_.Swap(other.impl_->doc_);
  }
  return *this;
}

//==============================================================================
// JsonArray - Append methods
//==============================================================================

void JsonArray::AppendInt64(int64_t value) {
  auto value_copy = rapidjson::Value(value);
  impl_->doc_.PushBack(value_copy, impl_->doc_.GetAllocator());
}

void JsonArray::AppendString(std::string_view value) {
  auto value_copy = rapidjson::Value(value.data(), value.size(), impl_->doc_.GetAllocator());
  impl_->doc_.PushBack(value_copy, impl_->doc_.GetAllocator());
}

void JsonArray::AppendJsonObj(const JsonObj& value) {
  auto value_copy = rapidjson::Value(value.impl_->doc_, impl_->doc_.GetAllocator());
  impl_->doc_.PushBack(value_copy, impl_->doc_.GetAllocator());
}

void JsonArray::AppendJsonArray(const JsonArray& value) {
  auto value_copy = rapidjson::Value(value.impl_->doc_, impl_->doc_.GetAllocator());
  impl_->doc_.PushBack(value_copy, impl_->doc_.GetAllocator());
}

//==============================================================================
// JsonArray - Get methods
//==============================================================================

Optional<int64_t> JsonArray::GetInt64(size_t index) const {
  if (!impl_->doc_.IsArray()) {
    return std::nullopt;
  }

  const auto& array = impl_->doc_.GetArray();
  if (index >= array.Size()) {
    return std::nullopt;
  }

  const auto& value = array[index];
  if (!value.IsInt64()) {
    return std::nullopt;
  }

  return value.GetInt64();
}

Optional<std::string_view> JsonArray::GetString(size_t index) const {
  if (!impl_->doc_.IsArray()) {
    return std::nullopt;
  }

  const auto& array = impl_->doc_.GetArray();
  if (index >= array.Size()) {
    return std::nullopt;
  }

  const auto& value = array[index];
  if (!value.IsString()) {
    return std::nullopt;
  }

  return std::string_view(value.GetString(), value.GetStringLength());
}

Optional<JsonObj> JsonArray::GetJsonObj(size_t index) const {
  if (!impl_->doc_.IsArray()) {
    return std::nullopt;
  }

  const auto& array = impl_->doc_.GetArray();
  if (index >= array.Size()) {
    return std::nullopt;
  }

  const auto& value = array[index];
  if (!value.IsObject()) {
    return std::nullopt;
  }

  JsonObj json_obj;
  json_obj.impl_->doc_.CopyFrom(value, json_obj.impl_->doc_.GetAllocator());
  return json_obj;
}

Optional<JsonArray> JsonArray::GetJsonArray(size_t index) const {
  if (!impl_->doc_.IsArray()) {
    return std::nullopt;
  }

  const auto& array = impl_->doc_.GetArray();
  if (index >= array.Size()) {
    return std::nullopt;
  }

  const auto& value = array[index];
  if (!value.IsArray()) {
    return std::nullopt;
  }

  JsonArray json_array;
  json_array.impl_->doc_.CopyFrom(value, json_array.impl_->doc_.GetAllocator());
  return json_array;
}

uint64_t JsonArray::Size() const {
  if (!impl_->doc_.IsArray()) {
    return 0;
  }

  return impl_->doc_.GetArray().Size();
}

} // namespace leanstore::utils
