#include "utils/json.hpp"

#include "leanstore/utils/error.hpp"
#include "leanstore/utils/result.hpp"

#include <cassert>

#define RAPIDJSON_NAMESPACE leanstore::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN namespace leanstore::rapidjson {
#define RAPIDJSON_NAMESPACE_END }

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#undef RAPIDJSON_NAMESPACE_END
#undef RAPIDJSON_NAMESPACE_BEGIN
#undef RAPIDJSON_NAMESPACE

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

namespace leanstore::utils {

JsonObj& JsonObj::operator=(JsonObj&& other) {
  if (this != &other) {
    doc_.SetObject();
    doc_.Swap(other.doc_);
  }
  return *this;
}

std::string JsonObj::Serialize() const {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc_.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetSize());
}

Result<void> JsonObj::Deserialize(std::string_view json) {
  doc_.Parse(json.data(), json.size());
  if (doc_.HasParseError()) {
    return std::unexpected(Error(ErrorCode::kGeneral, "Failed to parse JSON: {}",
                                 std::string(json.data(), json.size())));
  }
  return {};
}

void JsonObj::AddBool(std::string_view key, bool value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

void JsonObj::AddInt64(std::string_view key, int64_t value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

void JsonObj::AddUint64(std::string_view key, uint64_t value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value);
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

void JsonObj::AddString(std::string_view key, std::string_view value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.data(), doc_.GetAllocator());
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

void JsonObj::AddJsonObj(std::string_view key, const JsonObj& value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.doc_, doc_.GetAllocator());
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

void JsonObj::AddJsonArray(std::string_view key, const JsonArray& value) {
  auto key_copy = rapidjson::Value(key.data(), key.size(), doc_.GetAllocator());
  auto value_copy = rapidjson::Value(value.doc_, doc_.GetAllocator());
  doc_.AddMember(key_copy, value_copy, doc_.GetAllocator());
}

std::optional<bool> JsonObj::GetBool(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsBool()) {
    return {};
  }
  return value->GetBool();
}

std::optional<int64_t> JsonObj::GetInt64(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsInt64()) {
    return {};
  }
  return value->GetInt64();
}

std::optional<uint64_t> JsonObj::GetUint64(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsUint64()) {
    return {};
  }
  return value->GetUint64();
}

std::optional<std::string_view> JsonObj::GetString(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsString()) {
    return {};
  }
  return std::string_view(value->GetString(), value->GetStringLength());
}

std::optional<JsonObj> JsonObj::GetJsonObj(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsObject()) {
    return {};
  }

  JsonObj json;
  json.doc_.CopyFrom(*value, json.doc_.GetAllocator());
  return json;
}

std::optional<JsonArray> JsonObj::GetJsonArray(std::string_view key) const {
  const auto* value = GetJsonValue(key);
  if (!value || !value->IsArray()) {
    return {};
  }

  JsonArray json;
  json.doc_.CopyFrom(*value, json.doc_.GetAllocator());
  return json;
}

void JsonObj::Foreach(
    const std::function<void(std::string_view key, const JsonValue& value)>& fn) const {
  const auto& obj = doc_.GetObject();
  for (const auto& member : obj) {
    fn(member.name.GetString(), member.value);
  }
}

bool JsonObj::HasMember(std::string_view key) const {
  const auto& obj = doc_.GetObject();
  return obj.HasMember(key.data());
}

} // namespace leanstore::utils