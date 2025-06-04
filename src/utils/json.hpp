#pragma once

#include "leanstore/utils/result.hpp"

#define RAPIDJSON_NAMESPACE leanstore::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN namespace leanstore::rapidjson {
#define RAPIDJSON_NAMESPACE_END }

#include <rapidjson/document.h>

#undef RAPIDJSON_NAMESPACE_END
#undef RAPIDJSON_NAMESPACE_BEGIN
#undef RAPIDJSON_NAMESPACE

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

namespace leanstore::utils {

using JsonValue = rapidjson::Value;

class JsonArray;

class JsonObj {
public:
  JsonObj() {
    doc_.SetObject();
  }

  ~JsonObj() = default;

  // Not copyable or movable
  JsonObj(const JsonObj&) = delete;
  JsonObj& operator=(const JsonObj&) = delete;

  /// Move constructor
  JsonObj(JsonObj&& other) {
    // call move assignment
    *this = std::move(other);
  }

  /// Move assignment
  JsonObj& operator=(JsonObj&& other);

  std::string Serialize() const;
  Result<void> Deserialize(const std::string_view& json);

  //----------------------------------------------------------------------------
  // Utils to add element to a JSON object
  //----------------------------------------------------------------------------

  void AddBool(const std::string_view& key, bool value);
  void AddInt64(const std::string_view& key, int64_t value);
  void AddString(const std::string_view& key, const std::string_view& value);
  void AddJsonObj(const std::string_view& key, const JsonObj& value);
  void AddJsonArray(const std::string_view& key, const JsonArray& value);

  //----------------------------------------------------------------------------
  // Utils to access element in a JSON object
  //----------------------------------------------------------------------------

  std::optional<bool> GetBool(const std::string_view& key) const;
  std::optional<int64_t> GetInt64(const std::string_view& key) const;
  std::optional<std::string_view> GetString(const std::string_view& key) const;
  std::optional<JsonObj> GetJsonObj(const std::string_view& key) const;
  std::optional<JsonArray> GetJsonArray(const std::string_view& key) const;
  void Foreach(
      const std::function<void(const std::string_view& key, const JsonValue& value)>& fn) const;
  bool HasMember(const std::string_view& key) const;

private:
  rapidjson::Document doc_;

  friend class JsonArray;
};

class JsonArray {
public:
  JsonArray() {
    doc_.SetArray();
  }

  ~JsonArray() = default;

  // Not copyable or movable
  JsonArray(const JsonArray&) = delete;
  JsonArray& operator=(const JsonArray&) = delete;

  /// Move constructor
  JsonArray(JsonArray&& other) {
    // call move assignment
    *this = std::move(other);
  }

  /// Move assignment
  JsonArray& operator=(JsonArray&& other) {
    if (this != &other) {
      doc_ = std::move(other.doc_);
    }
    return *this;
  }

  //----------------------------------------------------------------------------
  // Utils to add element to a JSON array
  //----------------------------------------------------------------------------

  void AppendInt64(int64_t value) {
    auto value_copy = rapidjson::Value(value);
    doc_.PushBack(value_copy, doc_.GetAllocator());
  }

  void AppendString(const std::string_view& value) {
    auto value_copy = rapidjson::Value(value.data(), value.size(), doc_.GetAllocator());
    doc_.PushBack(value_copy, doc_.GetAllocator());
  }

  void AppendJsonObj(const JsonObj& value) {
    auto value_copy = rapidjson::Value(value.doc_, doc_.GetAllocator());
    doc_.PushBack(value_copy, doc_.GetAllocator());
  }

  void AppendJsonArray(const JsonArray& value) {
    auto value_copy = rapidjson::Value(value.doc_, doc_.GetAllocator());
    doc_.PushBack(value_copy, doc_.GetAllocator());
  }

  //----------------------------------------------------------------------------
  // Utils to access element in a JSON array
  //----------------------------------------------------------------------------

  std::optional<int64_t> GetInt64(size_t index) const {
    if (!doc_.IsArray()) {
      return {};
    }

    const auto& array = doc_.GetArray();
    if (index >= array.Size()) {
      return {};
    }

    const auto& value = array[index];
    if (!value.IsInt64()) {
      return {};
    }

    return value.GetInt64();
  }

  std::optional<std::string_view> GetString(size_t index) const {
    if (!doc_.IsArray()) {
      return {};
    }

    const auto& array = doc_.GetArray();
    if (index >= array.Size()) {
      return {};
    }

    const auto& value = array[index];
    if (!value.IsString()) {
      return {};
    }

    return std::string_view(value.GetString(), value.GetStringLength());
  }

  std::optional<JsonObj> GetJsonObj(size_t index) const {
    if (!doc_.IsArray()) {
      return {};
    }

    const auto& array = doc_.GetArray();
    if (index >= array.Size()) {
      return {};
    }

    const auto& value = array[index];
    if (!value.IsObject()) {
      return {};
    }

    JsonObj json_obj;
    json_obj.doc_.CopyFrom(value, json_obj.doc_.GetAllocator());
    return json_obj;
  }

  std::optional<JsonArray> GetJsonArray(size_t index) const {
    if (!doc_.IsArray()) {
      return {};
    }

    const auto& array = doc_.GetArray();
    if (index >= array.Size()) {
      return {};
    }

    const auto& value = array[index];
    if (!value.IsArray()) {
      return {};
    }

    JsonArray json_array;
    json_array.doc_.CopyFrom(value, json_array.doc_.GetAllocator());
    return json_array;
  }

  uint64_t Size() const {
    if (!doc_.IsArray()) {
      return 0;
    }

    return doc_.GetArray().Size();
  }

private:
  rapidjson::Document doc_;

  friend class JsonObj;
};

} // namespace leanstore::utils
