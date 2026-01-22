#pragma once

#include "leanstore/base/optional.hpp"
#include "leanstore/base/result.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

// Include rapidjson for JsonValue (used in Foreach callback)
// Note: JsonValue must be a complete type because it's used in public API callbacks
#define RAPIDJSON_NAMESPACE leanstore::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN namespace leanstore::rapidjson {
#define RAPIDJSON_NAMESPACE_END }

#include <rapidjson/document.h>

#undef RAPIDJSON_NAMESPACE_END
#undef RAPIDJSON_NAMESPACE_BEGIN
#undef RAPIDJSON_NAMESPACE

namespace leanstore::utils {

using JsonValue = rapidjson::Value;

class JsonArray;

class JsonObj {
public:
  JsonObj();
  ~JsonObj();

  // Not copyable
  JsonObj(const JsonObj&) = delete;
  JsonObj& operator=(const JsonObj&) = delete;

  /// Move constructor and assignment (defined in .cpp due to Pimpl)
  JsonObj(JsonObj&& other) noexcept;
  JsonObj& operator=(JsonObj&& other) noexcept;

  std::string Serialize() const;
  Result<void> Deserialize(std::string_view json);

  //----------------------------------------------------------------------------
  // Utils to add element to a JSON object
  //----------------------------------------------------------------------------

  void AddBool(std::string_view key, bool value);
  void AddInt64(std::string_view key, int64_t value);
  void AddUint64(std::string_view key, uint64_t value);
  void AddString(std::string_view key, std::string_view value);
  void AddJsonObj(std::string_view key, const JsonObj& value);
  void AddJsonArray(std::string_view key, const JsonArray& value);

  //----------------------------------------------------------------------------
  // Utils to access element in a JSON object
  //----------------------------------------------------------------------------

  Optional<bool> GetBool(std::string_view key) const;
  Optional<int64_t> GetInt64(std::string_view key) const;
  Optional<uint64_t> GetUint64(std::string_view key) const;
  Optional<std::string_view> GetString(std::string_view key) const;
  Optional<JsonObj> GetJsonObj(std::string_view key) const;
  Optional<JsonArray> GetJsonArray(std::string_view key) const;
  void Foreach(const std::function<void(std::string_view key, const JsonValue& value)>& fn) const;
  bool HasMember(std::string_view key) const;

private:
  const rapidjson::Value* GetJsonValue(std::string_view key) const;

  // Pimpl idiom to hide rapidjson::Document from public header
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class JsonArray;
};

class JsonArray {
public:
  JsonArray();
  ~JsonArray();

  // Not copyable
  JsonArray(const JsonArray&) = delete;
  JsonArray& operator=(const JsonArray&) = delete;

  /// Move constructor and assignment (defined in .cpp due to Pimpl)
  JsonArray(JsonArray&& other) noexcept;
  JsonArray& operator=(JsonArray&& other) noexcept;

  //----------------------------------------------------------------------------
  // Utils to add element to a JSON array
  //----------------------------------------------------------------------------

  void AppendInt64(int64_t value);
  void AppendString(std::string_view value);
  void AppendJsonObj(const JsonObj& value);
  void AppendJsonArray(const JsonArray& value);

  //----------------------------------------------------------------------------
  // Utils to access element in a JSON array
  //----------------------------------------------------------------------------

  Optional<int64_t> GetInt64(size_t index) const;
  Optional<std::string_view> GetString(size_t index) const;
  Optional<JsonObj> GetJsonObj(size_t index) const;
  Optional<JsonArray> GetJsonArray(size_t index) const;
  uint64_t Size() const;

private:
  // Pimpl idiom to hide rapidjson::Document from public header
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class JsonObj;
};

} // namespace leanstore::utils
