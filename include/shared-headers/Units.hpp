#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

using TREEID = uint64_t; // data structure ID
using PID = uint64_t;    // page ID
using LID = uint64_t;    // log ID
using TXID = uint64_t;

using COMMANDID = uint32_t;
using WORKERID = uint16_t;

const COMMANDID kRemoveCommandMark = 1u << 31;

using StringMap = std::unordered_map<std::string, std::string>;

struct Slice : public std::basic_string_view<uint8_t> {
  Slice() : std::basic_string_view<uint8_t>() {
  }

  Slice(const std::string& str)
      : std::basic_string_view<uint8_t>(
            reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const std::string_view& str)
      : std::basic_string_view<uint8_t>(
            reinterpret_cast<const uint8_t*>(str.data()), str.size()) {
  }

  Slice(const uint8_t* data, size_t size)
      : std::basic_string_view<uint8_t>(data, size) {
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data()), size());
  }
};

namespace leanstore {

inline std::string ToString(Slice slice) {
  return std::string(reinterpret_cast<const char*>(slice.data()), slice.size());
}

inline Slice ToSlice(const std::string& str) {
  return Slice(reinterpret_cast<const uint8_t*>(str.data()), str.size());
}

enum class TxMode : uint8_t {
  kLongRunning = 0,
  kShortRunning = 1,
};

inline std::string ToString(TxMode txMode) {
  switch (txMode) {
  case TxMode::kLongRunning: {
    return "LongRunning";
  }
  case TxMode::kShortRunning: {
    return "ShortRunning";
  }
  }
  return "Unknown TxMode";
}

enum class IsolationLevel : uint8_t {
  // kReadUnCommitted = 0,
  // kReadCommitted = 1,
  kSnapshotIsolation = 2,
  kSerializable = 3,
};

inline IsolationLevel ParseIsolationLevel(std::string str) {
  if (str == "ser") {
    return leanstore::IsolationLevel::kSerializable;
  }
  if (str == "si") {
    return leanstore::IsolationLevel::kSnapshotIsolation;
  }
  return leanstore::IsolationLevel::kSnapshotIsolation;
}

} // namespace leanstore
