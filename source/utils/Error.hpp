#pragma once

#include "shared-headers/Units.hpp"

#include <format>
#include <string>

namespace leanstore {
namespace utils {

enum class ErrorCode : u64 {
  kGeneral = 1,

  kBTreeLL = 100,
  kBTreeLLCreate = 101,

  kBTreeVI = 200,
  kBTreeVICreate = 201,
};

class Error {
public:
  u64 mCode = 0;
  std::string mMessage = "";

public:
  Error() = default;
  Error(u64 code, const std::string& message) : mCode(code), mMessage(message) {
  }

  ~Error() = default;

public:
  template <typename... Args> inline static Error General(Args&&... args) {
    const u64 code = static_cast<u64>(ErrorCode::kGeneral);
    const std::string msg = "ER-{}: {}";
    return Error(code, std::vformat(msg, std::make_format_args(code, args...)));
  }

  template <typename... Args>
  inline static Error BTreeLLCreation(Args&&... args) {
    const u64 code = static_cast<u64>(ErrorCode::kBTreeLLCreate);
    const std::string msg = "ER-{}: Fail to create BTreeLL, treeName={}";
    return Error(code, std::vformat(msg, std::make_format_args(code, args...)));
  }
};

} // namespace utils
} // namespace leanstore
