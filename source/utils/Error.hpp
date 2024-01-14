#pragma once

#include "shared-headers/Units.hpp"

#include <format>
#include <string>
#include <utility>

namespace leanstore {
namespace utils {

enum class ErrorCode : u64 {
  kGeneral = 1,

  // File related error code
  kFile = 100,
  kFileOpen,
  kFileClose,
  kFileSeek,
  kFileRead,

  // BTreeLL related error code
  kBTreeLL = 200,
  kBTreeLLCreate,

  // BTreeVI related error code
  kBTreeVI = 300,
  kBTreeVICreate,

};

class Error {
private:
  ErrorCode mCode = ErrorCode::kGeneral;
  std::string mMessage = "";

public:
  Error() = default;

  template <typename... Args>
  Error(ErrorCode code, const std::string& fmt, Args&&... args)
      : mCode(code),
        mMessage(std::vformat(fmt, std::make_format_args(args...))) {
  }

  // copy construct
  Error(const Error& other) = default;

  // copy assign
  Error& operator=(const Error& other) = default;

  // move construct
  Error(Error&& other) noexcept
      : mCode(other.mCode),
        mMessage(std::move(other.mMessage)) {
  }

  // move assign
  Error& operator=(Error&& other) noexcept {
    mCode = other.mCode;
    mMessage = std::move(other.mMessage);
    return *this;
  }

  ~Error() = default;

  inline bool operator==(const Error& other) const {
    return mCode == other.mCode && mMessage == other.mMessage;
  }

  inline std::string ToString() const {
    return std::format("ER-{}: {}", static_cast<u64>(mCode), mMessage);
  }

public:
  template <typename... Args> inline static Error General(Args&&... args) {
    const std::string msg = "{}";
    return Error(ErrorCode::kGeneral, msg, std::forward<Args>(args)...);
  }

  // BTreeVI
  template <typename... Args>
  inline static Error BTreeLLCreation(Args&&... args) {
    const std::string msg = "Fail to create BTreeLL, treeName={}";
    return Error(ErrorCode::kBTreeLLCreate, msg, std::forward<Args>(args)...);
  }

  // File
  template <typename... Args> inline static Error FileOpen(Args&&... args) {
    const std::string msg = "Fail to open file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileOpen, msg, std::forward<Args>(args)...);
  }

  template <typename... Args> inline static Error FileClose(Args&&... args) {
    const std::string msg =
        "Fail to close file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileClose, msg, std::forward<Args>(args)...);
  }

  template <typename... Args> inline static Error FileSeek(Args&&... args) {
    const std::string msg = "Fail to seek file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileSeek, msg, std::forward<Args>(args)...);
  }

  template <typename... Args> inline static Error FileRead(Args&&... args) {
    const std::string msg = "Fail to read file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileRead, msg, std::forward<Args>(args)...);
  }
};

} // namespace utils
} // namespace leanstore
