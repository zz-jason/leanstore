#pragma once

#include <cstdint>
#include <format>
#include <string>
#include <utility>

namespace leanstore {
namespace utils {

enum class ErrorCode : uint64_t {
  kGeneral = 1,

  // File related error code
  kFile = 100,
  kFileOpen,
  kFileClose,
  kFileSeek,
  kFileRead,
  kFileWrite,

  // AIO related error code
  kAioAgain = 150,
  kAioBadf,
  kAioFault,
  kAioInvalid,
  kAioNoSys,
  kAioPerm,
  kAioUnknown,

  // BasicKV related error code
  kBasicKV = 200,
  kBasicKVCreate,

  // TransactionKV related error code
  kTransactionKV = 300,
  kTransactionKVCreate,

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
  Error(Error&& other) noexcept : mCode(other.mCode), mMessage(std::move(other.mMessage)) {
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
    return std::format("ER-{}: {}", static_cast<uint64_t>(mCode), mMessage);
  }

  inline uint64_t Code() const {
    return static_cast<uint64_t>(mCode);
  }

public:
  template <typename... Args>
  inline static Error General(Args&&... args) {
    const std::string msg = "{}";
    return Error(ErrorCode::kGeneral, msg, std::forward<Args>(args)...);
  }

  // TransactionKV
  template <typename... Args>
  inline static Error BasicKVCreate(Args&&... args) {
    const std::string msg = "Fail to create BasicKV, treeName={}";
    return Error(ErrorCode::kBasicKVCreate, msg, std::forward<Args>(args)...);
  }

  // File
  template <typename... Args>
  inline static Error FileOpen(Args&&... args) {
    const std::string msg = "Fail to open file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileOpen, msg, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline static Error FileClose(Args&&... args) {
    const std::string msg = "Fail to close file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileClose, msg, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline static Error FileSeek(Args&&... args) {
    const std::string msg = "Fail to seek file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileSeek, msg, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline static Error FileRead(Args&&... args) {
    const std::string msg = "Fail to read file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileRead, msg, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline static Error FileWrite(Args&&... args) {
    const std::string msg = "Fail to write file, file={}, errno={}, strerror={}";
    return Error(ErrorCode::kFileWrite, msg, std::forward<Args>(args)...);
  }

  static Error ErrorAio(int retCode, const std::string& apiName) {
    switch (-retCode) {
    case EAGAIN:
      return Error(ErrorCode::kAioAgain,
                   std::format("AIO({}) failed with EAGAIN, insufficient resources", apiName));
    case EBADF:
      return Error(ErrorCode::kAioBadf, "AIO({}) failed with EBADF, bad file descriptor", apiName);
    case EFAULT:
      return Error(ErrorCode::kAioFault,
                   "AIO({}) failed with EFAULT, one of the data structures "
                   "points to invalid data",
                   apiName);
    case EINVAL:
      return Error(ErrorCode::kAioInvalid, "AIO({}) failed with EINVAL, invalid argument", apiName);
    case ENOSYS:
      return Error(ErrorCode::kAioNoSys, "AIO({}) failed with ENOSYS, not implemented", apiName);
    case EPERM:
      return Error(ErrorCode::kAioPerm, "AIO({}) failed with EPERM, operation not permitted",
                   apiName);
    default:
      return Error(ErrorCode::kAioUnknown, "AIO({}) failed with unknown error code {}", apiName,
                   retCode);
    }
  }
};

} // namespace utils
} // namespace leanstore
