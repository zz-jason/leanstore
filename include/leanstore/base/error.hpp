#pragma once

#include "leanstore/base/stacktrace.hpp"

#include <cstdint>
#include <format>
#include <string>

namespace leanstore {

/// Forward declaration of Error class
class Error;

/// All the error code names, values, and message formats are listed in this macro.
///
/// To add a new error code, simply add a new line in this macro with the
/// format, all the other code will be generated automatically.
#define LEAN_ERROR_CODE_LIST(ACTION)                                                               \
  ACTION(General, 001, "{}")                                                                       \
  ACTION(NotImplemented, 002, "{}")                                                                \
  ACTION(InvalidArgument, 003, "{}")                                                               \
  ACTION(Aio, 010, "AIO operation failed, operation={}, errno={}, strerror={}")                    \
  ACTION(FileOpen, 100, "Open file failed, file={}, errno={}, strerror={}")                        \
  ACTION(FileClose, 101, "Close file failed, file={}, errno={}, strerror={}")                      \
  ACTION(FileSeek, 102, "Seek file failed, file={}, errno={}, strerror={}")                        \
  ACTION(FileRead, 103, "Read file failed, file={}, errno={}, strerror={}")                        \
  ACTION(FileWrite, 104, "Write file failed, file={}, errno={}, strerror={}")                      \
  ACTION(FileSync, 105, "Sync file failed, file={}, errno={}, strerror={}")

#define LEAN_ERROR_CODE(ename) k##ename

#define LEAN_ERROR_FMT(ename) k##ename##MsgFmt

#define LEAN_DEFINE_ERROR_CODE(ename, evalue, ...) LEAN_ERROR_CODE(ename) = evalue,

#define LEAN_DEFINE_ERROR_BUILDER(ename, ...)                                                      \
  template <typename... Args>                                                                      \
  static Error ename(Args&&... args) {                                                             \
    return Error(Error::Code::LEAN_ERROR_CODE(ename),                                              \
                 std::vformat(LEAN_ERROR_FMT(ename), std::make_format_args(args...)));             \
  }
#define LEAN_DEFINE_ERROR_FMT(ename, evalue, efmt, ...)                                            \
  static const constexpr char* LEAN_ERROR_FMT(ename) = efmt;

/// Representation of an error with code, message, and stack trace if available.
///
/// 1. All the error codes and corresponding message formats are listed in
///    LEAN_ERROR_CODE_LIST macro.
///
/// 2. Errors should be created using the static factory methods. All factory
///    method names are the same as the error code names. Factory method
///    arguments should match the format string parameters in the
///    LEAN_ERROR_CODE_LIST macro.
///
/// 3. Error equality operators are provided for easy comparison of errors. Two
///    errors are considered equal if they have the same error code and message.
///
/// Example usage:
///   auto err1 = Error::General("A general error occurred");
///   auto err2 = Error::FileRead("data.txt", errno, strerror(errno));
///   auto err3 = std::move(err2);
class Error {
public:
  /// Error codes.
  enum Code : int64_t { LEAN_ERROR_CODE_LIST(LEAN_DEFINE_ERROR_CODE) };

  /// Returns the error code.
  Code GetCode() const {
    return code_;
  }

  /// Returns the string representation of the Error, including stack trace if available.
  std::string ToString() const {
    if (stacktrace_.empty()) {
      return std::format("[ERR-{:03}] {}", static_cast<int64_t>(code_), message_);
    }
    return std::format("[ERR-{:03}] {}\n{}", static_cast<int64_t>(code_), message_, stacktrace_);
  }

  /// Stream output operator for Error.
  friend std::ostream& operator<<(std::ostream& os, const Error& error) {
    return os << error.ToString();
  }

  /// Equality operator for Error.
  bool operator==(const Error& other) const {
    return code_ == other.code_ && message_ == other.message_;
  }

  /// Inequality operator for Error.
  bool operator!=(const Error& other) const {
    return !(*this == other);
  }

  /// Factory methods for creating errors for each error code.
  LEAN_ERROR_CODE_LIST(LEAN_DEFINE_ERROR_BUILDER);

private:
  /// Make constructor private to enforce usage of factory methods.
  Error(Code code, std::string&& message)
      : code_(code),
        message_(std::move(message)),
#ifdef DEBUG
        stacktrace_(Stacktrace())
#else
        stacktrace_("")
#endif
  {
  }

  /// Message formats for each error code.
  LEAN_ERROR_CODE_LIST(LEAN_DEFINE_ERROR_FMT);

  Code code_;              // error code.
  std::string message_;    // error message.
  std::string stacktrace_; // stack trace at error creation.
};

#undef LEAN_ERROR_CODE_LIST
#undef LEAN_ERROR_CODE
#undef LEAN_ERROR_FMT
#undef LEAN_DEFINE_ERROR_CODE
#undef LEAN_DEFINE_ERROR_BUILDER
#undef LEAN_DEFINE_ERROR_FMT

} // namespace leanstore