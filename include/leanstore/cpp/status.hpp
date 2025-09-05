#ifndef LEANSTORE_CPP_STATUS_HPP
#define LEANSTORE_CPP_STATUS_HPP

#include "leanstore/common/status.h"

namespace leanstore {

/// C++ wrapper for lean_status with convenient methods
class Status {
public:
  /// Default constructor (OK status)
  Status() : code_(LEAN_STATUS_OK) {
  }

  /// Constructor from lean_status
  Status(lean_status code) : code_(code) {
  }

  /// Copy constructor
  Status(const Status& other) = default;

  /// Assignment operator
  Status& operator=(const Status& other) = default;

  /// Assignment from lean_status
  Status& operator=(lean_status code) {
    code_ = code;
    return *this;
  }

  /// Get the underlying lean_status code
  lean_status Code() const {
    return code_;
  }

  /// Convert to lean_status for C interface compatibility
  operator lean_status() const {
    return code_;
  }

  /// Check if status indicates success
  bool IsOk() const {
    return code_ == LEAN_STATUS_OK;
  }

  /// Check if status indicates not found
  bool IsNotFound() const {
    return code_ == LEAN_ERR_NOT_FOUND;
  }

  /// Check if status indicates duplicate key
  bool IsDuplicate() const {
    return code_ == LEAN_ERR_DUPLICATED;
  }

  /// Check if status indicates transaction conflict
  bool IsConflict() const {
    return code_ == LEAN_ERR_CONFLICT;
  }

  /// Check if status indicates store open error
  bool IsOpenStoreError() const {
    return code_ == LEAN_ERR_OPEN_STORE;
  }

  /// Check if status indicates create btree error
  bool IsCreateBTreeError() const {
    return code_ == LEAN_ERR_CTEATE_BTREE;
  }

  /// Check if status indicates unsupported operation
  bool IsUnsupported() const {
    return code_ == LEAN_ERR_UNSUPPORTED;
  }

  /// Convert status code to human-readable string
  const char* ToString() const;

  /// Equality comparison with Status
  bool operator==(const Status& other) const {
    return code_ == other.code_;
  }

  /// Inequality comparison with Status
  bool operator!=(const Status& other) const {
    return code_ != other.code_;
  }

  /// Equality comparison with lean_status
  bool operator==(lean_status code) const {
    return code_ == code;
  }

  /// Inequality comparison with lean_status
  bool operator!=(lean_status code) const {
    return code_ != code;
  }

  /// Boolean conversion (true if OK)
  explicit operator bool() const {
    return IsOk();
  }

  /// Static factory methods for common statuses
  static Status Ok() {
    return Status(LEAN_STATUS_OK);
  }
  static Status NotFound() {
    return Status(LEAN_ERR_NOT_FOUND);
  }
  static Status Duplicate() {
    return Status(LEAN_ERR_DUPLICATED);
  }
  static Status Conflict() {
    return Status(LEAN_ERR_CONFLICT);
  }
  static Status OpenStoreError() {
    return Status(LEAN_ERR_OPEN_STORE);
  }
  static Status CreateBTreeError() {
    return Status(LEAN_ERR_CTEATE_BTREE);
  }
  static Status Unsupported() {
    return Status(LEAN_ERR_UNSUPPORTED);
  }

private:
  lean_status code_;
};

/// Global comparison operators for lean_status with Status
inline bool operator==(lean_status code, const Status& status) {
  return status == code;
}

inline bool operator!=(lean_status code, const Status& status) {
  return status != code;
}

} // namespace leanstore

#endif // LEANSTORE_CPP_STATUS_HPP