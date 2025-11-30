#pragma once

#include "leanstore/common/types.h"
#include "leanstore/common/wal_record.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/base/result.hpp"

#include <cassert>
#include <cstring>
#include <functional>
#include <memory>
#include <string_view>

namespace leanstore {

/// Abstract WAL cursor interface.
class WalCursor {
public:
  /// Create a new WAL cursor for the given WAL file path.
  static Result<std::unique_ptr<WalCursor>> New(std::string_view wal_path);

  /// Default constructor.
  WalCursor() = default;

  /// Destructor.
  virtual ~WalCursor() = default;

  /// Seek to the given LSN.
  virtual Optional<Error> Seek(lean_lid_t lsn) = 0;

  /// Advance to the next WAL record. Error is returned if any issue occurs.
  virtual Optional<Error> Next() = 0;

  /// Check if the cursor is valid. Cursor becomes invalid when reaching the end
  /// of WAL or any error occurs.
  virtual bool Valid() const = 0;

  /// Get the current WAL record referenced by the cursor.
  virtual const lean_wal_record& CurrentRecord() const = 0;

  /// Apply a custom function to each WAL record in the cursor.
  Optional<Error> Foreach(const std::function<bool(lean_wal_record& record)>& func);
};

} // namespace leanstore