#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/c/types.h"
#include "leanstore/c/wal_record.h"

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

/// Abstract WAL cursor interface.
class WalCursor {
public:
  /// Create a new WAL cursor for the given WAL file.
  static Result<std::unique_ptr<WalCursor>> New(std::string_view wal_path);

  /// Create WAL cursors for each WAL file.
  static Result<std::vector<std::unique_ptr<WalCursor>>> NewWalCursors(
      const std::vector<std::string>& wal_file_paths);

  /// Default constructor.
  WalCursor() = default;

  /// Destructor.
  virtual ~WalCursor() = default;

  /// Seek to the given LSN.
  virtual Result<void> Seek(lean_lid_t lsn) = 0;

  /// Advance to the next WAL record. Error is returned if any issue occurs.
  virtual Result<void> Next() = 0;

  /// Check if the cursor is valid. Cursor becomes invalid when reaching the end
  /// of WAL or any error occurs.
  virtual bool Valid() const = 0;

  /// Get the current WAL record referenced by the cursor.
  virtual const lean_wal_record& CurrentRecord() const = 0;

  /// Apply a custom function to each WAL record in the cursor.
  /// The function signature should be: Result<bool> func(lean_wal_record& record)
  /// The iteration stops when the function returns false or an error.
  template <typename F>
    requires std::is_invocable_r_v<Result<bool>, F, lean_wal_record&>
  Result<void> Foreach(F&& func);
};

template <typename F>
  requires std::is_invocable_r_v<Result<bool>, F, lean_wal_record&>
Result<void> WalCursor::Foreach(F&& func) {
  // Seek to the begining.
  if (auto res = Seek(0); !res) {
    return res;
  }

  // Iterate through all WAL records.
  while (Valid()) {
    // Apply the custom function for the current record.
    auto result = func(const_cast<lean_wal_record&>(CurrentRecord()));

    // Stop iteration and propagate the error.
    if (!result) {
      return std::move(result.error());
    }

    // Stop iteration as requested by the function.
    if (!result.value()) {
      return {};
    }

    // Advance to the next record.
    if (auto res = Next(); !res) {
      return res;
    }
  }

  return {};
}

} // namespace leanstore