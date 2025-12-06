#pragma once

#include "leanstore/common/types.h"
#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/recovery/recovery_context.hpp"

#include <unordered_map>

namespace leanstore {

/// WAL records are distributed across multiple files, the parallel
/// partitioned redo process works as follows:
///
/// - Redistribute WAL records by page ID, so that all WAL records of the same
///   page are processed by the same worker thread. Note: only the WAL records
///   of dirty pages are redistributed, other WAL records are ignored.
///
/// - Start N worker threads to read the redistributed WAL records and redo
///   them to the corresponding pages.
///
class RecoveryRedoer {
public:
  RecoveryRedoer(const RecoveryContext& recovery_ctx,
                 const std::unordered_map<lean_pid_t, lean_lid_t>& dirty_page_table)
      : recovery_ctx_(recovery_ctx),
        dirty_page_table_(dirty_page_table) {
  }

  ~RecoveryRedoer() = default;

  // no copy and assign
  RecoveryRedoer& operator=(const RecoveryRedoer&) = delete;
  RecoveryRedoer(const RecoveryRedoer&) = delete;

  /// The entry point for redo process.
  Optional<Error> Run();

private:
  const RecoveryContext& recovery_ctx_;
  const std::unordered_map<lean_pid_t, lean_lid_t>& dirty_page_table_;
};

} // namespace leanstore