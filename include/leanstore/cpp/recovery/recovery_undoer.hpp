#pragma once

#include "leanstore/common/types.h"
#include "leanstore/cpp/base/result.hpp"

#include <unordered_map>

namespace leanstore {

class RecoveryUndoer {
public:
  explicit RecoveryUndoer(const std::unordered_map<lean_txid_t, lean_lid_t>& active_tx_table)
      : active_tx_table_(active_tx_table) {
  }

  ~RecoveryUndoer() = default;

  // no copy and assign
  RecoveryUndoer& operator=(const RecoveryUndoer&) = delete;
  RecoveryUndoer(const RecoveryUndoer&) = delete;

  /// The entry point for undo process.
  Result<void> Run();

private:
  const std::unordered_map<lean_txid_t, lean_lid_t>& active_tx_table_;
};

} // namespace leanstore