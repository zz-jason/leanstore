#pragma once

#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/recovery/recovery_context.hpp"

#include <unordered_map>

namespace leanstore {

class RecoveryUndoer {
public:
  RecoveryUndoer(const RecoveryContext& recovery_ctx,
                 const std::unordered_map<lean_txid_t, lean_lid_t>& active_tx_table)
      : recovery_ctx_(recovery_ctx),
        active_tx_table_(active_tx_table) {
  }

  ~RecoveryUndoer() = default;

  // no copy and assign
  RecoveryUndoer& operator=(const RecoveryUndoer&) = delete;
  RecoveryUndoer(const RecoveryUndoer&) = delete;

  /// The entry point for undo process.
  Optional<Error> Run();

private:
  const RecoveryContext& recovery_ctx_;
  const std::unordered_map<lean_txid_t, lean_lid_t>& active_tx_table_;
};

} // namespace leanstore