#include "leanstore/recovery/recovery_undoer.hpp"

namespace leanstore {

Result<void> RecoveryUndoer::Run() {
  if (active_tx_table_.empty()) {
    return {}; // Nothing to undo
  }

  // TODO: implement undo logic
  return Error::NotImplemented("Recovery undo is not implemented");
}

} // namespace leanstore