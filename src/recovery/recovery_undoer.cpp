#include "leanstore/cpp/recovery/recovery_undoer.hpp"

namespace leanstore {

Optional<Error> RecoveryUndoer::Run() {
  if (active_tx_table_.empty()) {
    return std::nullopt; // Nothing to undo
  }

  // TODO: implement undo logic
  return Error::NotImplemented("Recovery undo is not implemented");
}

} // namespace leanstore