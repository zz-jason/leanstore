#include "leanstore/cpp/recovery/recovery_redoer.hpp"

namespace leanstore {

Optional<Error> RecoveryRedoer::Run() {
  if (dirty_page_table_.empty()) {
    return std::nullopt; // Nothing to redo
  }

  return Error::NotImplemented("Parallel recovery redo is not implemented");
}

} // namespace leanstore