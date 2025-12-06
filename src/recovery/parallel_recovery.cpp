#include "leanstore/cpp/recovery/parallel_recovery.hpp"

#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/recovery/recovery_analyzer.hpp"
#include "leanstore/cpp/recovery/recovery_redoer.hpp"
#include "leanstore/cpp/recovery/recovery_undoer.hpp"

#include <optional>

namespace leanstore {

Optional<Error> ParallelRecovery::Run() {
  // analysis phase
  RecoveryAnalyzer analyzer(recovery_ctx_);
  if (auto err = analyzer.Run()) {
    return err;
  }

  // redo phase
  RecoveryRedoer redoer(recovery_ctx_, analyzer.GetDirtyPageTable());
  if (auto err = redoer.Run()) {
    return err;
  }

  // TODO: update last checkpoint gsn to max observed page version after redo
  // auto max_observed_page_version = analyzer.GetMaxObservedPageVersion();

  // undo phase
  RecoveryUndoer undoer(recovery_ctx_, analyzer.GetActiveTxTable());
  if (auto err = undoer.Run()) {
    return err;
  }

  return std::nullopt;
}

} // namespace leanstore