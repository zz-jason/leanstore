#include "leanstore/recovery/parallel_recovery.hpp"

#include "leanstore/recovery/recovery_analyzer.hpp"
#include "leanstore/recovery/recovery_redoer.hpp"
#include "leanstore/recovery/recovery_undoer.hpp"

namespace leanstore {

Result<void> ParallelRecovery::Run() {
  // analysis phase
  RecoveryAnalyzer analyzer(recovery_ctx_);
  if (auto res = analyzer.Run(); !res) {
    return res;
  }

  // redo phase
  RecoveryRedoer redoer(recovery_ctx_.GetStoreDir(), recovery_ctx_.GetWalFilePaths(),
                        analyzer.GetDirtyPageTable(), recovery_ctx_.GetNumPartitions(),
                        recovery_ctx_.GetBufferManager());
  if (auto res = redoer.Run(); !res) {
    return res;
  }

  // TODO: update last checkpoint gsn to max observed page version after redo
  // auto max_observed_page_version = analyzer.GetMaxObservedPageVersion();

  // undo phase
  RecoveryUndoer undoer(analyzer.GetActiveTxTable());
  if (auto res = undoer.Run(); !res) {
    return res;
  }

  return {};
}

} // namespace leanstore