#pragma once

#include "leanstore/base/result.hpp"
#include "leanstore/recovery/recovery_context.hpp"

namespace leanstore {

class ParallelRecovery {
public:
  explicit ParallelRecovery(RecoveryContext&& recovery_ctx)
      : recovery_ctx_(std::move(recovery_ctx)) {
  }

  ~ParallelRecovery() = default;

  // no copy and assign
  ParallelRecovery& operator=(const ParallelRecovery&) = delete;
  ParallelRecovery(const ParallelRecovery&) = delete;

  /// The entry point for parallel recovery process.
  Result<void> Run();

  Result<void> Undo();

private:
  /// Wal meta information read from persistent storage.
  const RecoveryContext recovery_ctx_;
};

} // namespace leanstore