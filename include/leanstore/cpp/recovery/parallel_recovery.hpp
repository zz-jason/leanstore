#pragma once

#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"
#include "leanstore/cpp/recovery/recovery_context.hpp"

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
  Optional<Error> Run();

  Optional<Error> Undo();

private:
  /// Wal meta information read from persistent storage.
  const RecoveryContext recovery_ctx_;
};

} // namespace leanstore