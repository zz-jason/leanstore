#include "leanstore/cpp/recovery/recovery_redoer.hpp"

#include "leanstore/cpp/base/error.hpp"
#include "leanstore/cpp/base/optional.hpp"

#include <optional>

namespace leanstore {

Optional<Error> RecoveryRedoer::Run() {
  if (dirty_page_table_.empty()) {
    return std::nullopt;
  }

  auto partitioned_logs = PartitionWalLogs();
  if (!partitioned_logs) {
    return std::move(partitioned_logs.error());
  }

  for (auto& partitioned_log : partitioned_logs.value()) {
    if (auto err = RedoPartition(partitioned_log); err) {
      return err;
    }
  }

  return std::nullopt;
}

Result<std::vector<std::string>> RecoveryRedoer::PartitionWalLogs() {
  return Error::NotImplemented("PartitionWalLogs is not implemented");
}

Optional<Error> RecoveryRedoer::RedoPartition(std::string_view partitioned_log) {
  return Error::NotImplemented("RedoPartition is not implemented: " + std::string(partitioned_log));
}

} // namespace leanstore