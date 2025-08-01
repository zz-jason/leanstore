#include "utils/coroutine/auto_commit_protocol.hpp"

#include "leanstore/concurrency/logging.hpp"
#include "leanstore/concurrency/worker_context.hpp"

namespace leanstore {

bool AutoCommitProtocol::LogFlush() {
  return cr::WorkerContext::My().GetLogging().CoroFlush();
}

} // namespace leanstore