#include "leanstore/concurrency/Logging.hpp"

#include "leanstore/concurrency/WalEntry.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"
#include "leanstore/utils/Log.hpp"
#include "utils/ToJson.hpp"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstring>

namespace leanstore::cr {

void Logging::WriteWalTxAbort() {
  // Initialize a WalTxAbort
  auto size = sizeof(WalTxAbort);
  auto* data = mWalBuffer.Get(size);
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxAbort(size);

  mWalBuffer.Advance(size);
  LS_DLOG("WriteWalTxAbort, workerId={}, startTs={}, walJson={}", WorkerContext::My().mWorkerId,
          WorkerContext::My().mActiveTx.mStartTs, utils::ToJsonString(entry));
}

void Logging::WriteWalTxFinish() {
  // Reserve space
  auto size = sizeof(WalTxFinish);
  auto* data = mWalBuffer.Get(size);
  std::memset(data, 0, size);
  auto* entry [[maybe_unused]] = new (data) WalTxFinish(WorkerContext::My().mActiveTx.mStartTs);

  mWalBuffer.Advance(size);
  LS_DLOG("WriteWalTxFinish, workerId={}, startTs={}, walJson={}", WorkerContext::My().mWorkerId,
          WorkerContext::My().mActiveTx.mStartTs, utils::ToJsonString(entry));
}

} // namespace leanstore::cr
