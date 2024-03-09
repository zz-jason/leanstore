#include "concurrency/Logging.hpp"
#include "concurrency/WalEntry.hpp"
#include "concurrency/WalPayloadHandler.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Units.hpp"
#include "utils/Defer.hpp"

namespace leanstore::cr {

template <typename T, typename... Args>
WalPayloadHandler<T> Logging::ReserveWALEntryComplex(uint64_t payloadSize,
                                                     PID pageId, LID psn,
                                                     TREEID treeId,
                                                     Args&&... args) {
  SCOPED_DEFER(mPrevLSN = mActiveWALEntryComplex->mLsn);

  auto entryLSN = mLsnClock++;
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WALEntryComplex) + payloadSize;
  ReserveContiguousBuffer(entrySize);

  mActiveWALEntryComplex =
      new (entryPtr) WALEntryComplex(entryLSN, entrySize, psn, treeId, pageId);
  mActiveWALEntryComplex->mPrevLSN = mPrevLSN;
  auto& curWorker = leanstore::cr::Worker::My();
  mActiveWALEntryComplex->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);

  auto* payloadPtr = mActiveWALEntryComplex->mPayload;
  auto walPayload = new (payloadPtr) T(std::forward<Args>(args)...);
  return {walPayload, entrySize};
}

} // namespace leanstore::cr