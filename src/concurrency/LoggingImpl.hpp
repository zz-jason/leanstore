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
  // write transaction start on demand
  if (!ActiveTx().mHasWrote) {
    WriteSimpleWal(WalEntry::Type::kTxStart);
    ActiveTx().mHasWrote = true;
  }

  // update prev lsn in the end
  SCOPED_DEFER(mPrevLSN = mActiveWALEntryComplex->mLsn);

  auto entryLSN = mLsnClock++;
  auto* entryPtr = mWalBuffer + mWalBuffered;
  auto entrySize = sizeof(WalEntryComplex) + payloadSize;
  ReserveContiguousBuffer(entrySize);

  mActiveWALEntryComplex =
      new (entryPtr) WalEntryComplex(entryLSN, entrySize, psn, treeId, pageId);
  mActiveWALEntryComplex->mPrevLSN = mPrevLSN;
  auto& curWorker = leanstore::cr::Worker::My();
  mActiveWALEntryComplex->InitTxInfo(&curWorker.mActiveTx, curWorker.mWorkerId);

  auto* payloadPtr = mActiveWALEntryComplex->mPayload;
  auto walPayload = new (payloadPtr) T(std::forward<Args>(args)...);
  return {walPayload, entrySize};
}

} // namespace leanstore::cr