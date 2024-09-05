#include "leanstore/concurrency/GroupCommitter.hpp"

#include "leanstore/concurrency/CRManager.hpp"
#include "leanstore/concurrency/WorkerContext.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <format>

namespace leanstore::cr {

//! The alignment of the WAL record
constexpr size_t kAligment = 4096;

void GroupCommitter::runImpl() {
  TXID minFlushedSysTx = std::numeric_limits<TXID>::max();
  TXID minFlushedUsrTx = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> numRfaTxs(mWorkerCtxs.size(), 0);
  std::vector<WalFlushReq> walFlushReqCopies(mWorkerCtxs.size());

  while (mKeepRunning) {
    // phase 1
    collectWalRecords(minFlushedSysTx, minFlushedUsrTx, numRfaTxs, walFlushReqCopies);

    // phase 2
    if (!mAIo.IsEmpty()) {
      flushWalRecords();
    }

    // phase 3
    determineCommitableTx(minFlushedSysTx, minFlushedUsrTx, numRfaTxs, walFlushReqCopies);
  }
}

void GroupCommitter::collectWalRecords(TXID& minFlushedSysTx, TXID& minFlushedUsrTx,
                                       std::vector<uint64_t>& numRfaTxs,
                                       std::vector<WalFlushReq>& walFlushReqCopies) {
  minFlushedSysTx = std::numeric_limits<TXID>::max();
  minFlushedUsrTx = std::numeric_limits<TXID>::max();

  for (auto workerId = 0u; workerId < mWorkerCtxs.size(); workerId++) {
    auto& logging = mWorkerCtxs[workerId]->mLogging;

    // collect logging info
    std::unique_lock<std::mutex> guard(logging.mRfaTxToCommitMutex);
    numRfaTxs[workerId] = logging.mRfaTxToCommit.size();
    guard.unlock();

    auto lastReqVersion = walFlushReqCopies[workerId].mVersion;
    auto version = logging.mWalFlushReq.Get(walFlushReqCopies[workerId]);
    walFlushReqCopies[workerId].mVersion = version;
    const auto& reqCopy = walFlushReqCopies[workerId];

    if (reqCopy.mVersion == lastReqVersion) {
      // no transaction log write since last round group commit, skip.
      continue;
    }

    if (reqCopy.mSysTxWrittern > 0) {
      minFlushedSysTx = std::min(minFlushedSysTx, reqCopy.mSysTxWrittern);
    }
    if (reqCopy.mCurrTxId > 0) {
      minFlushedUsrTx = std::min(minFlushedUsrTx, reqCopy.mCurrTxId);
    }

    // prepare IOCBs on demand
    const uint64_t buffered = reqCopy.mWalBuffered;
    const uint64_t flushed = logging.mWalFlushed;
    const uint64_t bufferEnd = mStore->mStoreOption->mWalBufferSize;
    if (buffered > flushed) {
      append(logging.mWalBuffer, flushed, buffered);
    } else if (buffered < flushed) {
      append(logging.mWalBuffer, flushed, bufferEnd);
      append(logging.mWalBuffer, 0, buffered);
    }
  }

  if (!mAIo.IsEmpty() && mStore->mStoreOption->mEnableWalFsync) {
    mAIo.PrepareFsync(mWalFd);
  }
}

void GroupCommitter::flushWalRecords() {
  // submit all log writes using a single system call.
  if (auto res = mAIo.SubmitAll(); !res) {
    Log::Error("Failed to submit all IO, error={}", res.error().ToString());
  }

  //! wait all to finish.
  timespec timeout = {1, 0}; // 1s
  if (auto res = mAIo.WaitAll(&timeout); !res) {
    Log::Error("Failed to wait all IO, error={}", res.error().ToString());
  }

  //! sync the metadata in the end.
  if (mStore->mStoreOption->mEnableWalFsync) {
    auto failed = fdatasync(mWalFd);
    if (failed) {
      Log::Error("fdatasync failed, errno={}, error={}", errno, strerror(errno));
    }
  }
}

void GroupCommitter::determineCommitableTx(TXID minFlushedSysTx, TXID minFlushedUsrTx,
                                           const std::vector<uint64_t>& numRfaTxs,
                                           const std::vector<WalFlushReq>& walFlushReqCopies) {
  for (WORKERID workerId = 0; workerId < mWorkerCtxs.size(); workerId++) {
    auto& logging = mWorkerCtxs[workerId]->mLogging;
    const auto& reqCopy = walFlushReqCopies[workerId];

    // update the flushed commit TS info
    logging.mWalFlushed.store(reqCopy.mWalBuffered, std::memory_order_release);

    // commit transactions with remote dependency
    TXID maxCommitTs = 0;
    {
      std::unique_lock<std::mutex> g(logging.mTxToCommitMutex);
      uint64_t i = 0;
      for (; i < logging.mTxToCommit.size(); ++i) {
        auto& tx = logging.mTxToCommit[i];
        if (!tx.CanCommit(minFlushedSysTx, minFlushedUsrTx)) {
          break;
        }
        maxCommitTs = std::max<TXID>(maxCommitTs, tx.mCommitTs);
        tx.mState = TxState::kCommitted;
        LS_DLOG("Transaction with remote dependency committed, workerId={}, startTs={}, "
                "commitTs={}, minFlushedSysTx={}, minFlushedUsrTx={}",
                workerId, tx.mStartTs, tx.mCommitTs, minFlushedSysTx, minFlushedUsrTx);
      }
      if (i > 0) {
        logging.mTxToCommit.erase(logging.mTxToCommit.begin(), logging.mTxToCommit.begin() + i);
      }
    }

    // commit transactions without remote dependency
    TXID maxCommitTsRfa = 0;
    {
      std::unique_lock<std::mutex> g(logging.mRfaTxToCommitMutex);
      uint64_t i = 0;
      for (; i < numRfaTxs[workerId]; ++i) {
        auto& tx = logging.mRfaTxToCommit[i];
        maxCommitTsRfa = std::max<TXID>(maxCommitTsRfa, tx.mCommitTs);
        tx.mState = TxState::kCommitted;
        LS_DLOG("Transaction without remote dependency committed, workerId={}, startTs={}, "
                "commitTs={}",
                workerId, tx.mStartTs, tx.mCommitTs);
      }
      if (i > 0) {
        logging.mRfaTxToCommit.erase(logging.mRfaTxToCommit.begin(),
                                     logging.mRfaTxToCommit.begin() + i);
      }
    }

    // Has committed transaction
    TXID signaledUpTo = 0;
    if (maxCommitTs == 0 && maxCommitTsRfa != 0) {
      signaledUpTo = maxCommitTsRfa;
    } else if (maxCommitTs != 0 && maxCommitTsRfa == 0) {
      signaledUpTo = maxCommitTs;
    } else if (maxCommitTs != 0 && maxCommitTsRfa != 0) {
      signaledUpTo = std::min<TXID>(maxCommitTs, maxCommitTsRfa);
    }
    if (signaledUpTo > 0) {
      logging.UpdateSignaledCommitTs(signaledUpTo);
    }
  }

  mGlobalMinFlushedSysTx.store(minFlushedSysTx, std::memory_order_release);
}

void GroupCommitter::append(uint8_t* buf, uint64_t lower, uint64_t upper) {
  auto lowerAligned = utils::AlignDown(lower, kAligment);
  auto upperAligned = utils::AlignUp(upper, kAligment);
  auto* bufAligned = buf + lowerAligned;
  auto countAligned = upperAligned - lowerAligned;
  auto offsetAligned = utils::AlignDown(mWalSize, kAligment);

  mAIo.PrepareWrite(mWalFd, bufAligned, countAligned, offsetAligned);
  mWalSize += upper - lower;
};

} // namespace leanstore::cr