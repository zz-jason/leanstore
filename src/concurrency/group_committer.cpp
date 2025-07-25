#include "leanstore/concurrency/group_committer.hpp"

#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "utils/coroutine/lean_mutex.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <format>

namespace leanstore::cr {

/// The alignment of the WAL record
constexpr size_t kAligment = 4096;

void GroupCommitter::RunImpl() {
  TXID min_flushed_sys_tx = std::numeric_limits<TXID>::max();
  TXID min_flushed_usr_tx = std::numeric_limits<TXID>::max();
  std::vector<uint64_t> num_rfa_txs(worker_ctxs_.size(), 0);
  std::vector<WalFlushReq> wal_flush_req_copies(worker_ctxs_.size());

  while (keep_running_) {
    // phase 1
    CollectWalRecords(min_flushed_sys_tx, min_flushed_usr_tx, num_rfa_txs, wal_flush_req_copies);

    // phase 2
    if (!aio_.IsEmpty()) {
      FlushWalRecords();
    }

    // phase 3
    DetermineCommitableTx(min_flushed_sys_tx, min_flushed_usr_tx, num_rfa_txs,
                          wal_flush_req_copies);
  }
}

void GroupCommitter::CollectWalRecords(TXID& min_flushed_sys_tx, TXID& min_flushed_usr_tx,
                                       std::vector<uint64_t>& num_rfa_txs,
                                       std::vector<WalFlushReq>& wal_flush_req_copies) {
  min_flushed_sys_tx = std::numeric_limits<TXID>::max();
  min_flushed_usr_tx = std::numeric_limits<TXID>::max();

  for (auto worker_id = 0u; worker_id < worker_ctxs_.size(); worker_id++) {
    auto& logging = worker_ctxs_[worker_id]->logging_;

    // collect logging info
    {
      LEAN_UNIQUE_LOCK(logging.rfa_tx_to_commit_mutex_);
      num_rfa_txs[worker_id] = logging.rfa_tx_to_commit_.size();
    }

    auto last_req_version = wal_flush_req_copies[worker_id].version_;
    auto version = logging.wal_flush_req_.Get(wal_flush_req_copies[worker_id]);
    wal_flush_req_copies[worker_id].version_ = version;
    const auto& req_copy = wal_flush_req_copies[worker_id];

    if (req_copy.version_ == last_req_version) {
      // no transaction log write since last round group commit, skip.
      continue;
    }

    if (req_copy.sys_tx_writtern_ > 0) {
      min_flushed_sys_tx = std::min(min_flushed_sys_tx, req_copy.sys_tx_writtern_);
    }
    if (req_copy.curr_tx_id_ > 0) {
      min_flushed_usr_tx = std::min(min_flushed_usr_tx, req_copy.curr_tx_id_);
    }

    // prepare IOCBs on demand
    const uint64_t buffered = req_copy.wal_buffered_;
    const uint64_t flushed = logging.wal_flushed_;
    const uint64_t buffer_end = store_->store_option_->wal_buffer_size_;
    if (buffered > flushed) {
      Append(logging.wal_buffer_, flushed, buffered);
    } else if (buffered < flushed) {
      Append(logging.wal_buffer_, flushed, buffer_end);
      Append(logging.wal_buffer_, 0, buffered);
    }
  }

  if (!aio_.IsEmpty() && store_->store_option_->enable_wal_fsync_) {
    aio_.PrepareFsync(wal_fd_);
  }
}

void GroupCommitter::FlushWalRecords() {
  // submit all log writes using a single system call.
  if (auto res = aio_.SubmitAll(); !res) {
    Log::Error("Failed to submit all IO, error={}", res.error().ToString());
  }

  /// wait all to finish.
  timespec timeout = {1, 0}; // 1s
  if (auto res = aio_.WaitAll(&timeout); !res) {
    Log::Error("Failed to wait all IO, error={}", res.error().ToString());
  }

  /// sync the metadata in the end.
  if (store_->store_option_->enable_wal_fsync_) {
    auto failed = fdatasync(wal_fd_);
    if (failed) {
      Log::Error("fdatasync failed, errno={}, error={}", errno, strerror(errno));
    }
  }
}

void GroupCommitter::DetermineCommitableTx(TXID min_flushed_sys_tx, TXID min_flushed_usr_tx,
                                           const std::vector<uint64_t>& num_rfa_txs,
                                           const std::vector<WalFlushReq>& wal_flush_req_copies) {
  for (WORKERID worker_id = 0; worker_id < worker_ctxs_.size(); worker_id++) {
    auto& logging = worker_ctxs_[worker_id]->logging_;
    const auto& req_copy = wal_flush_req_copies[worker_id];

    // update the flushed commit TS info
    logging.wal_flushed_.store(req_copy.wal_buffered_, std::memory_order_release);

    // commit transactions with remote dependency
    TXID max_commit_ts = 0;
    {
      LEAN_UNIQUE_LOCK(logging.tx_to_commit_mutex_);
      uint64_t i = 0;
      for (; i < logging.tx_to_commit_.size(); ++i) {
        auto& tx = logging.tx_to_commit_[i];
        if (!tx.CanCommit(min_flushed_sys_tx, min_flushed_usr_tx)) {
          break;
        }
        max_commit_ts = std::max<TXID>(max_commit_ts, tx.commit_ts_);
        tx.state_ = TxState::kCommitted;
        LS_DLOG("Transaction with remote dependency committed, workerId={}, startTs={}, "
                "commitTs={}, minFlushedSysTx={}, minFlushedUsrTx={}",
                worker_id, tx.start_ts_, tx.commit_ts_, min_flushed_sys_tx, min_flushed_usr_tx);
      }
      if (i > 0) {
        logging.tx_to_commit_.erase(logging.tx_to_commit_.begin(),
                                    logging.tx_to_commit_.begin() + i);
      }
    }

    // commit transactions without remote dependency
    TXID max_commit_ts_rfa = 0;
    {
      LEAN_UNIQUE_LOCK(logging.rfa_tx_to_commit_mutex_);
      uint64_t i = 0;
      for (; i < num_rfa_txs[worker_id]; ++i) {
        auto& tx = logging.rfa_tx_to_commit_[i];
        max_commit_ts_rfa = std::max<TXID>(max_commit_ts_rfa, tx.commit_ts_);
        tx.state_ = TxState::kCommitted;
        LS_DLOG("Transaction without remote dependency committed, workerId={}, startTs={}, "
                "commitTs={}",
                worker_id, tx.start_ts_, tx.commit_ts_);
      }
      if (i > 0) {
        logging.rfa_tx_to_commit_.erase(logging.rfa_tx_to_commit_.begin(),
                                        logging.rfa_tx_to_commit_.begin() + i);
      }
    }

    // Has committed transaction
    TXID signaled_up_to = 0;
    if (max_commit_ts == 0 && max_commit_ts_rfa != 0) {
      signaled_up_to = max_commit_ts_rfa;
    } else if (max_commit_ts != 0 && max_commit_ts_rfa == 0) {
      signaled_up_to = max_commit_ts;
    } else if (max_commit_ts != 0 && max_commit_ts_rfa != 0) {
      signaled_up_to = std::min<TXID>(max_commit_ts, max_commit_ts_rfa);
    }
    if (signaled_up_to > 0) {
      logging.UpdateSignaledCommitTs(signaled_up_to);
    }
  }

  global_min_flushed_sys_tx_.store(min_flushed_sys_tx, std::memory_order_release);
}

void GroupCommitter::Append(uint8_t* buf, uint64_t lower, uint64_t upper) {
  auto lower_aligned = utils::AlignDown(lower, kAligment);
  auto upper_aligned = utils::AlignUp(upper, kAligment);
  auto* buf_aligned = buf + lower_aligned;
  auto count_aligned = upper_aligned - lower_aligned;
  auto offset_aligned = utils::AlignDown(wal_size_, kAligment);

  aio_.PrepareWrite(wal_fd_, buf_aligned, count_aligned, offset_aligned);
  wal_size_ += upper - lower;
};

} // namespace leanstore::cr