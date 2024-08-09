#pragma once

#include <cstdint>
#include <string>

namespace leanstore {

enum class LogLevel : uint8_t {
  kDebug = 0,
  kInfo,
  kWarn,
  kError,
};

class StoreOption {
public:
  // ---------------------------------------------------------------------------
  // Store related options
  // ---------------------------------------------------------------------------

  //! Whether to create store from scratch.
  bool mCreateFromScratch = true;

  //! The directory for all the database files.
  std::string mStoreDir = "~/.leanstore";

  // ---------------------------------------------------------------------------
  // log related options
  // ---------------------------------------------------------------------------

  //! The log level
  LogLevel mLogLevel = LogLevel::kInfo;

  // ---------------------------------------------------------------------------
  // Worker thread related options
  // ---------------------------------------------------------------------------

  //! The number of worker threads.
  uint64_t mWorkerThreads = 4;

  //! The WAL buffer size for each worker (bytes).
  uint64_t mWalBufferSize = 10 * 1024 * 1024;

  // ---------------------------------------------------------------------------
  // Buffer pool related options
  // ---------------------------------------------------------------------------

  //! The page size (bytes). For buffer manager.
  uint64_t mPageSize = 4 * 1024;

  uint64_t mBufferFrameSize = 512 + mPageSize;

  //! The number of partitions. For buffer manager.
  uint32_t mNumPartitions = 64;

  //! The buffer pool size (bytes). For buffer manager.
  uint64_t mBufferPoolSize = 1ull * 1024 * 1024 * 1024;

  //! The free percentage of the buffer pool. In the range of [0, 100].
  uint32_t mFreePct = 1;

  //! The number of page evictor threads.
  uint32_t mNumBufferProviders = 1;

  //! The async buffer
  uint32_t mBufferWriteBatchSize = 1024;

  //! Whether to perform crc check for buffer frames.
  bool mEnableBufferCrcCheck = false;

  //! BufferFrame recycle batch size. Everytime a batch of buffer frames is
  //! randomly picked and verified by page evictors, some of them are COOLed,
  //! some of them are EVICted.
  uint64_t mBufferFrameRecycleBatchSize = 64;

  //! Whether to reclaim unused free page ids
  bool mEnableReclaimPageIds = true;

  // ---------------------------------------------------------------------------
  // Logging and recovery related options
  // ---------------------------------------------------------------------------

  //! Whether to enable write-ahead log.
  bool mEnableWal = true;

  //! Whether to execute fsync after each WAL write.
  bool mEnableWalFsync = false;

  // ---------------------------------------------------------------------------
  // Generic BTree related options
  // ---------------------------------------------------------------------------

  bool mEnableBulkInsert = false;

  //! Whether to enable X-Merge
  bool mEnableXMerge = false;

  uint64_t mXMergeK = 5;

  double mXMergeTargetPct = 80;

  //! Whether to enable contention split.
  bool mEnableContentionSplit = true;

  //! Contention split probability, as exponent of 2
  uint64_t mContentionSplitProbility = 14;

  //! Contention stats sample probability, as exponent of 2
  uint64_t mContentionSplitSampleProbability = 7;

  //! Contention percentage to trigger the split, in the range of [0, 100].
  uint64_t mContentionSplitThresholdPct = 1;

  //! Whether to enable btree hints optimization. Available options:
  //! 0: disabled
  //! 1: serial
  //! 2: AVX512
  int64_t mBTreeHints = 1;

  //! Whether to enable heads optimization in LowerBound search.
  bool mEnableHeadOptimization = true;

  //! Whether to enable optimistic scan. Jump to next leaf directly if the
  //! pointer in the parent has not changed
  bool mEnableOptimisticScan = true;

  // ---------------------------------------------------------------------------
  // Transaction related options
  // ---------------------------------------------------------------------------

  //! Whether to enable long running transaction.
  bool mEnableLongRunningTx = true;

  //! Whether to enable fat tuple.
  bool mEnableFatTuple = false;

  //! Whether to enable garbage collection.
  bool mEnableGc = true;

  //! Whether to enable eager garbage collection. To enable eager garbage
  //! collection, the garbage collection must be enabled first. Once enabled,
  //! the garbage collection will be triggered after each transaction commit and
  //! abort.
  bool mEnableEagerGc = false;

  // ---------------------------------------------------------------------------
  // Metrics related options
  // ---------------------------------------------------------------------------

  //! Whether to enable metrics.
  bool mEnableMetrics = false;

  //! The metrics port.
  int32_t mMetricsPort = 8080;

  bool mEnableCpuCounters = true;

  bool mEnableTimeMeasure = false;

  bool mEnablePerfEvents = false;

  //! Helper functions
  std::string GetMetaFilePath() const {
    return mStoreDir + "/db.meta.json";
  }

  std::string GetDbFilePath() const {
    return mStoreDir + "/db.pages";
  }

  std::string GetWalFilePath() const {
    return mStoreDir + "/db.wal";
  }

  std::string GetLogPath() const {
    return mStoreDir + "/db.log";
  }
};

class BTreeConfig {
public:
  bool mEnableWal = true;
  bool mUseBulkInsert = false;
};

} // namespace leanstore