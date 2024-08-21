#ifndef LEANSTORE_STORE_OPTION_H
#define LEANSTORE_STORE_OPTION_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//! The log level
typedef enum LogLevel {
  kDebug = 0,
  kInfo,
  kWarn,
  kError,
} LogLevel;

//! The options for creating a new store.
typedef struct StoreOption {
  // ---------------------------------------------------------------------------
  // Store related options
  // ---------------------------------------------------------------------------

  //! Whether to create store from scratch.
  bool mCreateFromScratch;

  //! The directory for all the database files.
  const char* mStoreDir;

  // ---------------------------------------------------------------------------
  // log related options
  // ---------------------------------------------------------------------------

  //! The log level
  LogLevel mLogLevel;

  // ---------------------------------------------------------------------------
  // WorkerContext thread related options
  // ---------------------------------------------------------------------------

  //! The number of worker threads.
  uint64_t mWorkerThreads;

  //! The WAL buffer size for each worker (bytes).
  uint64_t mWalBufferSize;

  // ---------------------------------------------------------------------------
  // Buffer pool related options
  // ---------------------------------------------------------------------------

  //! The page size (bytes). For buffer manager.
  uint64_t mPageSize;

  uint64_t mBufferFrameSize;

  //! The number of partitions. For buffer manager.
  uint32_t mNumPartitions;

  //! The buffer pool size (bytes). For buffer manager.
  uint64_t mBufferPoolSize;

  //! The free percentage of the buffer pool. In the range of [0, 100].
  uint32_t mFreePct;

  //! The number of page evictor threads.
  uint32_t mNumBufferProviders;

  //! The async buffer
  uint32_t mBufferWriteBatchSize;

  //! Whether to perform crc check for buffer frames.
  bool mEnableBufferCrcCheck;

  //! BufferFrame recycle batch size. Everytime a batch of buffer frames is
  //! randomly picked and verified by page evictors, some of them are COOLed,
  //! some of them are EVICted.
  uint64_t mBufferFrameRecycleBatchSize;

  //! Whether to reclaim unused free page ids
  bool mEnableReclaimPageIds;

  // ---------------------------------------------------------------------------
  // Logging and recovery related options
  // ---------------------------------------------------------------------------

  //! Whether to enable write-ahead log.
  bool mEnableWal;

  //! Whether to execute fsync after each WAL write.
  bool mEnableWalFsync;

  // ---------------------------------------------------------------------------
  // Generic BTree related options
  // ---------------------------------------------------------------------------

  //! Whether to enable bulk insert.
  bool mEnableBulkInsert;

  //! Whether to enable X-Merge
  bool mEnableXMerge;

  //! The number of children to merge in X-Merge
  uint64_t mXMergeK;

  //! The target percentage of the number of children to merge in X-Merge
  double mXMergeTargetPct;

  //! Whether to enable contention split.
  bool mEnableContentionSplit;

  //! Contention split probability, as exponent of 2
  uint64_t mContentionSplitProbility;

  //! Contention stats sample probability, as exponent of 2
  uint64_t mContentionSplitSampleProbability;

  //! Contention percentage to trigger the split, in the range of [0, 100].
  uint64_t mContentionSplitThresholdPct;

  //! Whether to enable btree hints optimization. Available options:
  //! 0: disabled
  //! 1: serial
  //! 2: AVX512
  int64_t mBTreeHints;

  //! Whether to enable heads optimization in LowerBound search.
  bool mEnableHeadOptimization;

  //! Whether to enable optimistic scan. Jump to next leaf directly if the
  //! pointer in the parent has not changed
  bool mEnableOptimisticScan;

  // ---------------------------------------------------------------------------
  // Transaction related options
  // ---------------------------------------------------------------------------

  //! Whether to enable long running transaction.
  bool mEnableLongRunningTx;

  //! Whether to enable fat tuple.
  bool mEnableFatTuple;

  //! Whether to enable garbage collection.
  bool mEnableGc;

  //! Whether to enable eager garbage collection. To enable eager garbage
  //! collection, the garbage collection must be enabled first. Once enabled,
  //! the garbage collection will be triggered after each transaction commit and
  //! abort.
  bool mEnableEagerGc;

  // ---------------------------------------------------------------------------
  // Metrics related options
  // ---------------------------------------------------------------------------

  //! Whether to enable metrics.
  bool mEnableMetrics;

  //! The metrics port.
  int32_t mMetricsPort;

  //! Whether to enable cpu counters.
  bool mEnableCpuCounters;

  //! Whether to enable time measure.
  bool mEnableTimeMeasure;

  //! Whether to enable perf events.
  bool mEnablePerfEvents;

} StoreOption;

//! Create a new store option.
//! @param storeDir the directory for all the database files. The string content is deep copied to
//!                 the created store option.
StoreOption* CreateStoreOption(const char* storeDir);

//! Create a new store option from an existing store option.
//! @param storeDir the existing store option.
StoreOption* CreateStoreOptionFrom(const StoreOption* storeDir);

//! Destroy a store option.
//! @param option the store option to destroy.
void DestroyStoreOption(const StoreOption* option);

//! The options for creating a new BTree.
typedef struct BTreeConfig {
  //! Whether to enable write-ahead log.
  bool mEnableWal;

  //! Whether to enable bulk insert.
  bool mUseBulkInsert;

} BTreeConfig;

#ifdef __cplusplus
}
#endif

#endif // LEANSTORE_STORE_OPTION_H