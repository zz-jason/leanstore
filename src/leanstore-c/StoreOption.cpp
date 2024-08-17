#include "leanstore-c/StoreOption.h"

#include <string.h>

//! The default store option.
static const StoreOption kDefaultStoreOption = {
    // Store related options
    .mCreateFromScratch = true,
    .mStoreDir = "~/.leanstore",

    // log related options
    .mLogLevel = LogLevel::kInfo,

    // Worker thread related options
    .mWorkerThreads = 4,
    .mWalBufferSize = 10 * 1024 * 1024,

    // Buffer pool related options
    .mPageSize = 4 * 1024,
    .mBufferFrameSize = 512 + 4 * 1024,
    .mNumPartitions = 64,
    .mBufferPoolSize = 1ull * 1024 * 1024 * 1024,
    .mFreePct = 1,
    .mNumBufferProviders = 1,
    .mBufferWriteBatchSize = 1024,
    .mEnableBufferCrcCheck = false,
    .mBufferFrameRecycleBatchSize = 64,
    .mEnableReclaimPageIds = true,

    // Logging and recovery related options
    .mEnableWal = true,
    .mEnableWalFsync = false,

    // Generic BTree related options
    .mEnableBulkInsert = false,
    .mEnableXMerge = false,
    .mXMergeK = 5,
    .mXMergeTargetPct = 80,
    .mEnableContentionSplit = true,
    .mContentionSplitProbility = 14,
    .mContentionSplitSampleProbability = 7,
    .mContentionSplitThresholdPct = 1,
    .mBTreeHints = 1,
    .mEnableHeadOptimization = true,
    .mEnableOptimisticScan = true,

    // Transaction related options
    .mEnableLongRunningTx = true,
    .mEnableFatTuple = false,
    .mEnableGc = true,
    .mEnableEagerGc = false,

    // Metrics related options
    .mEnableMetrics = false,
    .mMetricsPort = 8080,
    .mEnableCpuCounters = true,
    .mEnableTimeMeasure = false,
    .mEnablePerfEvents = false,
};

StoreOption* CreateStoreOption(const char* storeDir) {
  // create a new store option with default values
  StoreOption* option = new StoreOption();
  *option = kDefaultStoreOption;

  if (storeDir == nullptr) {
    storeDir = kDefaultStoreOption.mStoreDir;
  }

  // override the default store directory
  char* storeDirCopy = new char[strlen(storeDir) + 1];
  memcpy(storeDirCopy, storeDir, strlen(storeDir));
  storeDirCopy[strlen(storeDir)] = '\0';
  option->mStoreDir = storeDirCopy;

  return option;
}

StoreOption* CreateStoreOptionFrom(const StoreOption* storeDir) {
  StoreOption* option = new StoreOption();
  *option = *storeDir;

  // deep copy the store directory
  char* storeDirCopy = new char[strlen(storeDir->mStoreDir) + 1];
  memcpy(storeDirCopy, storeDir->mStoreDir, strlen(storeDir->mStoreDir));
  storeDirCopy[strlen(storeDir->mStoreDir)] = '\0';
  option->mStoreDir = storeDirCopy;

  return option;
}

void DestroyStoreOption(const StoreOption* option) {
  if (option != nullptr) {
    if (option->mStoreDir != nullptr) {
      delete[] option->mStoreDir;
    }
    delete option;
  }
}