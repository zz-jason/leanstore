#include "leanstore/profiling/tables/ConfigsTable.hpp"

#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/UserThread.hpp"

namespace leanstore {
namespace profiling {

std::string ConfigsTable::getName() {
  return "configs";
}

void ConfigsTable::add(std::string name, std::string value) {
  columns.emplace(name, [&, value](Column& col) { col << value; });
}

void ConfigsTable::open() {
  columns.emplace("c_worker_threads",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mWorkerThreads; });

  columns.emplace("c_free_pct",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mFreePct; });

  columns.emplace("c_buffer_frame_providers",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mNumBufferProviders; });

  columns.emplace("c_num_partitions",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mNumPartitions; });

  columns.emplace("c_buffer_pool_size",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mBufferPoolSize; });

  columns.emplace("c_bulk_insert",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableBulkInsert; });

  columns.emplace("c_contention_split", [&](Column& col) {
    col << utils::tlsStore->mStoreOption.mEnableContentionSplit;
  });

  columns.emplace("c_contention_split_sample_probability", [&](Column& col) {
    col << utils::tlsStore->mStoreOption.mContentionSplitSampleProbability;
  });

  columns.emplace("c_cm_period", [&](Column& col) {
    col << utils::tlsStore->mStoreOption.mContentionSplitProbility;
  });

  columns.emplace("c_contention_split_threshold_pct", [&](Column& col) {
    col << utils::tlsStore->mStoreOption.mContentionSplitThresholdPct;
  });

  columns.emplace("c_xmerge",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableXMerge; });

  columns.emplace("c_xmerge_k",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mXMergeK; });

  columns.emplace("c_xmerge_target_pct",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mXMergeTargetPct; });

  columns.emplace("c_btree_heads", [&](Column& col) {
    col << utils::tlsStore->mStoreOption.mEnableHeadOptimization;
  });

  columns.emplace("c_btree_hints",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mBTreeHints; });

  columns.emplace("c_wal", [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableWal; });

  columns.emplace("c_wal_io_hack", [&](Column& col) { col << 1; });

  columns.emplace("c_wal_fsync",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableWalFsync; });

  columns.emplace("c_enable_garbage_collection",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableGc; });

  columns.emplace("c_vi_fat_tuple",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableFatTuple; });

  columns.emplace("c_enable_long_running_transaction",
                  [&](Column& col) { col << utils::tlsStore->mStoreOption.mEnableLongRunningTx; });

  for (auto& c : columns) {
    c.second.generator(c.second);
  }
}

uint64_t ConfigsTable::hash() {
  std::stringstream configConcatenation;
  for (const auto& c : columns) {
    configConcatenation << c.second.values[0];
  }
  return std::hash<std::string>{}(configConcatenation.str());
}

void ConfigsTable::next() {
  // one time is enough
  return;
}

} // namespace profiling
} // namespace leanstore
