#include "tools/ycsb/ycsb_backend_rocksdb.hpp"

#include "leanstore/cpp/base/result.hpp"
#include "tools/ycsb/console_logger.hpp"
#include "tools/ycsb/ycsb_backend.hpp"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <wiredtiger.h>

#include <cassert>
#include <memory>
#include <string>
#include <string_view>

namespace leanstore::ycsb {

//------------------------------------------------------------------------------
// YcsbRocksDb
//------------------------------------------------------------------------------

Result<std::unique_ptr<YcsbRocksDb>> YcsbRocksDb::Create(const YcsbOptions& options) {
  rocksdb::Options rocksdb_options;
  rocksdb_options.create_if_missing = true;
  rocksdb_options.error_if_exists = false;

  // Memory limit
  size_t physical_limit = size_t(options.dram_) << 30;
  size_t soft_limit = size_t(physical_limit * 0.9);

  auto shared_cache = rocksdb::NewLRUCache(soft_limit);
  rocksdb_options.write_buffer_manager =
      std::make_shared<rocksdb::WriteBufferManager>(soft_limit, shared_cache);

  // Table options
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.block_cache = shared_cache;
  table_opts.cache_index_and_filter_blocks = true;
  table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  table_opts.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_opts.partition_filters = true;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

  rocksdb_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  // Write options
  rocksdb_options.write_buffer_size = 64 * 1024 * 1024;
  rocksdb_options.max_write_buffer_number = 4;
  rocksdb_options.min_write_buffer_number_to_merge = 1;
  rocksdb_options.allow_concurrent_memtable_write = true;
  rocksdb_options.enable_pipelined_write = true;

  // Background job options
  rocksdb_options.max_background_jobs = options.workers_;

  // Compaction options
  rocksdb_options.level0_file_num_compaction_trigger = 6;
  rocksdb_options.level0_slowdown_writes_trigger = 24;
  rocksdb_options.level0_stop_writes_trigger = 36;

  std::unique_ptr<rocksdb::DB> db = nullptr;
  auto status = rocksdb::DB::Open(rocksdb_options, options.DataDir(), &db);
  if (!status.ok()) {
    ConsoleFatal(std::format("Failed to open rocksdb: {}", status.ToString()));
  }

  return std::make_unique<YcsbRocksDb>(std::move(db));
}

YcsbSession YcsbRocksDb::NewSession() {
  return YcsbSession(std::make_unique<YcsbRocksSession>(*db_));
}

//------------------------------------------------------------------------------
// YcsbRocksSession
//------------------------------------------------------------------------------

Result<void> YcsbRocksSession::CreateKvSpace(std::string_view name [[maybe_unused]]) {
  return {};
}

Result<YcsbKvSpace> YcsbRocksSession::GetKvSpace(std::string_view name [[maybe_unused]]) {
  return YcsbKvSpace(std::make_unique<YcsbRocksKvSpace>(db_));
}

//------------------------------------------------------------------------------
// YcsbRocksKvSpace
//------------------------------------------------------------------------------

Result<void> YcsbRocksKvSpace::Put(std::string_view key, std::string_view value) {
  rocksdb::Slice key_slice(key.data(), key.size());
  rocksdb::Slice val_slice(value.data(), value.size());
  auto status = db_.Put(write_options_, key_slice, val_slice);
  if (!status.ok()) {
    return Error::General(std::format("RocksDB Put failed: {}", status.ToString()));
  }
  return {};
}

Result<void> YcsbRocksKvSpace::Update(std::string_view key, std::string_view value) {
  return Put(key, value);
}

Result<void> YcsbRocksKvSpace::Get(std::string_view key, std::string& value_out) {
  rocksdb::Slice key_slice(key.data(), key.size());
  auto status = db_.Get(read_options_, key_slice, &value_out);
  if (!status.ok()) {
    return Error::General(std::format("RocksDB Get failed: {}", status.ToString()));
  }
  return {};
}

} // namespace leanstore::ycsb