#pragma once

#include "leanstore/base/result.hpp"
#include "tools/ycsb/ycsb_options.hpp"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include <cassert>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace leanstore::ycsb {

class YcsbDb;
class YcsbSession;
class YcsbKvSpace;

class YcsbRocksDb {
public:
  static Result<std::unique_ptr<YcsbRocksDb>> Create(const YcsbOptions& options);

  explicit YcsbRocksDb(std::unique_ptr<rocksdb::DB> db) : db_(std::move(db)) {
  }

  YcsbSession NewSession();

private:
  std::unique_ptr<rocksdb::DB> db_;
};

class YcsbRocksSession {
public:
  explicit YcsbRocksSession(rocksdb::DB& db) : db_(db) {
  }

  Result<void> CreateKvSpace(std::string_view name);
  Result<YcsbKvSpace> GetKvSpace(std::string_view name);

private:
  rocksdb::DB& db_;
};

class YcsbRocksKvSpace {
public:
  explicit YcsbRocksKvSpace(rocksdb::DB& db) : db_(db) {
  }

  Result<void> Put(std::string_view key, std::string_view value);
  Result<void> Update(std::string_view key, std::string_view value);
  Result<void> Get(std::string_view key, std::string& value_out);

private:
  rocksdb::DB& db_;
  rocksdb::ReadOptions read_options_;
  rocksdb::WriteOptions write_options_;
};

} // namespace leanstore::ycsb