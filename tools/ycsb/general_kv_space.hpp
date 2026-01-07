#pragma once

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/cpp/base/result.hpp"
#include "leanstore/cpp/base/slice.hpp"

#include <rocksdb/options.h>
#include <wiredtiger.h>

#include <cassert>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

namespace leanstore::ycsb {

using ByteBuffer = std::vector<uint8_t>;

class RocksDbKvSpace;
class WiredTigerKvSpace;
class LeanAtomicKvSpace;
class LeanMvccKvSpace;
class GeneralKvSpace;

class RocksDbKvSpace {
public:
  RocksDbKvSpace(RocksDbKvSpace&& other) noexcept : db_(other.db_) {
  }
  RocksDbKvSpace(const RocksDbKvSpace&) = delete;
  RocksDbKvSpace& operator=(const RocksDbKvSpace&) = delete;
  RocksDbKvSpace& operator=(RocksDbKvSpace&&) = delete;

  Result<void> Insert(Slice key, Slice value);
  Result<void> Lookup(Slice key, ByteBuffer& value_out);
  Result<void> Update(Slice key, Slice value);

private:
  friend class GeneralKvSpace;
  explicit RocksDbKvSpace(rocksdb::DB& db) : db_(db) {
  }

  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;
  rocksdb::DB& db_;
};

class WiredTigerKvSpace {
public:
  WiredTigerKvSpace(WiredTigerKvSpace&& other) noexcept : cursor_(other.cursor_) {
    other.moved_ = true;
  }
  WiredTigerKvSpace(const WiredTigerKvSpace&) = delete;
  WiredTigerKvSpace& operator=(const WiredTigerKvSpace&) = delete;
  WiredTigerKvSpace& operator=(WiredTigerKvSpace&&) = delete;

  ~WiredTigerKvSpace() {
    if (!moved_) {
      cursor_.close(&cursor_);
    }
  }

  Result<void> Insert(Slice key, Slice value);
  Result<void> Lookup(Slice key, ByteBuffer& value_out);
  Result<void> Update(Slice key, Slice value);

private:
  friend class GeneralKvSpace;
  explicit WiredTigerKvSpace(WT_CURSOR& cursor) : cursor_(cursor) {};

  static void ResetItem(WT_ITEM& item, Slice new_item) {
    item.data = new_item.data();
    item.size = new_item.size();
  }

  WT_CURSOR& cursor_;
  WT_ITEM key_item_;
  WT_ITEM val_item_;
  bool moved_ = false;
};

class LeanAtomicKvSpace {
public:
  LeanAtomicKvSpace(LeanAtomicKvSpace&& other) noexcept : atomic_btree_(other.atomic_btree_) {
  }
  LeanAtomicKvSpace(const LeanAtomicKvSpace&) = delete;
  LeanAtomicKvSpace& operator=(const LeanAtomicKvSpace&) = delete;
  LeanAtomicKvSpace& operator=(LeanAtomicKvSpace&&) = delete;

  Result<void> Insert(Slice key, Slice value);
  Result<void> Lookup(Slice key, ByteBuffer& value_out);
  Result<void> Update(Slice key, Slice value);

private:
  friend class GeneralKvSpace;
  explicit LeanAtomicKvSpace(BasicKV& atomic_btree) : atomic_btree_(atomic_btree) {
  }

  BasicKV& atomic_btree_;
};

class LeanMvccKvSpace {
public:
  LeanMvccKvSpace(LeanMvccKvSpace&& other) noexcept : mvcc_btree_(other.mvcc_btree_) {
  }
  LeanMvccKvSpace(const LeanMvccKvSpace&) = delete;
  LeanMvccKvSpace& operator=(const LeanMvccKvSpace&) = delete;
  LeanMvccKvSpace& operator=(LeanMvccKvSpace&&) = delete;

  Result<void> Insert(Slice key, Slice value);
  Result<void> Lookup(Slice key, ByteBuffer& value_out);
  Result<void> Update(Slice key, Slice value);

private:
  friend class GeneralKvSpace;
  explicit LeanMvccKvSpace(TransactionKV& mvcc_btree) : mvcc_btree_(mvcc_btree) {
  }

  TransactionKV& mvcc_btree_;
};

/// Concept for KvSpace
template <typename T>
concept KvSpaceConcept = requires(T kv_space, Slice key, Slice value, ByteBuffer& value_out) {
  { kv_space.Insert(key, value) } -> std::same_as<Result<void>>;
  { kv_space.Lookup(key, value_out) } -> std::same_as<Result<void>>;
  { kv_space.Update(key, value) } -> std::same_as<Result<void>>;
};

/// GeneralKvSpace that can wrap multiple KvSpace implementations
class GeneralKvSpace {
public:
  template <KvSpaceConcept... Ts>
  using KvSpaceVariant = std::variant<Ts...>;

  using AnyKvSpace =
      KvSpaceVariant<WiredTigerKvSpace, RocksDbKvSpace, LeanMvccKvSpace, LeanAtomicKvSpace>;

  static std::unique_ptr<GeneralKvSpace> CreateRocksDbKvSpace(rocksdb::DB& db) {
    return std::make_unique<GeneralKvSpace>(AnyKvSpace(RocksDbKvSpace(db)));
  }

  static std::unique_ptr<GeneralKvSpace> CreateWiredTigerKvSpace(WT_CURSOR& cursor) {
    return std::make_unique<GeneralKvSpace>(AnyKvSpace(WiredTigerKvSpace(cursor)));
  }

  static std::unique_ptr<GeneralKvSpace> CreateLeanAtomicKvSpace(BasicKV& atomic_btree) {
    return std::make_unique<GeneralKvSpace>(AnyKvSpace(LeanAtomicKvSpace(atomic_btree)));
  }

  static std::unique_ptr<GeneralKvSpace> CreateLeanMvccKvSpace(TransactionKV& mvcc_btree) {
    return std::make_unique<GeneralKvSpace>(AnyKvSpace(LeanMvccKvSpace(mvcc_btree)));
  }

  explicit GeneralKvSpace(AnyKvSpace kv_space) : kv_space_(std::move(kv_space)) {
  }

  Result<void> Insert(Slice key, Slice value) {
    return std::visit([&](auto&& kv_space) { return kv_space.Insert(key, value); }, kv_space_);
  }

  Result<void> Lookup(Slice key, ByteBuffer& value_out) {
    return std::visit([&](auto&& kv_space) { return kv_space.Lookup(key, value_out); }, kv_space_);
  }

  Result<void> Update(Slice key, Slice value) {
    return std::visit([&](auto&& kv_space) { return kv_space.Update(key, value); }, kv_space_);
  }

private:
  AnyKvSpace kv_space_;
};

} // namespace leanstore::ycsb