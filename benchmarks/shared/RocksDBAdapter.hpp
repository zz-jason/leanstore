#pragma once
#include "Adapter.hpp"
#include "Types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/KVInterface.hpp"
#include "leanstore/utils/JumpMU.hpp"

#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

struct RocksDB {
  union {
    rocksdb::DB* db = nullptr;
    rocksdb::TransactionDB* tx_db;
    rocksdb::OptimisticTransactionDB* optimistic_transaction_db;
  };
  static thread_local rocksdb::Transaction* txn;
  rocksdb::WriteOptions wo;
  rocksdb::ReadOptions ro;
  enum class DB_TYPE : uint8_t { DB, TransactionDB, OptimisticDB };
  const DB_TYPE type;
  // -------------------------------------------------------------------------------------
  RocksDB(DB_TYPE type = DB_TYPE::DB) : type(type) {
    wo.disableWAL = true;
    wo.sync = false;
    // -------------------------------------------------------------------------------------
    rocksdb::Options db_options;
    db_options.use_direct_reads = true;
    db_options.use_direct_io_for_flush_and_compaction = true;
    db_options.db_write_buffer_size = 0; // disabled
    // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
    db_options.create_if_missing = true;
    db_options.manual_wal_flush = true;
    db_options.compression = rocksdb::CompressionType::kNoCompression;
    // db_options.OptimizeLevelStyleCompaction(FLAGS_dram_gib * 1024 * 1024 *
    // 1024);
    db_options.row_cache = rocksdb::NewLRUCache(FLAGS_dram_gib * 1024 * 1024 * 1024);
    rocksdb::Status s;
    if (type == DB_TYPE::DB) {
      s = rocksdb::DB::Open(db_options, FLAGS_ssd_path, &db);
    } else if (type == DB_TYPE::TransactionDB) {
      s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, &tx_db);
    } else if (type == DB_TYPE::OptimisticDB) {
      s = rocksdb::OptimisticTransactionDB::Open(db_options, FLAGS_ssd_path,
                                                 &optimistic_transaction_db);
    }
    if (!s.ok())
      cerr << s.ToString() << endl;
    assert(s.ok());
  }

  ~RocksDB() {
    delete db;
  }
  void StartTx() {
    rocksdb::Status s;
    if (type == DB_TYPE::TransactionDB) {
      txn = tx_db->BeginTransaction(wo, {});
    } else if (type == DB_TYPE::OptimisticDB) {
      txn = optimistic_transaction_db->BeginTransaction({}, {});
    } else {
    }
  }
  void CommitTx() {
    if (type != DB_TYPE::DB) {
      rocksdb::Status s;
      s = txn->Commit();
      delete txn;
      txn = nullptr;
    }
  }
  void prepareThread() {
  }
};
// -------------------------------------------------------------------------------------
template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
  using SEP = uint32_t; // use 32-bits integer as separator instead of column family
  RocksDB& map;
  RocksDBAdapter(RocksDB& map) : map(map) {
  }
  // -------------------------------------------------------------------------------------
  template <typename T>
  rocksdb::Slice RSlice(T* ptr, uint64_t len) {
    return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
  }
  // -------------------------------------------------------------------------------------
  void insert(const typename Record::Key& key, const Record& record) final {
    uint8_t folded_key[Record::maxFoldLength() + sizeof(SEP)];
    const uint32_t folded_key_len =
        Fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
    // -------------------------------------------------------------------------------------
    rocksdb::Status s;
    if (map.type == RocksDB::DB_TYPE::DB) {
      s = map.db->Put(map.wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      ensure(s.ok());
    } else {
      s = map.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      if (!s.ok()) {
        map.txn->Rollback();
        jumpmu::Jump();
      }
    }
  }
  // -------------------------------------------------------------------------------------
  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& fn) final {
    uint8_t folded_key[Record::maxFoldLength() + sizeof(SEP)];
    const uint32_t folded_key_len =
        Fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
    // -------------------------------------------------------------------------------------
    rocksdb::PinnableSlice value;
    rocksdb::Status s;
    if (map.type == RocksDB::DB_TYPE::DB) {
      s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len),
                      &value);
    } else {
      s = map.txn->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len),
                       &value);
    }
    assert(s.ok());
    const Record& record = *reinterpret_cast<const Record*>(value.data());
    fn(record);
    value.Reset();
  }
  // -------------------------------------------------------------------------------------
  void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn,
               leanstore::UpdateDesc&) final {
    Record r;
    lookup1(key, [&](const Record& rec) { r = rec; });
    fn(r);
    insert(key, r);
  }
  // -------------------------------------------------------------------------------------
  bool erase(const typename Record::Key& key) final {
    uint8_t folded_key[Record::maxFoldLength() + sizeof(SEP)];
    const uint32_t folded_key_len =
        Fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
    // -------------------------------------------------------------------------------------
    rocksdb::Status s;
    if (map.type == RocksDB::DB_TYPE::DB) {
      s = map.db->Delete(map.wo, RSlice(folded_key, folded_key_len));
      if (s.ok()) {
        return true;
      } else {
        return false;
      }
    } else {
      s = map.txn->Delete(RSlice(folded_key, folded_key_len));
      if (!s.ok()) {
        map.txn->Rollback();
        jumpmu::Jump();
      }
      return true;
    }
  }
  // -------------------------------------------------------------------------------------
  template <class T>
  uint32_t getId(const T& str) {
    return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
  }
  //             [&](const neworder_t::Key& key, const neworder_t&) {
  void scan(const typename Record::Key& key,
            const std::function<bool(const typename Record::Key&, const Record&)>& fn,
            std::function<void()>) final {
    uint8_t folded_key[Record::maxFoldLength() + sizeof(SEP)];
    const uint32_t folded_key_len =
        Fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
    // -------------------------------------------------------------------------------------
    rocksdb::Iterator* it = map.db->NewIterator(map.ro);
    for (it->SeekToFirstGreaterEqual(RSlice(folded_key, folded_key_len));
         it->Valid() && getId(it->Key()) == Record::id; it->Next()) {
      typename Record::Key s_key;
      Record::unfoldKey(reinterpret_cast<const uint8_t*>(it->Key().data() + sizeof(SEP)), s_key);
      const Record& s_value = *reinterpret_cast<const Record*>(it->Val().data());
      if (!fn(s_key, s_value))
        break;
    }
    assert(it->status().ok());
    delete it;
  }
  // -------------------------------------------------------------------------------------
  void ScanDesc(const typename Record::Key& key,
                const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                std::function<void()>) final {
    uint8_t folded_key[Record::maxFoldLength() + sizeof(SEP)];
    const uint32_t folded_key_len =
        Fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
    // -------------------------------------------------------------------------------------
    rocksdb::Iterator* it = map.db->NewIterator(map.ro);
    for (it->SeekToLastLessEqual(RSlice(folded_key, folded_key_len));
         it->Valid() && getId(it->Key()) == Record::id; it->Prev()) {
      typename Record::Key s_key;
      Record::unfoldKey(reinterpret_cast<const uint8_t*>(it->Key().data() + sizeof(SEP)), s_key);
      const Record& s_value = *reinterpret_cast<const Record*>(it->Val().data());
      if (!fn(s_key, s_value))
        break;
    }
    assert(it->status().ok());
    delete it;
  }
  // -------------------------------------------------------------------------------------
  template <class Field>
  Field lookupField(const typename Record::Key& key, Field Record::*f) {
    Field local_f;
    bool found = false;
    lookup1(key, [&](const Record& record) {
      found = true;
      local_f = (record).*f;
    });
    assert(found);
    return local_f;
  }
};
