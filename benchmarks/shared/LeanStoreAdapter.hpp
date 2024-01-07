#pragma once

#include "Adapter.hpp"
#include "Exceptions.hpp"
#include "LeanStore.hpp"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

using namespace leanstore;
template <class Record> struct LeanStoreAdapter : Adapter<Record> {

  leanstore::KVInterface* btree;

  string name;

  LeanStoreAdapter() {
  }

  LeanStoreAdapter(LeanStore& db, string name) : name(name) {
    if (FLAGS_vi) {
      if (FLAGS_recover) {
        leanstore::storage::btree::BTreeVI* tree;
        db.GetBTreeVI(name, &tree);
        btree = reinterpret_cast<leanstore::KVInterface*>(tree);
      } else {
        leanstore::storage::btree::BTreeVI* tree;
        storage::btree::BTreeGeneric::Config config{.mEnableWal = FLAGS_wal,
                                                    .mUseBulkInsert = false};
        db.RegisterBTreeVI(name, config, &tree);
        btree = reinterpret_cast<leanstore::KVInterface*>(tree);
      }
    } else {
      if (FLAGS_recover) {
        leanstore::storage::btree::BTreeLL* tree;
        db.GetBTreeLL(name, &tree);
        btree = reinterpret_cast<leanstore::KVInterface*>(tree);
      } else {
        leanstore::storage::btree::BTreeLL* tree;
        storage::btree::BTreeGeneric::Config config{.mEnableWal = FLAGS_wal,
                                                    .mUseBulkInsert = false};
        db.RegisterBTreeLL(name, config, &tree);
        btree = reinterpret_cast<leanstore::KVInterface*>(tree);
      }
    }
  }

  void printTreeHeight() {
    cout << name << " height = " << btree->getHeight() << endl;
  }

  void scanDesc(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo [[maybe_unused]]) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    OpCode ret = btree->scanDesc(
        Slice(foldedKey, foldedKeySize), [&](Slice key, Slice val) {
          if (key.size() != foldedKeySize) {
            return false;
          }
          typename Record::Key typed_key;
          Record::unfoldKey(key.data(), typed_key);
          const auto& record = *reinterpret_cast<const Record*>(val.data());
          return cb(typed_key, record);
        });
    if (ret == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
  }

  void insert(const typename Record::Key& key, const Record& record) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    const OpCode res = btree->insert(Slice(foldedKey, foldedKeySize),
                                     Slice((u8*)(&record), sizeof(Record)));
    DCHECK(res == leanstore::OpCode::kOk || res == leanstore::OpCode::kAbortTx);
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
  }

  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& cb) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    const OpCode res =
        btree->Lookup(Slice(foldedKey, foldedKeySize), [&](Slice val) {
          const Record& record = *reinterpret_cast<const Record*>(val.data());
          cb(record);
        });
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
    DCHECK(res == leanstore::OpCode::kOk);
  }

  void update1(const typename Record::Key& key,
               const std::function<void(Record&)>& cb,
               UpdateDesc& updateDesc) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);

    const OpCode res = btree->updateSameSizeInPlace(
        Slice(foldedKey, foldedKeySize),
        [&](MutableSlice val) {
          DCHECK(val.Size() == sizeof(Record));
          auto& record = *reinterpret_cast<Record*>(val.data());
          cb(record);
        },
        updateDesc);
    DCHECK(res != leanstore::OpCode::kNotFound);
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
  }

  bool erase(const typename Record::Key& key) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    const auto res = btree->remove(Slice(foldedKey, foldedKeySize));
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
    return (res == leanstore::OpCode::kOk);
  }

  void scan(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo [[maybe_unused]]) final {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    OpCode ret = btree->scanAsc(
        Slice(foldedKey, foldedKeySize), [&](Slice key, Slice val) {
          if (key.size() != foldedKeySize) {
            return false;
          }
          static_cast<void>(val.size());
          typename Record::Key typed_key;
          Record::unfoldKey(key.data(), typed_key);
          const Record& record = *reinterpret_cast<const Record*>(val.data());
          return cb(typed_key, record);
        });
    if (ret == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
  }

  template <class Field>
  Field lookupField(const typename Record::Key& key, Field Record::*f) {
    u8 foldedKey[Record::maxFoldLength()];
    u16 foldedKeySize = Record::foldKey(foldedKey, key);
    Field local_f;
    const OpCode res = btree->Lookup(
        foldedKey, foldedKeySize, [&](const u8* payload, u16 payloadSize) {
          Record& record =
              *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
          local_f = (record).*f;
        });
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::my().abortTX();
    }
    DCHECK(res == OpCode::kOk);
    return local_f;
  }

  u64 count() {
    return btree->countEntries();
  }
};
