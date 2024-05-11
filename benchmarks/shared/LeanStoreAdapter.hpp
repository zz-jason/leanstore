#pragma once

#include "Adapter.hpp"
#include "leanstore/LeanStore.hpp"
#include "utils/Log.hpp"

#include <functional>

using namespace leanstore;
template <class Record> struct LeanStoreAdapter : Adapter<Record> {

  leanstore::KVInterface* btree;

  std::string name;

  LeanStoreAdapter() {
  }

  LeanStoreAdapter(LeanStore& db, std::string name) : name(name) {
    if (!db.mStoreOption.mCreateFromScratch) {
      leanstore::storage::btree::TransactionKV* tree;
      db.GetTransactionKV(name, &tree);
      btree = reinterpret_cast<leanstore::KVInterface*>(tree);
    } else {
      leanstore::storage::btree::TransactionKV* tree;
      auto res = db.CreateTransactionKV(name);
      if (res) {
        tree = res.value();
      } else {
        Log::Fatal(std::format("failed to create transaction kv, error={}",
                               res.error().ToString()));
      }
      btree = reinterpret_cast<leanstore::KVInterface*>(tree);
    }
  }

  void ScanDesc(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo [[maybe_unused]]) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    OpCode ret = btree->ScanDesc(
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
      cr::Worker::My().AbortTx();
    }
  }

  void insert(const typename Record::Key& key, const Record& record) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    const OpCode res =
        btree->Insert(Slice(foldedKey, foldedKeySize),
                      Slice((uint8_t*)(&record), sizeof(Record)));
    LS_DCHECK(res == leanstore::OpCode::kOK ||
              res == leanstore::OpCode::kAbortTx);
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::My().AbortTx();
    }
  }

  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& cb) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    const OpCode res =
        btree->Lookup(Slice(foldedKey, foldedKeySize), [&](Slice val) {
          const Record& record = *reinterpret_cast<const Record*>(val.data());
          cb(record);
        });
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::My().AbortTx();
    }
    LS_DCHECK(res == leanstore::OpCode::kOK);
  }

  void update1(const typename Record::Key& key,
               const std::function<void(Record&)>& cb,
               UpdateDesc& updateDesc) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);

    const OpCode res = btree->UpdatePartial(
        Slice(foldedKey, foldedKeySize),
        [&](MutableSlice mutRawVal) {
          LS_DCHECK(mutRawVal.Size() == sizeof(Record));
          auto& record = *reinterpret_cast<Record*>(mutRawVal.Data());
          cb(record);
        },
        updateDesc);
    LS_DCHECK(res != leanstore::OpCode::kNotFound);
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::My().AbortTx();
    }
  }

  bool erase(const typename Record::Key& key) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    const auto res = btree->Remove(Slice(foldedKey, foldedKeySize));
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::My().AbortTx();
    }
    return (res == leanstore::OpCode::kOK);
  }

  void scan(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo [[maybe_unused]]) final {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    OpCode ret = btree->ScanAsc(
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
      cr::Worker::My().AbortTx();
    }
  }

  template <class Field>
  Field lookupField(const typename Record::Key& key, Field Record::*f) {
    uint8_t foldedKey[Record::maxFoldLength()];
    uint16_t foldedKeySize = Record::foldKey(foldedKey, key);
    Field local_f;
    const OpCode res =
        btree->Lookup(foldedKey, foldedKeySize,
                      [&](const uint8_t* payload, uint16_t payloadSize) {
                        Record& record = *const_cast<Record*>(
                            reinterpret_cast<const Record*>(payload));
                        local_f = (record).*f;
                      });
    if (res == leanstore::OpCode::kAbortTx) {
      cr::Worker::My().AbortTx();
    }
    LS_DCHECK(res == OpCode::kOK);
    return local_f;
  }

  uint64_t count() {
    return btree->CountEntries();
  }
};
