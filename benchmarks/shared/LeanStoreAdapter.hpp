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
      std::function<void()> undo) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    OP_RESULT ret = btree->scanDesc(
        folded_key, folded_key_len,
        [&](const u8* key, [[maybe_unused]] u16 keySize, const u8* payload,
            [[maybe_unused]] u16 payload_length) {
          if (keySize != folded_key_len) {
            return false;
          }
          typename Record::Key typed_key;
          Record::unfoldKey(key, typed_key);
          const Record& typed_payload =
              *reinterpret_cast<const Record*>(payload);
          return cb(typed_key, typed_payload);
        },
        undo);
    if (ret == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
  }
  // -------------------------------------------------------------------------------------
  void insert(const typename Record::Key& key, const Record& record) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    const OP_RESULT res = btree->insert(Slice(folded_key, folded_key_len),
                                        Slice((u8*)(&record), sizeof(Record)));
    DCHECK(res == leanstore::OP_RESULT::OK ||
           res == leanstore::OP_RESULT::ABORT_TX);
    if (res == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
  }
  // -------------------------------------------------------------------------------------
  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& cb) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    const OP_RESULT res =
        btree->Lookup(Slice(folded_key, folded_key_len), [&](Slice val) {
          const Record& typed_payload =
              *reinterpret_cast<const Record*>(val.data());
          cb(typed_payload);
        });
    if (res == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
    DCHECK(res == leanstore::OP_RESULT::OK);
  }
  // -------------------------------------------------------------------------------------
  void update1(const typename Record::Key& key,
               const std::function<void(Record&)>& cb,
               UpdateDesc& update_descriptor) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    if (!FLAGS_vi_delta) {
      // Disable deltas, copy the whole tuple [hacky]
      DCHECK(update_descriptor.count > 0);
      DCHECK(!FLAGS_vi_fat_tuple);
      update_descriptor.count = 1;
      update_descriptor.mDiffSlots[0].offset = 0;
      update_descriptor.mDiffSlots[0].length = sizeof(Record);
    }
    // -------------------------------------------------------------------------------------
    const OP_RESULT res = btree->updateSameSizeInPlace(
        folded_key, folded_key_len,
        [&](u8* payload, u16 payload_length) {
          static_cast<void>(payload_length);
          DCHECK(payload_length == sizeof(Record));
          Record& typed_payload = *reinterpret_cast<Record*>(payload);
          cb(typed_payload);
        },
        update_descriptor);
    DCHECK(res != leanstore::OP_RESULT::NOT_FOUND);
    if (res == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
  }
  // -------------------------------------------------------------------------------------
  bool erase(const typename Record::Key& key) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    const auto res = btree->remove(folded_key, folded_key_len);
    if (res == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
    return (res == leanstore::OP_RESULT::OK);
  }
  // -------------------------------------------------------------------------------------
  void scan(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    OP_RESULT ret = btree->scanAsc(
        folded_key, folded_key_len,
        [&](const u8* key, u16 keySize, const u8* payload, u16 payload_length) {
          if (keySize != folded_key_len) {
            return false;
          }
          static_cast<void>(payload_length);
          typename Record::Key typed_key;
          Record::unfoldKey(key, typed_key);
          const Record& typed_payload =
              *reinterpret_cast<const Record*>(payload);
          return cb(typed_key, typed_payload);
        },
        undo);
    if (ret == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
  }
  // -------------------------------------------------------------------------------------
  template <class Field>
  Field lookupField(const typename Record::Key& key, Field Record::*f) {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    Field local_f;
    const OP_RESULT res = btree->lookup(
        folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
          static_cast<void>(payload_length);
          Record& typed_payload =
              *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
          local_f = (typed_payload).*f;
        });
    if (res == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
    }
    DCHECK(res == OP_RESULT::OK);
    return local_f;
  }
  // -------------------------------------------------------------------------------------
  u64 count() {
    return btree->countEntries();
  }
};
