#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

using namespace leanstore;
using TID = uint64_t;
std::atomic<TID> global_tid[1024] = {0};
// -------------------------------------------------------------------------------------
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
  leanstore::storage::btree::BasicKV* key_tid;
  leanstore::storage::btree::BasicKV* tid_value;
  std::string name;
  // -------------------------------------------------------------------------------------
  LeanStoreAdapter() {
    // hack
  }
  LeanStoreAdapter(LeanStore& db, std::string name) : name(name) {
    key_tid = &db.registerBasicKV(name + "_key_tid", false);
    tid_value = &db.registerBasicKV(name + "_tid_value", false);
  }
  // -------------------------------------------------------------------------------------
  void insert(const typename Record::Key& key, const Record& record) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    TID tid = global_tid[Record::id * 8].fetch_add(1);
    OpCode res;
    res = key_tid->Insert(folded_key, folded_key_len, (uint8_t*)(&tid), sizeof(TID));
    ensure(res == leanstore::OpCode::kOK);
    res = tid_value->Insert((uint8_t*)&tid, sizeof(TID), (uint8_t*)(&record), sizeof(Record));
    ensure(res == leanstore::OpCode::kOK);
  }

  void moveIt(TID tid, uint8_t* folded_key, uint16_t folded_key_len) {
    if (tid & (1ull << 63)) {
      return;
    }
  }
  // -------------------------------------------------------------------------------------
  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& cb) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(
        folded_key, folded_key_len, [&](const uint8_t* payload, uint16_t payload_length) {
          ensure(payload_length == sizeof(TID));
          tid = *reinterpret_cast<const TID*>(payload);
          // -------------------------------------------------------------------------------------
          tid_value->lookup(
              (uint8_t*)&tid, sizeof(TID), [&](const uint8_t* payload, uint16_t payload_length) {
                ensure(payload_length == sizeof(Record));
                const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
                cb(typed_payload);
              });
        });
    ensure(ret == OpCode::kOK);
    // -------------------------------------------------------------------------------------
    moveIt(tid, folded_key, folded_key_len);
  }
  // -------------------------------------------------------------------------------------
  void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb,
               UpdateDesc& update_descriptor) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    UpdateDesc tmp;
    tmp.count = 0;
    OpCode ret;
    TID tid;
    ret = key_tid->UpdatePartial(
        folded_key, folded_key_len,
        [&](uint8_t* tid_payload, uint16_t tid_payload_length) {
          ensure(tid_payload_length == sizeof(TID));
          tid = *reinterpret_cast<const TID*>(tid_payload);
          // -------------------------------------------------------------------------------------
          OpCode ret2 = tid_value->UpdatePartial((uint8_t*)&tid, sizeof(TID),
                                                 [&](uint8_t* payload, uint16_t payload_length) {
                                                   static_cast<void>(payload_length);
                                                   assert(payload_length == sizeof(Record));
                                                   Record& typed_payload =
                                                       *reinterpret_cast<Record*>(payload);
                                                   cb(typed_payload);
                                                 },
                                                 update_descriptor);
          ensure(ret2 == OpCode::kOK);
        },
        tmp);
    ensure(ret == OpCode::kOK);
    moveIt(tid, folded_key, folded_key_len);
  }
  // -------------------------------------------------------------------------------------
  bool erase(const typename Record::Key& key) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(folded_key, folded_key_len,
                          [&](const uint8_t* payload, uint16_t payload_length) {
                            ensure(payload_length == sizeof(TID));
                            tid = *reinterpret_cast<const TID*>(payload);
                          });
    if (ret != OpCode::kOK) {
      return false;
    }
    // -------------------------------------------------------------------------------------
    ret = tid_value->Remove((uint8_t*)&tid, sizeof(TID));
    if (ret != OpCode::kOK) {
      return false;
    }
    ret = key_tid->Remove(folded_key, folded_key_len);
    if (ret != OpCode::kOK) {
      return false;
    }
    return true;
  }
  // -------------------------------------------------------------------------------------
  void scan(const typename Record::Key& key,
            const std::function<bool(const typename Record::Key&, const Record&)>& cb,
            std::function<void()> undo) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    ret = key_tid->ScanAsc(
        folded_key, folded_key_len,
        [&](const uint8_t* key, [[maybe_unused]] uint16_t keySize, const uint8_t* tid_ptr,
            [[maybe_unused]] uint16_t tid_length) {
          TID tid = *reinterpret_cast<const TID*>(tid_ptr);
          ensure(tid_length == sizeof(TID));
          // -------------------------------------------------------------------------------------
          bool should_continue;
          OpCode res2 = tid_value->lookup(
              (uint8_t*)&tid, sizeof(TID), [&](const uint8_t* value_ptr, uint16_t valSize) {
                ensure(valSize == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload = *reinterpret_cast<const Record*>(value_ptr);
                should_continue = cb(typed_key, typed_payload);
              });
          if (res2 == OpCode::kOK) {
            return should_continue;
          } else {
            return true;
          }
        },
        undo);
    ensure(ret == OpCode::kOK);
  }
  // -------------------------------------------------------------------------------------
  void ScanDesc(const typename Record::Key& key,
                const std::function<bool(const typename Record::Key&, const Record&)>& cb,
                std::function<void()> undo) final {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    ret = key_tid->ScanDesc(
        folded_key, folded_key_len,
        [&](const uint8_t* key, [[maybe_unused]] uint16_t keySize, const uint8_t* tid_ptr,
            [[maybe_unused]] uint16_t tid_length) {
          const TID tid = *reinterpret_cast<const TID*>(tid_ptr);
          ensure(tid_length == sizeof(TID));
          // -------------------------------------------------------------------------------------
          bool should_continue;
          OpCode res2 = tid_value->lookup(
              (uint8_t*)&tid, sizeof(TID), [&](const uint8_t* value_ptr, uint16_t valSize) {
                ensure(valSize == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload = *reinterpret_cast<const Record*>(value_ptr);
                should_continue = cb(typed_key, typed_payload);
              });
          if (res2 == OpCode::kOK) {
            return should_continue;
          } else {
            return true;
          }
        },
        undo);
    ensure(ret == OpCode::kOK);
  }
  // -------------------------------------------------------------------------------------
  template <class Field>
  Field lookupField(const typename Record::Key& key, Field Record::*f) {
    uint8_t folded_key[Record::maxFoldLength()];
    uint16_t folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(folded_key, folded_key_len,
                          [&](const uint8_t* payload, uint16_t payload_length) {
                            ensure(payload_length == sizeof(TID));
                            tid = *reinterpret_cast<const TID*>(payload);
                          });
    ensure(ret == OpCode::kOK);
    // -------------------------------------------------------------------------------------
    Field local_f;
    ret = tid_value->lookup(
        (uint8_t*)&tid, sizeof(TID), [&](const uint8_t* payload, uint16_t payload_length) {
          ensure(payload_length == sizeof(Record));
          const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
          local_f = (typed_payload).*f;
        });
    ensure(ret == OpCode::kOK);
    moveIt(tid, folded_key, folded_key_len);
    return local_f;
  }
  // -------------------------------------------------------------------------------------
  uint64_t count() {
    return 0;
  }
};
