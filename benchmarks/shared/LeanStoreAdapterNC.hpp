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
using TID = u64;
std::atomic<TID> global_tid[1024] = {0};
// -------------------------------------------------------------------------------------
template <class Record> struct LeanStoreAdapter : Adapter<Record> {
  leanstore::storage::btree::BasicKV* key_tid;
  leanstore::storage::btree::BasicKV* tid_value;
  string name;
  // -------------------------------------------------------------------------------------
  LeanStoreAdapter() {
    // hack
  }
  LeanStoreAdapter(LeanStore& db, string name) : name(name) {
    key_tid = &db.registerBasicKV(name + "_key_tid", false);
    tid_value = &db.registerBasicKV(name + "_tid_value", false);
  }
  // -------------------------------------------------------------------------------------
  void insert(const typename Record::Key& key, const Record& record) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    TID tid = global_tid[Record::id * 8].fetch_add(1);
    OpCode res;
    res = key_tid->Insert(folded_key, folded_key_len, (u8*)(&tid), sizeof(TID));
    ensure(res == leanstore::OpCode::kOK);
    res = tid_value->Insert((u8*)&tid, sizeof(TID), (u8*)(&record),
                            sizeof(Record));
    ensure(res == leanstore::OpCode::kOK);
  }

  void moveIt(TID tid, u8* folded_key, u16 folded_key_len) {
    if (tid & (1ull << 63)) {
      return;
    }
  }
  // -------------------------------------------------------------------------------------
  void lookup1(const typename Record::Key& key,
               const std::function<void(const Record&)>& cb) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(
        folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
          ensure(payload_length == sizeof(TID));
          tid = *reinterpret_cast<const TID*>(payload);
          // -------------------------------------------------------------------------------------
          tid_value->lookup((u8*)&tid, sizeof(TID),
                            [&](const u8* payload, u16 payload_length) {
                              ensure(payload_length == sizeof(Record));
                              const Record& typed_payload =
                                  *reinterpret_cast<const Record*>(payload);
                              cb(typed_payload);
                            });
        });
    ensure(ret == OpCode::kOK);
    // -------------------------------------------------------------------------------------
    moveIt(tid, folded_key, folded_key_len);
  }
  // -------------------------------------------------------------------------------------
  void update1(const typename Record::Key& key,
               const std::function<void(Record&)>& cb,
               UpdateDesc& update_descriptor) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    UpdateDesc tmp;
    tmp.count = 0;
    OpCode ret;
    TID tid;
    ret = key_tid->UpdateInPlace(
        folded_key, folded_key_len,
        [&](u8* tid_payload, u16 tid_payload_length) {
          ensure(tid_payload_length == sizeof(TID));
          tid = *reinterpret_cast<const TID*>(tid_payload);
          // -------------------------------------------------------------------------------------
          OpCode ret2 = tid_value->UpdateInPlace(
              (u8*)&tid, sizeof(TID),
              [&](u8* payload, u16 payload_length) {
                static_cast<void>(payload_length);
                assert(payload_length == sizeof(Record));
                Record& typed_payload = *reinterpret_cast<Record*>(payload);
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
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(folded_key, folded_key_len,
                          [&](const u8* payload, u16 payload_length) {
                            ensure(payload_length == sizeof(TID));
                            tid = *reinterpret_cast<const TID*>(payload);
                          });
    if (ret != OpCode::kOK) {
      return false;
    }
    // -------------------------------------------------------------------------------------
    ret = tid_value->Remove((u8*)&tid, sizeof(TID));
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
  void scan(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    ret = key_tid->ScanAsc(
        folded_key, folded_key_len,
        [&](const u8* key, [[maybe_unused]] u16 keySize, const u8* tid_ptr,
            [[maybe_unused]] u16 tid_length) {
          TID tid = *reinterpret_cast<const TID*>(tid_ptr);
          ensure(tid_length == sizeof(TID));
          // -------------------------------------------------------------------------------------
          bool should_continue;
          OpCode res2 = tid_value->lookup(
              (u8*)&tid, sizeof(TID), [&](const u8* value_ptr, u16 valSize) {
                ensure(valSize == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload =
                    *reinterpret_cast<const Record*>(value_ptr);
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
  void ScanDesc(
      const typename Record::Key& key,
      const std::function<bool(const typename Record::Key&, const Record&)>& cb,
      std::function<void()> undo) final {
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    ret = key_tid->ScanDesc(
        folded_key, folded_key_len,
        [&](const u8* key, [[maybe_unused]] u16 keySize, const u8* tid_ptr,
            [[maybe_unused]] u16 tid_length) {
          const TID tid = *reinterpret_cast<const TID*>(tid_ptr);
          ensure(tid_length == sizeof(TID));
          // -------------------------------------------------------------------------------------
          bool should_continue;
          OpCode res2 = tid_value->lookup(
              (u8*)&tid, sizeof(TID), [&](const u8* value_ptr, u16 valSize) {
                ensure(valSize == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload =
                    *reinterpret_cast<const Record*>(value_ptr);
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
    u8 folded_key[Record::maxFoldLength()];
    u16 folded_key_len = Record::foldKey(folded_key, key);
    // -------------------------------------------------------------------------------------
    OpCode ret;
    TID tid;
    ret = key_tid->lookup(folded_key, folded_key_len,
                          [&](const u8* payload, u16 payload_length) {
                            ensure(payload_length == sizeof(TID));
                            tid = *reinterpret_cast<const TID*>(payload);
                          });
    ensure(ret == OpCode::kOK);
    // -------------------------------------------------------------------------------------
    Field local_f;
    ret = tid_value->lookup((u8*)&tid, sizeof(TID),
                            [&](const u8* payload, u16 payload_length) {
                              ensure(payload_length == sizeof(Record));
                              const Record& typed_payload =
                                  *reinterpret_cast<const Record*>(payload);
                              local_f = (typed_payload).*f;
                            });
    ensure(ret == OpCode::kOK);
    moveIt(tid, folded_key, folded_key_len);
    return local_f;
  }
  // -------------------------------------------------------------------------------------
  u64 count() {
    return 0;
  }
};
