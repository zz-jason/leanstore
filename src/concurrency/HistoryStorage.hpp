#pragma once

#include "leanstore/Units.hpp"

#include <cstdint>
#include <functional>

namespace leanstore::storage::btree {
class BasicKV;
} // namespace leanstore::storage::btree

namespace leanstore::storage {
class BufferFrame;
} // namespace leanstore::storage

namespace leanstore::cr {

using RemoveVersionCallback =
    std::function<void(const TXID versionTxId, const TREEID treeId, const uint8_t* versionData,
                       uint64_t versionSize, const bool visitedBefore)>;

struct __attribute__((packed)) VersionMeta {
  bool mCalledBefore = false;

  TREEID mTreeId;

  uint8_t mPayload[];

public:
  inline static const VersionMeta* From(const uint8_t* buffer) {
    return reinterpret_cast<const VersionMeta*>(buffer);
  }

  inline static VersionMeta* From(uint8_t* buffer) {
    return reinterpret_cast<VersionMeta*>(buffer);
  }
};

class HistoryStorage {
private:
  struct alignas(64) Session {
    leanstore::storage::BufferFrame* mRightmostBf = nullptr;

    uint64_t mRightmostVersion = 0;

    int64_t mRightmostPos = -1;

    leanstore::storage::BufferFrame* mLeftMostBf = nullptr;

    uint64_t mLeftMostVersion = 0;

    TXID mLastTxId = 0;
  };

  //! Stores all the update versions generated by the current worker thread.
  leanstore::storage::btree::BasicKV* mUpdateIndex;

  //! Stores all the remove versions generated by the current worker thread.
  leanstore::storage::btree::BasicKV* mRemoveIndex;

  Session mUpdateSession;

  Session mRemoveSession;

public:
  HistoryStorage() : mUpdateIndex(nullptr), mRemoveIndex(nullptr) {
  }

  ~HistoryStorage() = default;

  leanstore::storage::btree::BasicKV* GetUpdateIndex() {
    return mUpdateIndex;
  }

  void SetUpdateIndex(leanstore::storage::btree::BasicKV* updateIndex) {
    mUpdateIndex = updateIndex;
  }

  leanstore::storage::btree::BasicKV* GetRemoveIndex() {
    return mRemoveIndex;
  }

  void SetRemoveIndex(leanstore::storage::btree::BasicKV* removeIndex) {
    mRemoveIndex = removeIndex;
  }

  void PutVersion(TXID txId, COMMANDID commandId, TREEID treeId, bool isRemove,
                  uint64_t payloadLength, std::function<void(uint8_t*)> cb, bool sameThread = true);

  bool GetVersion(TXID newerTxId, COMMANDID newerCommandId, const bool isRemoveCommand,
                  std::function<void(const uint8_t*, uint64_t)> cb);

  void PurgeVersions(TXID fromTxId, TXID toTxId, RemoveVersionCallback cb, const uint64_t limit);

  void VisitRemovedVersions(TXID fromTxId, TXID toTxId, RemoveVersionCallback cb);
};

} // namespace leanstore::cr
