#pragma once

#include "HistoryTreeInterface.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "leanstore/Units.hpp"
#include "btree/BasicKV.hpp"

#include <functional>

namespace leanstore {
namespace cr {

struct __attribute__((packed)) VersionMeta {
  bool called_before = false;
  TREEID mTreeId;
  uint8_t payload[];

public:
  inline static const VersionMeta* From(const uint8_t* buffer) {
    return reinterpret_cast<const VersionMeta*>(buffer);
  }

  inline static VersionMeta* From(uint8_t* buffer) {
    return reinterpret_cast<VersionMeta*>(buffer);
  }
};

using BasicKV = leanstore::storage::btree::BasicKV;

static constexpr uint16_t kWorkerLimit = std::numeric_limits<WORKERID>::max();

class HistoryTree : public HistoryTreeInterface {
private:
  struct alignas(64) Session {
    bool mRightmostInited = false;
    BufferFrame* mRightmostBf = nullptr;
    uint64_t mRightmostVersion = 0;
    int64_t mRightmostPos = -1;

    bool leftmost_init = false;
    BufferFrame* leftmost_bf = nullptr;
    uint64_t leftmost_version = 0;

    TXID mLastTxId = 0;
  };
  Session mUpdateSessions[leanstore::cr::kWorkerLimit];
  Session mRemoveSessions[leanstore::cr::kWorkerLimit];

public:
  std::unique_ptr<BasicKV*[]> mUpdateBTrees;
  std::unique_ptr<BasicKV*[]> mRemoveBTrees;

  virtual ~HistoryTree() = default;

  virtual void PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                          TREEID treeId, bool isRemove, uint64_t payloadLength,
                          std::function<void(uint8_t*)> cb,
                          bool sameThread) override;

  virtual bool GetVersion(
      WORKERID workerId, TXID txId, COMMANDID commandId, const bool isRemove,
      std::function<void(const uint8_t*, uint64_t payloadLength)> cb) override;

  virtual void PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                             RemoveVersionCallback cb,
                             const uint64_t limit) override;

  // [from, to]
  virtual void VisitRemovedVersions(WORKERID workerId, TXID fromTxId,
                                    TXID toTxId,
                                    RemoveVersionCallback cb) override;
};

} // namespace cr
} // namespace leanstore
