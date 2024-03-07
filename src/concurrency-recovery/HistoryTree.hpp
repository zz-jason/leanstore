#pragma once

#include "HistoryTreeInterface.hpp"
#include "btree/BasicKV.hpp"
#include "leanstore/Units.hpp"

#include <cstdint>
#include <functional>
#include <vector>

namespace leanstore {
namespace cr {

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

using BasicKV = leanstore::storage::btree::BasicKV;

static constexpr uint16_t kWorkerLimit = std::numeric_limits<WORKERID>::max();

class HistoryTree : public HistoryTreeInterface {
private:
  struct alignas(64) Session {
    BufferFrame* mRightmostBf = nullptr;

    uint64_t mRightmostVersion = 0;

    int64_t mRightmostPos = -1;

    BufferFrame* mLeftMostBf = nullptr;

    uint64_t mLeftMostVersion = 0;

    TXID mLastTxId = 0;
  };

  std::vector<Session> mUpdateSessions;
  std::vector<Session> mRemoveSessions;

public:
  std::vector<BasicKV*> mUpdateBTrees;
  std::vector<BasicKV*> mRemoveBTrees;

  HistoryTree(WORKERID numWorkers)
      : mUpdateSessions(numWorkers),
        mRemoveSessions(numWorkers),
        mUpdateBTrees(numWorkers, nullptr),
        mRemoveBTrees(numWorkers, nullptr) {
  }

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
