#pragma once

#include "HistoryTreeInterface.hpp"
#include "concurrency-recovery/Worker.hpp"
#include "shared-headers/Units.hpp"
#include "storage/btree/BasicKV.hpp"

#include <functional>

namespace leanstore {
namespace cr {

struct __attribute__((packed)) VersionMeta {
  bool called_before = false;
  TREEID mTreeId;
  u8 payload[];

public:
  inline static const VersionMeta* From(const u8* buffer) {
    return reinterpret_cast<const VersionMeta*>(buffer);
  }

  inline static VersionMeta* From(u8* buffer) {
    return reinterpret_cast<VersionMeta*>(buffer);
  }
};

using BasicKV = leanstore::storage::btree::BasicKV;

class HistoryTree : public HistoryTreeInterface {
private:
  struct alignas(64) Session {
    BufferFrame* rightmost_bf;
    BufferFrame* leftmost_bf;
    u64 rightmost_version;
    u64 leftmost_version;
    s64 rightmost_pos = -1;
    TXID last_tx_id;
    bool rightmost_init = false;
    bool leftmost_init = false;
  };
  Session mUpdateSessions[leanstore::cr::kWorkerLimit];
  Session mRemoveSessions[leanstore::cr::kWorkerLimit];

public:
  std::unique_ptr<BasicKV*[]> mUpdateBTrees;
  std::unique_ptr<BasicKV*[]> mRemoveBTrees;

  virtual ~HistoryTree() = default;

  virtual void PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                          TREEID treeId, bool isRemove, u64 payloadLength,
                          std::function<void(u8*)> cb,
                          bool sameThread) override;

  virtual bool GetVersion(
      WORKERID workerId, TXID txId, COMMANDID commandId, const bool isRemove,
      std::function<void(const u8*, u64 payloadLength)> cb) override;

  virtual void PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                             RemoveVersionCallback cb,
                             const u64 limit) override;

  // [from, to]
  virtual void VisitRemovedVersions(WORKERID workerId, TXID fromTxId,
                                    TXID toTxId,
                                    RemoveVersionCallback cb) override;
};

} // namespace cr
} // namespace leanstore
