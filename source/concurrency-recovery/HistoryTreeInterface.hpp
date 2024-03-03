#pragma once

#include "shared-headers/Units.hpp"

#include <functional>

namespace leanstore {
namespace cr {

using RemoveVersionCallback = std::function<void(
    const TXID versionTxId, const TREEID treeId, const uint8_t* versionData,
    uint64_t versionSize, const bool visitedBefore)>;

class HistoryTreeInterface {
public:
  virtual ~HistoryTreeInterface() = default;

  virtual void PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                          TREEID treeId, bool isRemove, uint64_t payloadSize,
                          std::function<void(uint8_t*)> cb,
                          bool sameThread = true) = 0;

  virtual bool GetVersion(
      WORKERID workerId, TXID txId, COMMANDID commandId, const bool isRemove,
      std::function<void(const uint8_t*, uint64_t payloadSize)> cb) = 0;

  // represents the range of [from, to], both inclusive
  virtual void PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                             RemoveVersionCallback cb,
                             const uint64_t limit = 0) = 0;

  // represents the range of [from, to], both inclusive
  virtual void VisitRemovedVersions(WORKERID workerId, TXID fromTxId,
                                    TXID toTxId, RemoveVersionCallback cb) = 0;
};

} // namespace cr
} // namespace leanstore
