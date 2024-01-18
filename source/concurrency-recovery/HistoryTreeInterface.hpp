#pragma once

#include "shared-headers/Units.hpp"

#include <functional>

namespace leanstore {
namespace cr {

using RemoveVersionCallback = std::function<void(
    const TXID, const TREEID, const u8*, u64, const bool visitedBefore)>;

class HistoryTreeInterface {
public:
  virtual ~HistoryTreeInterface() = default;

  virtual void PutVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                          TREEID treeId, bool isRemove, u64 payloadSize,
                          std::function<void(u8*)> cb,
                          bool sameThread = true) = 0;

  virtual bool GetVersion(
      WORKERID workerId, TXID txId, COMMANDID commandId, const bool isRemove,
      std::function<void(const u8*, u64 payloadSize)> cb) = 0;

  // represents the range of [from, to], both inclusive
  virtual void PurgeVersions(WORKERID workerId, TXID fromTxId, TXID toTxId,
                             RemoveVersionCallback cb, const u64 limit = 0) = 0;

  // represents the range of [from, to], both inclusive
  virtual void VisitRemovedVersions(WORKERID workerId, TXID fromTxId,
                                    TXID toTxId, RemoveVersionCallback cb) = 0;
};

} // namespace cr
} // namespace leanstore
