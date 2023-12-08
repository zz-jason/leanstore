#pragma once

#include "Exceptions.hpp"
#include "Units.hpp"
#include "Config.hpp"
#include "KVInterface.hpp"
#include "utils/Misc.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace leanstore {
namespace cr {

using RemoveVersionCallback = std::function<void(
    const TXID, const TREEID, const u8*, u64, const bool visited_before)>;

class HistoryTreeInterface {
public:
  virtual void insertVersion(WORKERID workerId, TXID tx_id,
                             COMMANDID command_id, TREEID treeId,
                             bool is_remove, u64 payload_length,
                             std::function<void(u8*)> cb,
                             bool same_thread = true) = 0;

  virtual bool retrieveVersion(
      WORKERID workerId, TXID tx_id, COMMANDID command_id, const bool is_remove,
      std::function<void(const u8*, u64 payload_length)> cb) = 0;

  virtual void purgeVersions(WORKERID workerId, TXID from_tx_id, TXID to_tx_id,
                             RemoveVersionCallback cb, const u64 limit = 0) = 0;

  virtual void visitRemoveVersions(WORKERID workerId, TXID from_tx_id,
                                   TXID to_tx_id,
                                   RemoveVersionCallback cb) = 0; // [from, to]
};

} // namespace cr
} // namespace leanstore
