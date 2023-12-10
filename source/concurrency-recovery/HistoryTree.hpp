#pragma once

#include "Config.hpp"
#include "Exceptions.hpp"
#include "HistoryTreeInterface.hpp"
#include "KVInterface.hpp"
#include "Units.hpp"
#include "storage/btree/BTreeLL.hpp"
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

struct __attribute__((packed)) VersionMeta {
  bool called_before = false;
  TREEID mTreeId;
  u8 payload[];
};

using BTreeLL = leanstore::storage::btree::BTreeLL;

class HistoryTree : public HistoryTreeInterface {
private:
  struct alignas(64) Session {
    BufferFrame *rightmost_bf, *leftmost_bf;
    u64 rightmost_version, leftmost_version;
    s64 rightmost_pos = -1;
    TXID last_tx_id;
    bool rightmost_init = false, leftmost_init = false;
  };
  Session update_sessions[leanstore::cr::STATIC_MAX_WORKERS];
  Session remove_sessions[leanstore::cr::STATIC_MAX_WORKERS];

public:
  std::unique_ptr<BTreeLL*[]> update_btrees;
  std::unique_ptr<BTreeLL*[]> remove_btrees;

  virtual void insertVersion(WORKERID workerId, TXID tx_id,
                             COMMANDID command_id, TREEID treeId,
                             bool is_remove, u64 payload_length,
                             std::function<void(u8*)> cb,
                             bool same_thread) override;

  virtual bool retrieveVersion(
      WORKERID workerId, TXID tx_id, COMMANDID command_id, const bool is_remove,
      std::function<void(const u8*, u64 payload_length)> cb) override;

  virtual void purgeVersions(WORKERID workerId, TXID from_tx_id, TXID to_tx_id,
                             RemoveVersionCallback cb,
                             const u64 limit) override;

  // [from, to]
  virtual void visitRemoveVersions(WORKERID workerId, TXID from_tx_id,
                                   TXID to_tx_id,
                                   RemoveVersionCallback cb) override;
};

} // namespace cr
} // namespace leanstore
