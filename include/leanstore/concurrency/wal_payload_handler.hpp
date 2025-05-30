#pragma once

#include "leanstore/concurrency/group_committer.hpp"
#include "leanstore/concurrency/worker_context.hpp"

namespace leanstore::cr {

template <typename T>
class WalPayloadHandler {
public:
  /// payload of the active WAL
  T* wal_payload_;

  /// size of the whole WalEntry, including payloads
  uint64_t total_size_;

public:
  WalPayloadHandler() = default;

  ///  Initialize a WalPayloadHandler
  /// @param walPayload the WalPayload object, should already being initialized
  /// @param size the total size of the WalEntry
  WalPayloadHandler(T* wal_payload, uint64_t size) : wal_payload_(wal_payload), total_size_(size) {
  }

public:
  inline T* operator->() {
    return wal_payload_;
  }

  inline T& operator*() {
    return *wal_payload_;
  }

  void SubmitWal();
};

template <typename T>
inline void WalPayloadHandler<T>::SubmitWal() {
  cr::WorkerContext::My().logging_.SubmitWALEntryComplex(total_size_);
}

} // namespace leanstore::cr