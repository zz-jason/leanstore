#pragma once

#include "leanstore/concurrency/WorkerContext.hpp"

namespace leanstore::cr {

template <typename T>
class WalPayloadHandler {
public:
  //! payload of the active WAL
  T* mWalPayload;

  //! size of the whole WalEntry, including payloads
  uint64_t mTotalSize;

public:
  WalPayloadHandler() = default;

  //! @brief Initialize a WalPayloadHandler
  //! @param walPayload the WalPayload object, should already being initialized
  //! @param size the total size of the WalEntry
  WalPayloadHandler(T* walPayload, uint64_t size) : mWalPayload(walPayload), mTotalSize(size) {
  }

  T* operator->() {
    return mWalPayload;
  }

  T& operator*() {
    return *mWalPayload;
  }

  void SubmitWal() {
    cr::WorkerContext::My().mLogging.mWalBuffer.Advance(mTotalSize);
  }
};

} // namespace leanstore::cr