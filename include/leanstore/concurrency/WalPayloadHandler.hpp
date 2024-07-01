#pragma once

#include "leanstore/concurrency/GroupCommitter.hpp"
#include "leanstore/concurrency/Worker.hpp"

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

public:
  inline T* operator->() {
    return mWalPayload;
  }

  inline T& operator*() {
    return *mWalPayload;
  }

  void SubmitWal();
};

template <typename T>
inline void WalPayloadHandler<T>::SubmitWal() {
  cr::Worker::My().mLogging.SubmitWALEntryComplex(mTotalSize);
}

} // namespace leanstore::cr