#pragma once

#include "concurrency/GroupCommitter.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Exceptions.hpp"
#include "utils/Defer.hpp"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace leanstore::cr {

template <typename T> class WalPayloadHandler {
public:
  // payload of the active WAL
  T* mWalPayload;

  // size of the whole WalEntry, including payloads
  uint64_t mTotalSize;

public:
  WalPayloadHandler() = default;

  /// @brief Initialize a WalPayloadHandler
  /// @param walPayload the WalPayload object, should already being initialized
  /// @param size the total size of the WalEntry
  WalPayloadHandler(T* walPayload, uint64_t size)
      : mWalPayload(walPayload),
        mTotalSize(size) {
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

template <typename T> inline void WalPayloadHandler<T>::SubmitWal() {
  SCOPED_DEFER(DEBUG_BLOCK(){
      // rapidjson::Document walDoc(rapidjson::kObjectType);
      // auto* walEntry = cr::Worker::My().mLogging.mActiveWALEntryComplex;
      // WalEntry::ToJson(walEntry, &walDoc);

      // rapidjson::StringBuffer buffer;
      // rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      // walDoc.Accept(writer);
      // LS_DLOG("SubmitWal, workerId={}, startTs={}, curGsn={}, walJson={}",
      //            Worker::My().mWorkerId, Worker::My().mActiveTx.mStartTs,
      //            Worker::My().mLogging.GetCurrentGsn(),
      //            WalEntry::ToJsonString(mWalPayload));
  });

  cr::Worker::My().mLogging.SubmitWALEntryComplex(mTotalSize);
}

} // namespace leanstore::cr