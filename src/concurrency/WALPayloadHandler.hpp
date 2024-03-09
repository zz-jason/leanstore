#pragma once

#include "concurrency/GroupCommitter.hpp"
#include "concurrency/Transaction.hpp"
#include "concurrency/WALEntry.hpp"
#include "concurrency/Worker.hpp"
#include "leanstore/Exceptions.hpp"
#include "utils/Defer.hpp"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <glog/logging.h>

#include <iostream>

namespace leanstore::cr {

template <typename T> class WALPayloadHandler {
public:
  // payload of the active WAL
  T* mWalPayload;

  // size of the whole WALEntry, including payloads
  uint64_t mTotalSize;

public:
  WALPayloadHandler() = default;

  /// @brief Initialize a WALPayloadHandler
  /// @param walPayload the WalPayload object, should already being initialized
  /// @param size the total size of the WALEntry
  WALPayloadHandler(T* walPayload, uint64_t size)
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

template <typename T> inline void WALPayloadHandler<T>::SubmitWal() {
  SCOPED_DEFER(DEBUG_BLOCK() {
    auto walDoc = cr::Worker::My().mLogging.mActiveWALEntryComplex->ToJson();
    auto entry = reinterpret_cast<T*>(
        cr::Worker::My().mLogging.mActiveWALEntryComplex->mPayload);
    auto payloadDoc = entry->ToJson();
    walDoc->AddMember("payload", *payloadDoc, walDoc->GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    walDoc->Accept(writer);
    LOG(INFO) << "SubmitWal"
              << ", workerId=" << Worker::My().mWorkerId
              << ", startTs=" << Worker::My().mActiveTx.mStartTs
              << ", curGSN=" << Worker::My().mLogging.GetCurrentGsn()
              << ", walJson=" << buffer.GetString();
  });

  cr::Worker::My().mLogging.SubmitWALEntryComplex(mTotalSize);
}

} // namespace leanstore::cr