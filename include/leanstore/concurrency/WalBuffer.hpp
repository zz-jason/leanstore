#pragma once

#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency/WalEntry.hpp"
#include "leanstore/utils/AsyncIo.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/UserThread.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>

#include <fcntl.h>

namespace leanstore::cr {

class WalBuffer {
private:
  //! The wal buffer.
  utils::AlignedBuffer<512> mBuffer;

  //! Capacity of the wal buffer.
  const uint64_t mCapacity;

  //! Position of buffered bytes;
  uint64_t mPosBuffered;

  //! Position of the persisted bytes;
  uint64_t mPosPersisted;

  const std::string mWalFilePath;

  int32_t mWalFd;

  uint64_t mWalFileSize;

  utils::AsyncIo mAio;

public:
  WalBuffer(uint64_t capacity, std::string walFilePath, uint64_t ioBatchSize,
            bool createFromScratch)
      : mBuffer(capacity),
        mCapacity(capacity),
        mPosBuffered(0),
        mPosPersisted(0),
        mWalFilePath(std::move(walFilePath)),
        mWalFileSize(0),
        mAio(ioBatchSize) {
    // clear buffer
    std::memset(mBuffer.Get(), 0, mCapacity);

    // open wal file
    int flags = createFromScratch ? O_TRUNC | O_CREAT | O_RDWR | O_DIRECT : O_RDWR | O_DIRECT;
    mWalFd = open(mWalFilePath.c_str(), flags, 0666);
    if (mWalFd == -1) {
      Log::Fatal("Could not open file at: {}, error: {}", mWalFilePath, strerror(errno));
    }
    Log::Info("Init walFd succeed, walFd={}, walFile={}", mWalFd, mWalFilePath);
  }

  ~WalBuffer() {
    if (close(mWalFd) == -1) {
      Log::Error("Close WAL file failed: {}, walFile={}", strerror(errno), mWalFilePath);
    } else {
      Log::Info("Close WAL file succeed, walFile={}", mWalFilePath);
    }
  }

  //! Get a contiguous buffer of the given size, return the buffer address.
  uint8_t* Get(uint64_t size) {
    ensureContiguousBuffer(size);
    return mBuffer.Get() + mPosBuffered;
  }

  //! Advance the buffer pointer by the given size.
  void Advance(uint64_t size) {
    mPosBuffered += size;
  }

  //! Persist all the buffered bytes to the wal file. Called when the transaction is committed,
  //! aborted, or no more space in the buffer.
  void Persist();

  //! Iterate all the wal records in the descending order. It firstly iterates the buffered records
  //! and then the persisted records.
  void VisitWalRecordsDesc(std::function<void(const WalEntry& entry)> callback [[maybe_unused]]) {
    // TODO: implement
  }

private:
  void ensureContiguousBuffer(uint64_t size);

  void carriageReturn();

  void persistRange(uint64_t lowerBufferPos, uint64_t upperBufferPos);

  void writeToWalFile();

}; // WalBuffer

inline void WalBuffer::Persist() {
  if (mPosPersisted == mPosBuffered) {
    return;
  }

  if (mPosPersisted < mPosBuffered) {
    persistRange(mPosPersisted, mPosBuffered);
  } else {
    persistRange(mPosPersisted, mCapacity);
    persistRange(0, mPosBuffered);
  }

  if (!mAio.IsEmpty()) {
    writeToWalFile();
  }

  mPosPersisted = mPosBuffered;
}

inline void WalBuffer::ensureContiguousBuffer(uint64_t size) {
  while (true) {
    if (mPosPersisted <= mPosBuffered) {
      if (mCapacity - mPosBuffered >= size) {
        return;
      }

      // not enough space
      carriageReturn();
      if (mPosPersisted == 0) {
        Persist();
      }
      mPosBuffered = 0;
      continue;
    }

    if (mPosPersisted - mPosBuffered >= size) {
      return;
    }

    // not enough space
    Persist();
    continue;
  }
}

inline void WalBuffer::carriageReturn() {
  auto entrySize = mCapacity - mPosBuffered;
  auto* entryPtr = mBuffer.Get() + mPosBuffered;
  new (entryPtr) WalCarriageReturn(entrySize);
  mPosBuffered = mCapacity;
}

inline void WalBuffer::persistRange(uint64_t lowerBufferPos, uint64_t upperBufferPos) {
  static constexpr uint64_t kAligment = 4096;
  auto fileOffsetAligned = utils::AlignDown(mWalFileSize, kAligment);
  auto bufferOffsetAligned = utils::AlignDown(lowerBufferPos, kAligment);
  auto upperAligned = utils::AlignUp(upperBufferPos, kAligment);
  while (bufferOffsetAligned < upperAligned) {
    if (mAio.IsFull()) {
      writeToWalFile();
    }
    mAio.PrepareWrite(mWalFd, mBuffer.Get() + bufferOffsetAligned, kAligment, fileOffsetAligned);
    bufferOffsetAligned += kAligment;
    fileOffsetAligned += kAligment;
  }
  mWalFileSize += upperBufferPos - lowerBufferPos;
}

inline void WalBuffer::writeToWalFile() {
  if (auto res = mAio.SubmitAll(); !res) {
    Log::Error("Failed to submit all IO, error={}", res.error().ToString());
  }

  timespec timeout = {1, 0}; // 1s
  if (auto res = mAio.WaitAll(&timeout); !res) {
    Log::Error("Failed to wait all IO, error={}", res.error().ToString());
  }

  if (utils::tlsStore->mStoreOption->mEnableWalFsync) {
    auto failed = fdatasync(mWalFd);
    if (failed) {
      Log::Error("fdatasync failed, errno={}, error={}", errno, strerror(errno));
    }
  }
}

} // namespace leanstore::cr