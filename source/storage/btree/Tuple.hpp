#pragma once

#include "Units.hpp"
#include "storage/btree/core/BTreeExclusiveIterator.hpp"

#include <glog/logging.h>

namespace leanstore {
namespace storage {
namespace btree {

/// Plan: we should handle frequently and infrequently updated tuples
/// differently when it comes to maintaining versions in the b-tree. For
/// frequently updated tuples, we store them in a FatTuple
///
/// Prepartion phase: iterate over the chain and check whether all updated
/// attributes are the same and whether they fit on a page If both conditions
/// are fullfiled then we can store them in a fat tuple When FatTuple runs out
/// of space, we simply crash for now (real solutions approx variable-size pages
/// or fallback to chained keys)
///
/// How to convert CHAINED to FAT_TUPLE: Random number generation, similar to
/// contention split, don't eagerly remove the deltas to allow concurrent
/// readers to continue without complicating the logic if we fail
///
/// Glossary:
/// - UpdateDescriptor: (offset, length)[]
/// - Diff: raw bytes copied from src/dst next to each other according to the
///   descriptor Delta: WWTS + diff + (descriptor)?
enum class TupleFormat : u8 {
  CHAINED = 0,
  FAT = 1,
};

struct TupleFormatUtil {
  inline static std::string ToString(TupleFormat format) {
    switch (format) {
    case TupleFormat::CHAINED: {
      return "CHAINED";
    }
    case TupleFormat::FAT: {
      return "FAT";
    }
    }
    return "Unknown tuple format";
  }
};

// forward declaration
class FatTuple;
class ChainedTuple;

static constexpr COMMANDID INVALID_COMMANDID =
    std::numeric_limits<COMMANDID>::max();

/// The internal value format in BTreeVI.
///
/// NOTE: __attribute__((packed)) is used to avoid padding between field members
/// to avoid using space more than given.
class __attribute__((packed)) Tuple {
public:
  /// Format of the current tuple.
  TupleFormat mFormat;

  /// ID of the worker who created this tuple.
  WORKERID mWorkerId;

  /// ID of the transaction who created this tuple, it's the start timestamp of
  /// the transaction.
  TXID mTxId;

  /// ID of the command who created this tuple.
  COMMANDID mCommandId;

  /// Whether the tuple is locked for write.
  bool mWriteLocked : true;

public:
  Tuple(TupleFormat format, WORKERID workerId, TXID txId)
      : mFormat(format), mWorkerId(workerId), mTxId(txId),
        mCommandId(INVALID_COMMANDID) {
    mWriteLocked = false;
  }

public:
  bool IsWriteLocked() const {
    return mWriteLocked;
  }

  void WriteLock() {
    mWriteLocked = true;
  }

  void WriteUnlock() {
    mWriteLocked = false;
  }

public:
  inline static const Tuple* From(const u8* buffer) {
    return reinterpret_cast<const Tuple*>(buffer);
  }
};

/// A delta is consisted of the following parts:
/// 1. The creator info: mWorkerId, mTxId, mCommandId
/// 2. The update descriptor: payload
struct __attribute__((packed)) Delta {
public:
  /// ID of the worker who creates this delta.
  WORKERID mWorkerId;

  /// ID of the transaction who creates this delta.
  TXID mTxId;

  /// ID of the command who creates this delta.
  /// NOTE: Take care, otherwise we would overwrite another undo version
  COMMANDID mCommandId = INVALID_COMMANDID;

  /// Descriptor + Diff
  u8 payload[];

public:
  UpdateSameSizeInPlaceDescriptor& getDescriptor() {
    return *reinterpret_cast<UpdateSameSizeInPlaceDescriptor*>(payload);
  }

  const UpdateSameSizeInPlaceDescriptor& getDescriptor() const {
    return *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload);
  }

  inline u32 TotalSize() {
    const auto& desc = getDescriptor();
    return sizeof(Delta) + desc.TotalSize();
  }
};

/// We always append the descriptor, one format to keep simple
struct __attribute__((packed)) FatTuple : Tuple {
  /// Size of the base value.
  u16 mValSize = 0;

  u32 total_space = 0; // From the payload bytes array

  u32 used_space = 0; // does not include the struct itself

  u32 mDataOffset = 0;

  u16 mNumDeltas = 0; // Attention: coupled with used_space

  // value, Delta+Descriptor+Diff[] O2N
  u8 payload[];

public:
  FatTuple(const u32 totalSize)
      : Tuple(TupleFormat::FAT, 0, 0), total_space(totalSize),
        mDataOffset(totalSize) {
  }

  // returns false to fallback to chained mode
  static bool update(BTreeExclusiveIterator& iterator, Slice key, ValCallback,
                     UpdateSameSizeInPlaceDescriptor&);

  bool hasSpaceFor(const UpdateSameSizeInPlaceDescriptor&);

  void append(UpdateSameSizeInPlaceDescriptor&);

  Delta& allocateDelta(u32 totalDeltaSize);

  void garbageCollection();

  void undoLastUpdate();

  inline Slice GetValue() const {
    return Slice(GetValPtr(), mValSize);
  }

  inline u8* GetValPtr() {
    return payload;
  }

  inline const u8* GetValPtr() const {
    return payload;
  }

  /// Get the newest visible version of the current tuple, and apply the
  /// callback if the newest visible tuple is found.
  ///
  /// @return whether the tuple is found, and the number of visited versions
  std::tuple<OP_RESULT, u16> GetVisibleTuple(ValCallback valCallback) const;

  void convertToChained(TREEID treeId);

  void resize(u32 new_length);

  /// Used when converting to chained tuple. Deltas in chained tuple are stored
  /// N2O (from newest to oldest), but are stored O2N (from oldest to newest) in
  /// fat tuple. Reversing the deltas is a naive and simple solution.
  ///
  /// TODO(jian.z): verify whether it's expected to reverse the bytes instead of
  /// Deltas
  inline void ReverseDeltas() {
    std::reverse(getDeltaOffsets(), getDeltaOffsets() + mNumDeltas);
  }

private:
  inline Delta& getDelta(u16 i) {
    DCHECK(i < mNumDeltas);
    return *reinterpret_cast<Delta*>(payload + getDeltaOffsets()[i]);
  }

  inline const Delta& getDelta(u16 i) const {
    DCHECK(i < mNumDeltas);
    return *reinterpret_cast<const Delta*>(payload + getDeltaOffsets()[i]);
  }

  inline u16* getDeltaOffsets() {
    return reinterpret_cast<u16*>(payload + mValSize);
  }

  inline const u16* getDeltaOffsets() const {
    return reinterpret_cast<const u16*>(payload + mValSize);
  }

public:
  inline static const FatTuple* From(const u8* buffer) {
    return reinterpret_cast<const FatTuple*>(buffer);
  }
};

// Chained: only scheduled gc todos. FatTuple: eager pgc, no scheduled gc
// todos
struct __attribute__((packed)) ChainedTuple : Tuple {
  u16 updates_counter = 0;

  u16 oldest_tx = 0;

  u8 is_removed = 1;

  u8 payload[]; // latest version in-place

public:
  /// Construct a ChainedTuple, copy the value to its payload
  ///
  /// NOTE: Payload space should be allocated in advance. This constructor is
  /// usually called by a placmenet new operator.
  ChainedTuple(WORKERID workerId, TXID txId, Slice val)
      : Tuple(TupleFormat::CHAINED, workerId, txId), is_removed(false) {
    std::memcpy(payload, val.data(), val.size());
  }

  /// Construct a ChainedTuple from an existing FatTuple, the new ChainedTuple
  /// may share the same space with the input FatTuple, so std::memmove is used
  /// to handle the overlap bytes.
  ///
  /// NOTE: This constructor is usually called by a placmenet new operator on
  /// the address of the FatTuple
  ChainedTuple(FatTuple& oldFatTuple)
      : Tuple(TupleFormat::CHAINED, oldFatTuple.mWorkerId, oldFatTuple.mTxId),
        is_removed(false) {
    mCommandId = oldFatTuple.mCommandId;
    std::memmove(payload, oldFatTuple.payload, oldFatTuple.mValSize);
  }

public:
  inline Slice GetValue(size_t size) const {
    return Slice(payload, size);
  }

  bool isFinal() const {
    return mCommandId == INVALID_COMMANDID;
  }

  void reset() {
  }

public:
  inline static const ChainedTuple* From(const u8* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }
};

static_assert(sizeof(FatTuple) >= sizeof(ChainedTuple));

} // namespace btree
} // namespace storage
} // namespace leanstore