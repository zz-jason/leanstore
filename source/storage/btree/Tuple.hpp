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
///   descriptor FatTupleDelta: WWTS + diff + (descriptor)?

// Forward declaration
class FatTuple;
class ChainedTuple;

static constexpr COMMANDID INVALID_COMMANDID =
    std::numeric_limits<COMMANDID>::max();

// -----------------------------------------------------------------------------
// TupleFormat
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Tuple
// -----------------------------------------------------------------------------

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
  inline bool IsWriteLocked() const {
    return mWriteLocked;
  }

  inline void WriteLock() {
    mWriteLocked = true;
  }

  inline void WriteUnlock() {
    mWriteLocked = false;
  }

public:
  inline static Tuple* From(u8* buffer) {
    return reinterpret_cast<Tuple*>(buffer);
  }

  inline static const Tuple* From(const u8* buffer) {
    return reinterpret_cast<const Tuple*>(buffer);
  }

  static bool ToChainedTuple(BTreeExclusiveIterator& iterator);
};

// -----------------------------------------------------------------------------
// FatTupleDelta
// -----------------------------------------------------------------------------

/// FatTupleDelta is the delta changes made to the previous version, a delta
/// is consisted of the following parts:
/// 1. The creator info: mWorkerId, mTxId, mCommandId
/// 2. The update descriptor: payload
///
/// Data loyout of a FatTupleDelta:
/// | mWorkerId | mTxId | mCommandId | UpdateDesc | Diff |
///
/// FatTuple: eager pgc, no scheduled gc todos
struct __attribute__((packed)) FatTupleDelta {
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
  FatTupleDelta() = default;

  FatTupleDelta(WORKERID workerId, TXID txId, COMMANDID commandId,
                const u8* buf, u32 size)
      : mWorkerId(workerId), mTxId(txId), mCommandId(commandId) {
    std::memcpy(payload, buf, size);
  }

public:
  inline UpdateDesc& getDescriptor() {
    return *reinterpret_cast<UpdateDesc*>(payload);
  }

  inline const UpdateDesc& getDescriptor() const {
    return *reinterpret_cast<const UpdateDesc*>(payload);
  }

  inline u32 TotalSize() {
    const auto& desc = getDescriptor();
    return sizeof(FatTupleDelta) + desc.TotalSize();
  }
};

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

/// FatTuple stores all the versions in the value. Layout of FatTuple:
///
/// | FatTuple meta |  newest value | FatTupleDelta O2N |
///
struct __attribute__((packed)) FatTuple : Tuple {
  /// Size of the base value.
  u16 mValSize = 0;

  u32 total_space = 0; // Size of the payload bytes array

  u32 used_space = 0; // does not include the struct itself

  u32 mDataOffset = 0;

  u16 mNumDeltas = 0; // Attention: coupled with used_space

  // value, FatTupleDelta+Descriptor+Diff[] O2N
  u8 payload[];

public:
  FatTuple(u32 payloadSize)
      : Tuple(TupleFormat::FAT, 0, 0), total_space(payloadSize),
        mDataOffset(payloadSize) {
  }

  FatTuple(u32 payloadSize, u32 valSize, const ChainedTuple& chainedTuple);

  // returns false to fallback to chained mode
  static bool update(BTreeExclusiveIterator& iterator, Slice key, ValCallback,
                     UpdateDesc&);

  bool hasSpaceFor(const UpdateDesc&);

  void append(UpdateDesc&);

  template <typename... Args>
  FatTupleDelta& NewDelta(u32 totalDeltaSize, Args&&... args);

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

  /// Used when converting to chained tuple. Deltas in chained tuple are
  /// stored N2O (from newest to oldest), but are stored O2N (from oldest to
  /// newest) in fat tuple. Reversing the deltas is a naive and simple
  /// solution.
  ///
  /// TODO(jian.z): verify whether it's expected to reverse the bytes instead
  /// of Deltas
  inline void ReverseDeltas() {
    std::reverse(getDeltaOffsets(), getDeltaOffsets() + mNumDeltas);
  }

private:
  inline FatTupleDelta& getDelta(u16 i) {
    DCHECK(i < mNumDeltas);
    return *reinterpret_cast<FatTupleDelta*>(payload + getDeltaOffsets()[i]);
  }

  inline const FatTupleDelta& getDelta(u16 i) const {
    DCHECK(i < mNumDeltas);
    return *reinterpret_cast<const FatTupleDelta*>(payload +
                                                   getDeltaOffsets()[i]);
  }

  inline u16* getDeltaOffsets() {
    return reinterpret_cast<u16*>(payload + mValSize);
  }

  inline const u16* getDeltaOffsets() const {
    return reinterpret_cast<const u16*>(payload + mValSize);
  }

public:
  inline static FatTuple* From(u8* buffer) {
    return reinterpret_cast<FatTuple*>(buffer);
  }

  inline static const FatTuple* From(const u8* buffer) {
    return reinterpret_cast<const FatTuple*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// DanglingPointer
// -----------------------------------------------------------------------------

struct __attribute__((packed)) DanglingPointer {
  BufferFrame* bf = nullptr;
  u64 latch_version_should_be = -1;
  s32 head_slot = -1;
};

// -----------------------------------------------------------------------------
// Version
// -----------------------------------------------------------------------------

struct __attribute__((packed)) Version {
  enum class TYPE : u8 { UPDATE, REMOVE };

  TYPE type;

  WORKERID mWorkerId;

  TXID mTxId;

  COMMANDID mCommandId;

  Version(TYPE type, WORKERID workerId, TXID txId, COMMANDID commandId)
      : type(type), mWorkerId(workerId), mTxId(txId), mCommandId(commandId) {
  }
};

// -----------------------------------------------------------------------------
// UpdateVersion
// -----------------------------------------------------------------------------

struct __attribute__((packed)) UpdateVersion : Version {
  u8 is_delta : 1;
  u8 payload[]; // UpdateDescriptor + Diff

  UpdateVersion(WORKERID workerId, TXID txId, COMMANDID commandId, bool isDelta)
      : Version(Version::TYPE::UPDATE, workerId, txId, commandId),
        is_delta(isDelta) {
  }

  UpdateVersion(const FatTupleDelta& delta, u64 deltaPayloadSize)
      : Version(Version::TYPE::UPDATE, delta.mWorkerId, delta.mTxId,
                delta.mCommandId),
        is_delta(true) {
    std::memcpy(payload, delta.payload, deltaPayloadSize);
  }

  bool isFinal() const {
    return mCommandId == 0;
  }

public:
  inline static const UpdateVersion* From(const u8* buffer) {
    return reinterpret_cast<const UpdateVersion*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// RemoveVersion
// -----------------------------------------------------------------------------

struct __attribute__((packed)) RemoveVersion : Version {
public:
  u16 mKeySize;
  u16 mValSize;
  DanglingPointer dangling_pointer;
  bool moved_to_graveway = false;
  u8 payload[]; // Key + Value

public:
  RemoveVersion(WORKERID workerId, TXID txId, COMMANDID commandId, u16 keySize,
                u16 valSize)
      : Version(Version::TYPE::REMOVE, workerId, txId, commandId),
        mKeySize(keySize), mValSize(valSize) {
  }

public:
  inline static const RemoveVersion* From(const u8* buffer) {
    return reinterpret_cast<const RemoveVersion*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// ChainedTuple
// -----------------------------------------------------------------------------

/// History versions of chained tuple are stored in the history tree of the
/// current worker thread.
/// Chained: only scheduled gc todos.
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
  /// may share the same space with the input FatTuple, so std::memmove is
  /// used to handle the overlap bytes.
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

  std::tuple<OP_RESULT, u16> GetVisibleTuple(Slice payload,
                                             ValCallback callback) const;

  bool isFinal() const {
    return mCommandId == INVALID_COMMANDID;
  }

  void reset() {
  }

public:
  inline static const ChainedTuple* From(const u8* buffer) {
    return reinterpret_cast<const ChainedTuple*>(buffer);
  }

  inline static ChainedTuple* From(u8* buffer) {
    return reinterpret_cast<ChainedTuple*>(buffer);
  }
};

static_assert(sizeof(FatTuple) >= sizeof(ChainedTuple));

} // namespace btree
} // namespace storage
} // namespace leanstore