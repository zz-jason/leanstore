#pragma once

#include "btree/core/BTreePessimisticExclusiveIterator.hpp"
#include "leanstore/Units.hpp"
#include "utils/Log.hpp"

namespace leanstore::storage::btree {
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
/// How to convert kChained to FAT_TUPLE: Random number generation, similar to
/// contention split, don't eagerly remove the deltas to allow concurrent
/// readers to continue without complicating the logic if we fail
///
/// Glossary:
/// - UpdateDescriptor: (offset, length)[]
/// - Delta: raw bytes copied from src/dst next to each other according to the
///   descriptor FatTupleDelta: WWTS + Delta + (descriptor)?

// Forward declaration
class FatTuple;
class ChainedTuple;

static constexpr COMMANDID kInvalidCommandid =
    std::numeric_limits<COMMANDID>::max();

// -----------------------------------------------------------------------------
// TupleFormat
// -----------------------------------------------------------------------------

enum class TupleFormat : uint8_t {
  kChained = 0,
  kFat = 1,
};

struct TupleFormatUtil {
  inline static std::string ToString(TupleFormat format) {
    switch (format) {
    case TupleFormat::kChained: {
      return "kChained";
    }
    case TupleFormat::kFat: {
      return "kFat";
    }
    }
    return "Unknown tuple format";
  }
};

// -----------------------------------------------------------------------------
// Tuple
// -----------------------------------------------------------------------------

/// The internal value format in TransactionKV.
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
  bool mWriteLocked;

public:
  Tuple(TupleFormat format, WORKERID workerId, TXID txId,
        COMMANDID commandId = kInvalidCommandid, bool writeLocked = false)
      : mFormat(format),
        mWorkerId(workerId),
        mTxId(txId),
        mCommandId(commandId),
        mWriteLocked(writeLocked) {
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
  inline static Tuple* From(uint8_t* buffer) {
    return reinterpret_cast<Tuple*>(buffer);
  }

  inline static const Tuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const Tuple*>(buffer);
  }

  static bool ToFat(BTreePessimisticExclusiveIterator& iterator);
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
/// | mWorkerId | mTxId | mCommandId | UpdateDesc | Delta |
///
/// FatTuple uses precise garbage collection
struct __attribute__((packed)) FatTupleDelta {
public:
  /// ID of the worker who creates this delta.
  WORKERID mWorkerId;

  /// ID of the transaction who creates this delta.
  TXID mTxId;

  /// ID of the command who creates this delta.
  /// NOTE: Take care, otherwise we would overwrite another undo version
  COMMANDID mCommandId = kInvalidCommandid;

  /// Descriptor + Delta
  uint8_t mPayload[];

public:
  FatTupleDelta() = default;

  FatTupleDelta(WORKERID workerId, TXID txId, COMMANDID commandId,
                const uint8_t* buf, uint32_t size)
      : mWorkerId(workerId),
        mTxId(txId),
        mCommandId(commandId) {
    std::memcpy(mPayload, buf, size);
  }

public:
  inline UpdateDesc& GetUpdateDesc() {
    return *reinterpret_cast<UpdateDesc*>(mPayload);
  }

  inline const UpdateDesc& GetUpdateDesc() const {
    return *reinterpret_cast<const UpdateDesc*>(mPayload);
  }

  inline uint8_t* GetDeltaPtr() {
    return mPayload + GetUpdateDesc().Size();
  }

  inline const uint8_t* GetDeltaPtr() const {
    return mPayload + GetUpdateDesc().Size();
  }

  inline uint32_t TotalSize() {
    const auto& updateDesc = GetUpdateDesc();
    return sizeof(FatTupleDelta) + updateDesc.SizeWithDelta();
  }
};

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

/// FatTuple stores all the versions in the value. Layout of FatTuple:
///
/// | FatTuple meta |  newest value | FatTupleDelta O2N |
///
class __attribute__((packed)) FatTuple : public Tuple {
public:
  /// Size of the newest value.
  uint16_t mValSize = 0;

  /// Capacity of the payload
  uint32_t mPayloadCapacity = 0;

  /// Space used of the payload
  uint32_t mPayloadSize = 0;

  uint32_t mDataOffset = 0;

  uint16_t mNumDeltas = 0; // Attention: coupled with mPayloadSize

  // value, FatTupleDelta+Descriptor+Delta[] O2N
  uint8_t mPayload[];

public:
  FatTuple(uint32_t payloadCapacity)
      : Tuple(TupleFormat::kFat, 0, 0),
        mPayloadCapacity(payloadCapacity),
        mDataOffset(payloadCapacity) {
  }

  FatTuple(uint32_t payloadCapacity, uint32_t valSize,
           const ChainedTuple& chainedTuple);

  bool HasSpaceFor(const UpdateDesc&);

  void Append(UpdateDesc&);

  template <typename... Args>
  FatTupleDelta& NewDelta(uint32_t totalDeltaSize, Args&&... args);

  void GarbageCollection();

  void UndoLastUpdate();

  inline MutableSlice GetMutableValue() {
    return MutableSlice(GetValPtr(), mValSize);
  }

  inline Slice GetValue() const {
    return Slice(GetValPtr(), mValSize);
  }

  inline uint8_t* GetValPtr() {
    return mPayload;
  }

  inline const uint8_t* GetValPtr() const {
    return mPayload;
  }

  /// Get the newest visible version of the current tuple, and apply the
  /// callback if the newest visible tuple is found.
  ///
  /// @return whether the tuple is found, and the number of visited versions
  std::tuple<OpCode, uint16_t> GetVisibleTuple(ValCallback valCallback) const;

  void ConvertToChained(TREEID treeId);

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
  void resize(uint32_t newSize);

  inline FatTupleDelta& getDelta(uint16_t i) {
    Log::DebugCheck(i < mNumDeltas);
    return *reinterpret_cast<FatTupleDelta*>(mPayload + getDeltaOffsets()[i]);
  }

  inline const FatTupleDelta& getDelta(uint16_t i) const {
    Log::DebugCheck(i < mNumDeltas);
    return *reinterpret_cast<const FatTupleDelta*>(mPayload +
                                                   getDeltaOffsets()[i]);
  }

  inline uint16_t* getDeltaOffsets() {
    return reinterpret_cast<uint16_t*>(mPayload + mValSize);
  }

  inline const uint16_t* getDeltaOffsets() const {
    return reinterpret_cast<const uint16_t*>(mPayload + mValSize);
  }

public:
  inline static FatTuple* From(uint8_t* buffer) {
    return reinterpret_cast<FatTuple*>(buffer);
  }

  inline static const FatTuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const FatTuple*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// DanglingPointer
// -----------------------------------------------------------------------------

struct __attribute__((packed)) DanglingPointer {
  BufferFrame* mBf = nullptr;

  uint64_t mLatchVersionShouldBe = -1;

  int32_t mHeadSlot = -1;

public:
  DanglingPointer() = default;

  DanglingPointer(const BTreePessimisticExclusiveIterator& xIter)
      : mBf(xIter.mGuardedLeaf.mBf),
        mLatchVersionShouldBe(xIter.mGuardedLeaf.mGuard.mVersion),
        mHeadSlot(xIter.mSlotId) {
  }
};

// -----------------------------------------------------------------------------
// Version
// -----------------------------------------------------------------------------

enum class VersionType : uint8_t { kUpdate, kInsert, kRemove };

struct __attribute__((packed)) Version {
public:
  VersionType mType;

  WORKERID mWorkerId;

  TXID mTxId;

  COMMANDID mCommandId;

  Version(VersionType type, WORKERID workerId, TXID txId, COMMANDID commandId)
      : mType(type),
        mWorkerId(workerId),
        mTxId(txId),
        mCommandId(commandId) {
  }
};

// -----------------------------------------------------------------------------
// UpdateVersion
// -----------------------------------------------------------------------------

struct __attribute__((packed)) UpdateVersion : Version {
  uint8_t mIsDelta = 1;

  uint8_t mPayload[]; // UpdateDescriptor + Delta

public:
  UpdateVersion(WORKERID workerId, TXID txId, COMMANDID commandId, bool isDelta)
      : Version(VersionType::kUpdate, workerId, txId, commandId),
        mIsDelta(isDelta) {
  }

  UpdateVersion(const FatTupleDelta& delta, uint64_t deltaPayloadSize)
      : Version(VersionType::kUpdate, delta.mWorkerId, delta.mTxId,
                delta.mCommandId),
        mIsDelta(true) {
    std::memcpy(mPayload, delta.mPayload, deltaPayloadSize);
  }

public:
  inline static const UpdateVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const UpdateVersion*>(buffer);
  }
};

struct __attribute__((packed)) InsertVersion : Version {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  // Key + Value
  uint8_t mPayload[];

public:
  InsertVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                uint16_t keySize, uint16_t valSize)
      : Version(VersionType::kInsert, workerId, txId, commandId),
        mKeySize(keySize),
        mValSize(valSize) {
  }

  InsertVersion(WORKERID workerId, TXID txId, COMMANDID commandId, Slice key,
                Slice val)
      : Version(VersionType::kInsert, workerId, txId, commandId),
        mKeySize(key.size()),
        mValSize(val.size()) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }

  Slice InsertedKey() const {
    return Slice(mPayload, mKeySize);
  }

  Slice InsertedVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }

public:
  inline static const InsertVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const InsertVersion*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// RemoveVersion
// -----------------------------------------------------------------------------

struct __attribute__((packed)) RemoveVersion : Version {
public:
  uint16_t mKeySize;

  uint16_t mValSize;

  DanglingPointer mDanglingPointer;

  // Key + Value
  uint8_t mPayload[];

public:
  RemoveVersion(WORKERID workerId, TXID txId, COMMANDID commandId,
                uint16_t keySize, uint16_t valSize)
      : Version(VersionType::kRemove, workerId, txId, commandId),
        mKeySize(keySize),
        mValSize(valSize) {
  }

  RemoveVersion(WORKERID workerId, TXID txId, COMMANDID commandId, Slice key,
                Slice val, const DanglingPointer& danglingPointer)
      : Version(VersionType::kRemove, workerId, txId, commandId),
        mKeySize(key.size()),
        mValSize(val.size()),
        mDanglingPointer(danglingPointer) {
    std::memcpy(mPayload, key.data(), key.size());
    std::memcpy(mPayload + key.size(), val.data(), val.size());
  }

  Slice RemovedKey() const {
    return Slice(mPayload, mKeySize);
  }

  Slice RemovedVal() const {
    return Slice(mPayload + mKeySize, mValSize);
  }

public:
  inline static const RemoveVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const RemoveVersion*>(buffer);
  }
};

} // namespace leanstore::storage::btree