#pragma once

#include "leanstore/btree/core/btree_iter_mut.hpp"
#include "leanstore/common/portable.h"
#include "leanstore/units.hpp"
#include "leanstore/utils/log.hpp"

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
class PACKED Tuple {
public:
  /// Format of the current tuple.
  TupleFormat format_;

  /// ID of the worker who created this tuple.
  lean_wid_t worker_id_;

  /// ID of the transaction who created this tuple, it's the start timestamp of
  /// the transaction.
  lean_txid_t tx_id_;

  /// ID of the command who created this tuple.
  lean_cmdid_t command_id_;

  /// Whether the tuple is locked for write.
  bool write_locked_;

public:
  Tuple(TupleFormat format, lean_wid_t worker_id, lean_txid_t tx_id,
        lean_cmdid_t command_id = kInvalidCommandid, bool write_locked = false)
      : format_(format),
        worker_id_(worker_id),
        tx_id_(tx_id),
        command_id_(command_id),
        write_locked_(write_locked) {
  }

public:
  inline bool IsWriteLocked() const {
    return write_locked_;
  }

  inline void WriteLock() {
    write_locked_ = true;
  }

  inline void WriteUnlock() {
    write_locked_ = false;
  }

public:
  inline static Tuple* From(uint8_t* buffer) {
    return reinterpret_cast<Tuple*>(buffer);
  }

  inline static const Tuple* From(const uint8_t* buffer) {
    return reinterpret_cast<const Tuple*>(buffer);
  }

  static bool ToFat(BTreeIterMut* iterator);
};

// -----------------------------------------------------------------------------
// FatTupleDelta
// -----------------------------------------------------------------------------

/// FatTupleDelta is the delta changes made to the previous version, a delta
/// is consisted of the following parts:
/// 1. The creator info: worker_id_, tx_id_, command_id_
/// 2. The update descriptor: payload
///
/// Data loyout of a FatTupleDelta:
/// | worker_id_ | tx_id_ | command_id_ | UpdateDesc | Delta |
///
/// FatTuple uses precise garbage collection
struct PACKED FatTupleDelta {
public:
  /// ID of the worker who creates this delta.
  lean_wid_t worker_id_;

  /// ID of the transaction who creates this delta.
  lean_txid_t tx_id_;

  /// ID of the command who creates this delta.
  /// NOTE: Take care, otherwise we would overwrite another undo version
  lean_cmdid_t command_id_ = kInvalidCommandid;

  /// Descriptor + Delta
  uint8_t payload_[];

public:
  FatTupleDelta() = default;

  FatTupleDelta(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id,
                const uint8_t* buf, uint32_t size)
      : worker_id_(worker_id),
        tx_id_(tx_id),
        command_id_(command_id) {
    std::memcpy(payload_, buf, size);
  }

public:
  inline UpdateDesc& GetUpdateDesc() {
    return *reinterpret_cast<UpdateDesc*>(payload_);
  }

  inline const UpdateDesc& GetUpdateDesc() const {
    return *reinterpret_cast<const UpdateDesc*>(payload_);
  }

  inline uint8_t* GetDeltaPtr() {
    return payload_ + GetUpdateDesc().Size();
  }

  inline const uint8_t* GetDeltaPtr() const {
    return payload_ + GetUpdateDesc().Size();
  }

  inline uint32_t TotalSize() {
    const auto& update_desc = GetUpdateDesc();
    return sizeof(FatTupleDelta) + update_desc.SizeWithDelta();
  }
};

// -----------------------------------------------------------------------------
// FatTuple
// -----------------------------------------------------------------------------

/// FatTuple stores all the versions in the value. Layout of FatTuple:
///
/// | FatTuple meta |  newest value | FatTupleDelta O2N |
///
class PACKED FatTuple : public Tuple {
public:
  /// Size of the newest value.
  uint16_t val_size_ = 0;

  /// Capacity of the payload
  uint32_t payload_capacity_ = 0;

  /// Space used of the payload
  uint32_t payload_size_ = 0;

  uint32_t data_offset_ = 0;

  uint16_t num_deltas_ = 0; // Attention: coupled with payload_size_

  // value, FatTupleDelta+Descriptor+Delta[] O2N
  uint8_t payload_[];

public:
  FatTuple(uint32_t payload_capacity)
      : Tuple(TupleFormat::kFat, 0, 0),
        payload_capacity_(payload_capacity),
        data_offset_(payload_capacity) {
  }

  FatTuple(uint32_t payload_capacity, uint32_t val_size, const ChainedTuple& chained_tuple);

  bool HasSpaceFor(const UpdateDesc&);

  void Append(UpdateDesc&);

  template <typename... Args>
  FatTupleDelta& NewDelta(uint32_t total_delta_size, Args&&... args);

  void GarbageCollection();

  void UndoLastUpdate();

  inline MutableSlice GetMutableValue() {
    return MutableSlice(GetValPtr(), val_size_);
  }

  inline Slice GetValue() const {
    return Slice(GetValPtr(), val_size_);
  }

  inline uint8_t* GetValPtr() {
    return payload_;
  }

  inline const uint8_t* GetValPtr() const {
    return payload_;
  }

  /// Get the newest visible version of the current tuple, and apply the
  /// callback if the newest visible tuple is found.
  ///
  /// @return whether the tuple is found, and the number of visited versions
  std::tuple<OpCode, uint16_t> GetVisibleTuple(ValCallback val_callback) const;

  void ConvertToChained(lean_treeid_t tree_id);

  /// Used when converting to chained tuple. Deltas in chained tuple are
  /// stored N2O (from newest to oldest), but are stored O2N (from oldest to
  /// newest) in fat tuple. Reversing the deltas is a naive and simple
  /// solution.
  ///
  /// TODO(jian.z): verify whether it's expected to reverse the bytes instead
  /// of Deltas
  inline void ReverseDeltas() {
    std::reverse(get_delta_offsets(), get_delta_offsets() + num_deltas_);
  }

private:
  void resize(uint32_t new_size);

  inline FatTupleDelta& get_delta(uint16_t i) {
    LEAN_DCHECK(i < num_deltas_);
    return *reinterpret_cast<FatTupleDelta*>(payload_ + get_delta_offsets()[i]);
  }

  inline const FatTupleDelta& get_delta(uint16_t i) const {
    LEAN_DCHECK(i < num_deltas_);
    return *reinterpret_cast<const FatTupleDelta*>(payload_ + get_delta_offsets()[i]);
  }

  inline uint16_t* get_delta_offsets() {
    return reinterpret_cast<uint16_t*>(payload_ + val_size_);
  }

  inline const uint16_t* get_delta_offsets() const {
    return reinterpret_cast<const uint16_t*>(payload_ + val_size_);
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

struct PACKED DanglingPointer {
  BufferFrame* bf_ = nullptr;

  uint64_t latch_version_should_be_ = -1;

  int32_t head_slot_ = -1;

public:
  DanglingPointer() = default;

  DanglingPointer(const BTreeIterMut* x_iter)
      : bf_(x_iter->guarded_leaf_.bf_),
        latch_version_should_be_(x_iter->guarded_leaf_.guard_.version_),
        head_slot_(x_iter->slot_id_) {
  }
};

// -----------------------------------------------------------------------------
// Version
// -----------------------------------------------------------------------------

enum class VersionType : uint8_t { kUpdate, kInsert, kRemove };

struct PACKED Version {
public:
  VersionType type_;

  lean_wid_t worker_id_;

  lean_txid_t tx_id_;

  lean_cmdid_t command_id_;

  Version(VersionType type, lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id)
      : type_(type),
        worker_id_(worker_id),
        tx_id_(tx_id),
        command_id_(command_id) {
  }
};

// -----------------------------------------------------------------------------
// UpdateVersion
// -----------------------------------------------------------------------------

struct PACKED UpdateVersion : Version {
  uint8_t is_delta_ = 1;

  uint8_t payload_[]; // UpdateDescriptor + Delta

public:
  UpdateVersion(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, bool is_delta)
      : Version(VersionType::kUpdate, worker_id, tx_id, command_id),
        is_delta_(is_delta) {
  }

  UpdateVersion(const FatTupleDelta& delta, uint64_t delta_payload_size)
      : Version(VersionType::kUpdate, delta.worker_id_, delta.tx_id_, delta.command_id_),
        is_delta_(true) {
    std::memcpy(payload_, delta.payload_, delta_payload_size);
  }

public:
  inline static const UpdateVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const UpdateVersion*>(buffer);
  }
};

struct PACKED InsertVersion : Version {
public:
  uint16_t key_size_;

  uint16_t val_size_;

  // Key + Value
  uint8_t payload_[];

public:
  InsertVersion(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, uint16_t key_size,
                uint16_t val_size)
      : Version(VersionType::kInsert, worker_id, tx_id, command_id),
        key_size_(key_size),
        val_size_(val_size) {
  }

  InsertVersion(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, Slice key,
                Slice val)
      : Version(VersionType::kInsert, worker_id, tx_id, command_id),
        key_size_(key.size()),
        val_size_(val.size()) {
    std::memcpy(payload_, key.data(), key.size());
    std::memcpy(payload_ + key.size(), val.data(), val.size());
  }

  Slice InsertedKey() const {
    return Slice(payload_, key_size_);
  }

  Slice InsertedVal() const {
    return Slice(payload_ + key_size_, val_size_);
  }

public:
  inline static const InsertVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const InsertVersion*>(buffer);
  }
};

// -----------------------------------------------------------------------------
// RemoveVersion
// -----------------------------------------------------------------------------

struct PACKED RemoveVersion : Version {
public:
  uint16_t key_size_;

  uint16_t val_size_;

  DanglingPointer dangling_pointer_;

  // Key + Value
  uint8_t payload_[];

public:
  RemoveVersion(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, uint16_t key_size,
                uint16_t val_size)
      : Version(VersionType::kRemove, worker_id, tx_id, command_id),
        key_size_(key_size),
        val_size_(val_size) {
  }

  RemoveVersion(lean_wid_t worker_id, lean_txid_t tx_id, lean_cmdid_t command_id, Slice key,
                Slice val, const DanglingPointer& dangling_pointer)
      : Version(VersionType::kRemove, worker_id, tx_id, command_id),
        key_size_(key.size()),
        val_size_(val.size()),
        dangling_pointer_(dangling_pointer) {
    std::memcpy(payload_, key.data(), key.size());
    std::memcpy(payload_ + key.size(), val.data(), val.size());
  }

  Slice RemovedKey() const {
    return Slice(payload_, key_size_);
  }

  Slice RemovedVal() const {
    return Slice(payload_ + key_size_, val_size_);
  }

public:
  inline static const RemoveVersion* From(const uint8_t* buffer) {
    return reinterpret_cast<const RemoveVersion*>(buffer);
  }
};

} // namespace leanstore::storage::btree