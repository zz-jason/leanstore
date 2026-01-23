#pragma once

#include "leanstore/base/log.hpp"
#include "leanstore/base/slice.hpp"
#include "leanstore/c/wal_record.h"
#include "leanstore/wal/wal_traits.hpp"

namespace leanstore {

#ifdef DEBUG
#define LEAN_DCHECK_TYPE_MATCH(T, actual_wal_type)                                                 \
  if (actual_wal_type != WalRecordTraits<T>::kType) {                                              \
    Log::Fatal("WAL Record Type Mismatch: expected {} ({}), got {}", WalRecordTraits<T>::kName,    \
               static_cast<int>(WalRecordTraits<T>::kType), static_cast<int>(actual_wal_type));    \
  }
#define LEAN_DCHECK_IS_MVCC_BTREE_WAL_RECORD_TYPE(actual_wal_type)                                 \
  if (!IsMvccBTreeWalRecordType(actual_wal_type)) {                                                \
    Log::Fatal("WAL Record Type Mismatch: expected MVCC BTree WAL Record, got {}",                 \
               static_cast<int>(actual_wal_type));                                                 \
  }
#else
#define LEAN_DCHECK_TYPE_MATCH(T, actual_wal_type)
#define LEAN_DCHECK_IS_MVCC_BTREE_WAL_RECORD_TYPE(actual_wal_type)
#endif

/// Cast a generic lean_wal_record to a specific WAL record type T.
template <typename T>
T& CastTo(lean_wal_record& record) {
  LEAN_DCHECK_TYPE_MATCH(T, record.type_);
  return *reinterpret_cast<T*>(&record);
}

/// Specialization for lean_wal_tx_base
template <>
inline lean_wal_tx_base& CastTo(lean_wal_record& record) {
  LEAN_DCHECK_IS_MVCC_BTREE_WAL_RECORD_TYPE(record.type_);
  return *reinterpret_cast<lean_wal_tx_base*>(&record);
}

/// Const version
template <typename T>
const T& CastTo(const lean_wal_record& record) {
  LEAN_DCHECK_TYPE_MATCH(T, record.type_);
  return *reinterpret_cast<const T*>(&record);
}

/// Specialization for lean_wal_tx_base, const version
template <>
inline const lean_wal_tx_base& CastTo(const lean_wal_record& record) {
  LEAN_DCHECK_IS_MVCC_BTREE_WAL_RECORD_TYPE(record.type_);
  return *reinterpret_cast<const lean_wal_tx_base*>(&record);
}

#undef LEAN_DCHECK_TYPE_MATCH
#undef LEAN_DCHECK_IS_MVCC_BTREE_WAL_RECORD_TYPE

} // namespace leanstore