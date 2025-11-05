#pragma once

#include "leanstore/common/wal_record.h"

namespace leanstore {

inline bool IsSmoBTreeWalRecordType(lean_wal_type_t type) {
  switch (type) {
  case LEAN_WAL_TYPE_SMO_COMPLETE:
  case LEAN_WAL_TYPE_SMO_PAGENEW:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT:
  case LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT:
    return true;
  default:
    return false;
  }
}

inline bool IsAtomicBTreeWalRecordType(lean_wal_type_t type) {
  switch (type) {
  case LEAN_WAL_TYPE_INSERT:
  case LEAN_WAL_TYPE_UPDATE:
  case LEAN_WAL_TYPE_REMOVE:
    return true;
  default:
    return false;
  }
}

inline bool IsMvccBTreeWalRecordType(lean_wal_type_t type) {
  switch (type) {
  case LEAN_WAL_TYPE_TX_ABORT:
  case LEAN_WAL_TYPE_TX_COMPLETE:
  case LEAN_WAL_TYPE_TX_INSERT:
  case LEAN_WAL_TYPE_TX_REMOVE:
  case LEAN_WAL_TYPE_TX_UPDATE:
    return true;
  default:
    return false;
  }
}

template <typename T>
struct WalRecordTraits;

template <>
struct WalRecordTraits<lean_wal_carriage_return> {
  static constexpr auto kType = LEAN_WAL_TYPE_CARRIAGE_RETURN;
  static constexpr auto kName = "CarriageReturn";
};

template <>
struct WalRecordTraits<lean_wal_smo_complete> {
  static constexpr auto kType = LEAN_WAL_TYPE_SMO_COMPLETE;
  static constexpr auto kName = "SmoComplete";
};

template <>
struct WalRecordTraits<lean_wal_smo_pagenew> {
  static constexpr auto kType = LEAN_WAL_TYPE_SMO_PAGENEW;
  static constexpr auto kName = "SmoPageNew";
};

template <>
struct WalRecordTraits<lean_wal_smo_pagesplit_root> {
  static constexpr auto kType = LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT;
  static constexpr auto kName = "SmoPageSplitRoot";
};

template <>
struct WalRecordTraits<lean_wal_smo_pagesplit_nonroot> {
  static constexpr auto kType = LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT;
  static constexpr auto kName = "SmoPageSplitNonRoot";
};

template <>
struct WalRecordTraits<lean_wal_insert> {
  static constexpr auto kType = LEAN_WAL_TYPE_INSERT;
  static constexpr auto kName = "Insert";
};

template <>
struct WalRecordTraits<lean_wal_update> {
  static constexpr auto kType = LEAN_WAL_TYPE_UPDATE;
  static constexpr auto kName = "Update";
};

template <>
struct WalRecordTraits<lean_wal_remove> {
  static constexpr auto kType = LEAN_WAL_TYPE_REMOVE;
  static constexpr auto kName = "Remove";
};

template <>
struct WalRecordTraits<lean_wal_tx_abort> {
  static constexpr auto kType = LEAN_WAL_TYPE_TX_ABORT;
  static constexpr auto kName = "TxAbort";
};

template <>
struct WalRecordTraits<lean_wal_tx_complete> {
  static constexpr auto kType = LEAN_WAL_TYPE_TX_COMPLETE;
  static constexpr auto kName = "TxComplete";
};

template <>
struct WalRecordTraits<lean_wal_tx_insert> {
  static constexpr auto kType = LEAN_WAL_TYPE_TX_INSERT;
  static constexpr auto kName = "TxInsert";
};

template <>
struct WalRecordTraits<lean_wal_tx_remove> {
  static constexpr auto kType = LEAN_WAL_TYPE_TX_REMOVE;
  static constexpr auto kName = "TxRemove";
};

template <>
struct WalRecordTraits<lean_wal_tx_update> {
  static constexpr auto kType = LEAN_WAL_TYPE_TX_UPDATE;
  static constexpr auto kName = "TxUpdate";
};

} // namespace leanstore