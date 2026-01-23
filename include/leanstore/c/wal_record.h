#ifndef LEANSTORE_COMMON_WAL_FORMAT_H
#define LEANSTORE_COMMON_WAL_FORMAT_H

#include "leanstore/c/portable.h"
#include "leanstore/c/types.h"

#include <assert.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN: To keep the C-style naming conventions for structs

//------------------------------------------------------------------------------
// Integer type aliases used in WAL records
//------------------------------------------------------------------------------

typedef uint32_t lean_wal_size_t; // Size in WAL records

//------------------------------------------------------------------------------
// All forward declarations
//------------------------------------------------------------------------------

/// Base WAL record, all the other WAL records are derived from this base class.
struct lean_wal_record;

/// For carriage return in the WAL buffer
struct lean_wal_carriage_return;

// For SMO (structural modification operation)
struct lean_wal_smo_complete;          // completes a SMO operation
struct lean_wal_smo_pagenew;           // creates a new btree node
struct lean_wal_smo_pagesplit_root;    // splits a btree root node
struct lean_wal_smo_pagesplit_nonroot; // splits a btree non-root node

// For atomic btree operations
struct lean_wal_insert;
struct lean_wal_update;
struct lean_wal_remove;

// For mvcc btree operations
struct lean_wal_tx_base;     // base for all transaction-related WAL records
struct lean_wal_tx_abort;    // aborts a transaction
struct lean_wal_tx_complete; // completes a transaction
struct lean_wal_tx_insert;   // inserts a key
struct lean_wal_tx_remove;   // removes a key
struct lean_wal_tx_update;   // updates the value of a key

/// WAL record type
typedef uint8_t lean_wal_type_t;
enum {
  LEAN_WAL_TYPE_CARRIAGE_RETURN = 1,

  LEAN_WAL_TYPE_SMO_COMPLETE,
  LEAN_WAL_TYPE_SMO_PAGENEW,
  LEAN_WAL_TYPE_SMO_PAGESPLIT_ROOT,
  LEAN_WAL_TYPE_SMO_PAGESPLIT_NONROOT,

  LEAN_WAL_TYPE_INSERT,
  LEAN_WAL_TYPE_UPDATE,
  LEAN_WAL_TYPE_REMOVE,

  LEAN_WAL_TYPE_TX_ABORT,
  LEAN_WAL_TYPE_TX_COMPLETE,
  LEAN_WAL_TYPE_TX_INSERT,
  LEAN_WAL_TYPE_TX_REMOVE,
  LEAN_WAL_TYPE_TX_UPDATE,
};

typedef struct PACKED lean_wal_record {
  /// Type of the WAL record
  lean_wal_type_t type_;

  /// ID of this WAL record. It is globally and monotonically increased in a WAL
  /// file, used to identify the order of WAL records.
  lean_lid_t lsn_;

  /// Size of the whole WAL record, including all the payloads.
  lean_wal_size_t size_;

  /// BTree id of this WAL record
  lean_treeid_t btree_id_;

  /// CRC32 of the whole WAL record, including all the payloads. When
  /// calculating, this field is set to 0.
  uint32_t crc32_;

} lean_wal_record;

//------------------------------------------------------------------------------
// Carriage Return
//------------------------------------------------------------------------------

typedef struct PACKED lean_wal_carriage_return {
  lean_wal_record base_;

} lean_wal_carriage_return;

//------------------------------------------------------------------------------
// Structural Modification Operations (SMO)
//------------------------------------------------------------------------------

typedef struct PACKED lean_wal_smo_complete {
  lean_wal_record base_;

  lean_txid_t sys_txid_;

} lean_wal_smo_complete;

typedef struct PACKED lean_wal_smo_pagenew {
  lean_wal_record base_;

  lean_txid_t sys_txid_;

  lean_pid_t page_id_;
  lean_lid_t page_version_; // version after applying this WAL record

  bool is_leaf_;

} lean_wal_smo_pagenew;

typedef struct PACKED lean_wal_smo_pagesplit_root {
  lean_wal_record base_;

  lean_txid_t sys_txid_;

  lean_pid_t page_id_;
  lean_lid_t page_version_; // version after applying this WAL record

  lean_pid_t parent_; // meta node
  lean_pid_t new_lhs_;
  lean_pid_t new_rhs_;

  uint16_t sep_slot_;
  uint16_t sep_size_;
  bool sep_truncated_;

} lean_wal_smo_pagesplit_root;

typedef struct PACKED lean_wal_smo_pagesplit_nonroot {
  lean_wal_record base_;

  lean_txid_t sys_txid_;

  lean_pid_t page_id_;
  lean_lid_t page_version_; // version after applying this WAL record

  lean_pid_t parent_;
  lean_pid_t new_lhs_;

  uint16_t sep_slot_;
  uint16_t sep_size_;
  bool sep_truncated_;

} lean_wal_smo_pagesplit_nonroot;

//------------------------------------------------------------------------------
// Atomic B-tree Operations
//------------------------------------------------------------------------------

typedef struct PACKED lean_wal_insert {
  lean_wal_record base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Size of the key inserted.
  lean_wal_size_t key_size_;

  /// Size of the value inserted.
  lean_wal_size_t val_size_;

  /// key_data_[key_size_] + val_data_[val_size_]
  uint8_t payload_[];

} lean_wal_insert;

typedef struct PACKED lean_wal_update {
  lean_wal_record base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Size of the key updated.
  lean_wal_size_t key_size_;

  /// Size of the updated field info.
  lean_wal_size_t update_desc_size_;

  /// Size of the update delta.
  lean_wal_size_t delta_size_;

  /// key_data_[key_size_] + update_desc_[update_desc_size_] + delta_[delta_size_]
  uint8_t payload_[];

} lean_wal_update;

inline uint8_t* lean_wal_update_get_key(lean_wal_update* wal) {
  return wal->payload_;
}

inline uint8_t* lean_wal_update_get_update_desc(lean_wal_update* wal) {
  return wal->payload_ + wal->key_size_;
}

inline uint8_t* lean_wal_update_get_delta(lean_wal_update* wal) {
  return wal->payload_ + wal->key_size_ + wal->update_desc_size_;
}

typedef struct PACKED lean_wal_remove {
  lean_wal_record base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Size of the key updated.
  lean_wal_size_t key_size_;

  /// Size of the value removed.
  lean_wal_size_t val_size_;

  /// key_data_[key_size_] + val_data_[val_size_]
  uint8_t payload_[];

} lean_wal_remove;

//------------------------------------------------------------------------------
// MVCC B-tree Operations
//------------------------------------------------------------------------------

typedef struct PACKED lean_wal_tx_base {
  lean_wal_record base_;

  /// ID of the TxManager who executes the transaction and generates this WAL
  /// record.
  lean_wid_t wid_;

  /// ID of the transaction this WAL record belongs to.
  lean_txid_t txid_;

  /// Previous WAL record ID of the same transaction. 0 if it's the first WAL
  /// record of the transaction. Used as a linked list to chain all the WAL
  /// records of the same transaction.
  ///
  /// When transaction aborts, this field is used to quickly locate the previous
  /// WAL record of the same transaction, so that we can undo the changes made
  /// by the transaction, and write compensation log records (CLRs) if needed.
  lean_lid_t prev_lsn_;

} lean_wal_tx_base;

typedef struct PACKED lean_wal_tx_abort {
  lean_wal_tx_base tx_base_;

} lean_wal_tx_abort;

typedef struct PACKED lean_wal_tx_complete {
  lean_wal_tx_base tx_base_;

} lean_wal_tx_complete;

typedef struct PACKED lean_wal_tx_insert {
  lean_wal_tx_base tx_base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Previous worker id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_wid_t prev_wid_;

  /// Previous transaction id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_txid_t prev_txid_;

  /// Previous command id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_cmdid_t prev_cmd_id_;

  /// Size of the key inserted.
  lean_wal_size_t key_size_;

  /// Size of the value inserted.
  lean_wal_size_t val_size_;

  /// key_data_[key_size_] + val_data_[val_size_]
  uint8_t payload_[];

} lean_wal_tx_insert;

inline char* lean_wal_tx_insert_get_key(const lean_wal_tx_insert* wal) {
  return (char*)wal->payload_;
}

typedef struct PACKED lean_wal_tx_remove {
  lean_wal_tx_base tx_base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Previous worker id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_wid_t prev_wid_;

  /// Previous transaction id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_txid_t prev_txid_;

  /// Previous command id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_cmdid_t prev_cmd_id_;

  /// Size of the key removed.
  lean_wal_size_t key_size_;

  /// Size of the value removed.
  lean_wal_size_t val_size_;

  /// key_data_[key_size_] + val_data_[val_size_]
  uint8_t payload_[];

} lean_wal_tx_remove;

inline char* lean_wal_tx_remove_get_key(const lean_wal_tx_remove* wal) {
  return (char*)wal->payload_;
}

inline char* lean_wal_tx_remove_get_val(const lean_wal_tx_remove* wal) {
  return (char*)wal->payload_ + wal->key_size_;
}

typedef struct PACKED lean_wal_tx_update {
  lean_wal_tx_base tx_base_;

  /// ID of the page this WAL record operates on.
  /// Used to identify the btree node.
  lean_pid_t page_id_;

  /// Version of the page after applying this WAL record.
  ///
  /// Page version is updated to the LSN of this WAL record after applying this
  /// WAL record.
  lean_lid_t page_version_;

  /// Previous worker id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_wid_t prev_wid_;

  /// Previous transaction id of the same key.
  /// Used during undo to restore the previous version of the key.
  lean_txid_t prev_txid_;

  /// Command id of old_cmd_id XOR new_cmd_id. Used in both redo and undo to
  /// restore the command id of the key:
  /// - During redo, new_cmd_id = old_cmd_id XOR xor_cmd_id_
  /// - During undo, old_cmd_id = new_cmd_id XOR xor_cmd_id_
  lean_cmdid_t xor_cmd_id_;

  /// Size of the key updated.
  lean_wal_size_t key_size_;

  /// Size of the updated field info.
  lean_wal_size_t update_desc_size_;

  /// Size of the update delta.
  lean_wal_size_t delta_size_;

  /// key_data_[key_size_] + update_desc_[update_desc_size_] + delta_[delta_size_]
  uint8_t payload_[];

} lean_wal_tx_update;

inline char* lean_wal_tx_update_get_key(const lean_wal_tx_update* wal) {
  return (char*)wal->payload_;
}

inline char* lean_wal_tx_update_get_update_desc(const lean_wal_tx_update* wal) {
  return (char*)wal->payload_ + wal->key_size_;
}

inline char* lean_wal_tx_update_get_delta(const lean_wal_tx_update* wal) {
  return (char*)wal->payload_ + wal->key_size_ + wal->update_desc_size_;
}

/// NOLINTEND

#ifdef __cplusplus
} // extern "C"
#endif

#endif