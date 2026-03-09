#pragma once

#include "leanstore/btree/column_store/column_store.hpp"
#include "leanstore/kv_interface.hpp"

#include <functional>
#include <vector>

namespace leanstore {

class BTreeIter;
class BTreeNode;
class LeanStore;
template <typename T>
class GuardedBufferFrame;

} // namespace leanstore

namespace leanstore::column_store {

struct ColumnBlockScanState {
  bool need_start_row_ = false;
  bool key_decoded_ = false;
  std::vector<Datum> key_datums_;
};

struct ColumnBlockScanResult {
  bool emitted_ = false;
  bool stop_ = false;
};

using ColumnBlockScanCallback = std::function<bool(Slice key, Slice val)>;

Result<bool> LookupColumnBlock(LeanStore* store, const ColumnBlockRef& ref, Slice key,
                               std::string* out_value);

Result<ColumnBlockScanResult> ScanColumnBlockAsc(LeanStore* store, const ColumnBlockRef& ref,
                                                 Slice start_key, ColumnBlockScanState* state,
                                                 const ColumnBlockScanCallback& callback);

Result<ColumnBlockScanResult> ScanColumnBlockDesc(LeanStore* store, const ColumnBlockRef& ref,
                                                  Slice start_key, ColumnBlockScanState* state,
                                                  const ColumnBlockScanCallback& callback);

OpCode LookupColumnLeaf(LeanStore* store, GuardedBufferFrame<BTreeNode>& guarded_leaf, Slice key,
                        ValCallback val_callback);

OpCode ScanColumnLeafAsc(LeanStore* store, BTreeIter* iter, Slice start_key, ScanCallback callback);

OpCode ScanColumnLeafDesc(LeanStore* store, BTreeIter* iter, Slice start_key,
                          ScanCallback callback);

} // namespace leanstore::column_store
