#include "c/cursor_impl.hpp"

#include "leanstore/base/slice.hpp"
#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/coro/coro_scheduler.hpp"

#include <cassert>

namespace leanstore {

bool CursorImpl::SeekToFirst() {
  is_valid_ = false;
  session_impl_->Session().ExecSync([&]() {
    auto iter = btree_->NewBTreeIter();
    iter->SeekToFirst();
    is_valid_ = iter->Valid();
    RecordCurrent(iter.get());
  });
  return is_valid_;
}

bool CursorImpl::SeekToFirstGe(lean_str_view key) {
  is_valid_ = false;
  session_impl_->Session().ExecSync([&]() {
    auto iter = btree_->NewBTreeIter();
    iter->SeekToFirstGreaterEqual({key.data, key.size});
    is_valid_ = iter->Valid();
    RecordCurrent(iter.get());
  });
  return is_valid_;
}

bool CursorImpl::Next() {
  if (is_valid_) {
    session_impl_->Session().ExecSync([&]() {
      auto iter = btree_->NewBTreeIter();
      if (is_removed_) {
        iter->SeekToFirstGreaterEqual({current_key_.data(), current_key_.size()});
      } else {
        iter->SeekToEqual({current_key_.data(), current_key_.size()});
        iter->Next();
      }
      is_valid_ = iter->Valid();
      RecordCurrent(iter.get());
    });
  }
  return is_valid_;
}

bool CursorImpl::SeekToLast() {
  is_valid_ = false;
  session_impl_->Session().ExecSync([&]() {
    auto iter = btree_->NewBTreeIter();
    iter->SeekToLast();
    is_valid_ = iter->Valid();
    RecordCurrent(iter.get());
  });
  return is_valid_;
}

bool CursorImpl::SeekToLastLe(lean_str_view key) {
  is_valid_ = false;
  session_impl_->Session().ExecSync([&]() {
    auto iter = btree_->NewBTreeIter();
    iter->SeekToLastLessEqual({key.data, key.size});
    is_valid_ = iter->Valid();
    RecordCurrent(iter.get());
  });
  return is_valid_;
}

bool CursorImpl::Prev() {
  if (is_valid_) {
    session_impl_->Session().ExecSync([&]() {
      auto iter = btree_->NewBTreeIter();
      if (is_removed_) {
        iter->SeekToLastLessEqual({current_key_.data(), current_key_.size()});
      } else {
        iter->SeekToEqual({current_key_.data(), current_key_.size()});
        iter->Prev();
      }
      is_valid_ = iter->Valid();
      RecordCurrent(iter.get());
    });
  }
  return is_valid_;
}

bool CursorImpl::IsValid() {
  return is_valid_;
}

void CursorImpl::CurrentKey(lean_str* key) {
  if (is_valid_) {
    lean_str_assign(key, current_key_.data(), current_key_.size());
  }
}

void CursorImpl::CurrentValue(lean_str* value) {
  if (is_valid_) {
    lean_str_assign(value, current_value_.data(), current_value_.size());
  }
}

lean_status CursorImpl::RemoveCurrent() {
  lean_status status = lean_status::LEAN_STATUS_OK;
  if (is_valid_) {
    session_impl_->Session().ExecSync([&]() {
      status = (lean_status)btree_->Remove({current_key_.data(), current_key_.size()});
      if (status == lean_status::LEAN_STATUS_OK) {
        is_removed_ = true;
      }
    });
  }
  return status;
}

lean_status CursorImpl::UpdateCurrent(lean_str_view new_value [[maybe_unused]]) {
  return lean_status::LEAN_ERR_UNSUPPORTED;
}

void CursorImpl::RecordCurrent(BTreeIter* iter) {
  if (iter->Valid()) {
    iter->AssembleKey();
    Slice key = iter->Key();
    Slice visible_val = iter->Val();
    current_key_.assign((const char*)key.data(), key.size());
    current_value_.assign((const char*)visible_val.data(), visible_val.size());
  }
}

} // namespace leanstore
