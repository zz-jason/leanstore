#include "c/cursor_mvcc_impl.hpp"

#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/c/leanstore.h"
#include "leanstore/coro/coro_env.hpp"
#include "leanstore/coro/coro_scheduler.hpp"
#include "leanstore/tx/transaction_kv.hpp"

#include <cassert>

namespace leanstore {

bool CursorMvccImpl::SeekToFirst() {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    iter->SeekToFirst();
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::SeekToFirstGe(lean_str_view key) {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    iter->SeekToFirstGreaterEqual({key.data, key.size});
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::Next() {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    if (is_removed_) {
      iter->SeekToFirstGreaterEqual({current_key_.data(), current_key_.size()});
    } else {
      iter->SeekToEqual({current_key_.data(), current_key_.size()});
      iter->Next();
    }
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::SeekToLast() {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    iter->SeekToLast();
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::SeekToLastLe(lean_str_view key) {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    iter->SeekToLastLessEqual({key.data, key.size});
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::Prev() {
  is_valid_ = false;
  session_impl_->ExecSync([&]() {
    assert(CoroEnv::CurTxMgr().IsTxStarted() && "lean_cursor operation should be in a transaction");
    auto iter = btree_->NewBTreeIter();
    if (is_removed_) {
      iter->SeekToLastLessEqual({current_key_.data(), current_key_.size()});
    } else {
      iter->SeekToEqual({current_key_.data(), current_key_.size()});
      iter->Prev();
    }
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool CursorMvccImpl::IsValid() {
  return is_valid_;
}

void CursorMvccImpl::CurrentKey(lean_str* key) {
  if (is_valid_) {
    lean_str_assign(key, current_key_.data(), current_key_.size());
  }
}

void CursorMvccImpl::CurrentValue(lean_str* value) {
  if (is_valid_) {
    lean_str_assign(value, current_value_.data(), current_value_.size());
  }
}

lean_status CursorMvccImpl::RemoveCurrent() {
  lean_status status = lean_status::LEAN_STATUS_OK;
  if (is_valid_) {
    session_impl_->ExecSync([&]() {
      assert(CoroEnv::CurTxMgr().IsTxStarted() &&
             "lean_cursor operation should be in a transaction");
      status = (lean_status)btree_->Remove({current_key_.data(), current_key_.size()});
      if (status == lean_status::LEAN_STATUS_OK) {
        is_removed_ = true;
      }
    });
  }
  return status;
}

lean_status CursorMvccImpl::UpdateCurrent(lean_str_view new_value [[maybe_unused]]) {
  return lean_status::LEAN_ERR_UNSUPPORTED;
}

bool CursorMvccImpl::SeekToVisibleAsc(BTreeIter* iter) {
  bool found = false;
  for (; !found && iter->Valid(); iter->Next()) {
    btree_->GetVisibleTuple(iter->Val(), [&](Slice visible_val) {
      found = true;

      iter->AssembleKey();
      Slice key = iter->Key();
      current_key_.assign((const char*)key.data(), key.size());
      current_value_.assign((const char*)visible_val.data(), visible_val.size());
    });
  }
  return found;
}

bool CursorMvccImpl::SeekToVisibleDesc(BTreeIter* iter) {
  bool found = false;
  for (; !found && iter->Valid(); iter->Prev()) {
    btree_->GetVisibleTuple(iter->Val(), [&](Slice visible_val) {
      found = true;

      iter->AssembleKey();
      Slice key = iter->Key();
      current_key_.assign((const char*)key.data(), key.size());
      current_value_.assign((const char*)visible_val.data(), visible_val.size());
    });
  }
  return found;
}

} // namespace leanstore