#include "leanstore/lean_cursor.hpp"

#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/lean_btree.hpp"
#include "leanstore/lean_session.hpp"
#include "leanstore/tx/transaction_kv.hpp"

#include <cassert>
#include <type_traits>
#include <variant>

namespace leanstore {

LeanCursor::LeanCursor(LeanBTree* btree) : btree_(btree) {
}

LeanCursor::~LeanCursor() {
  Close();
}

bool LeanCursor::SeekToFirst() {
  is_valid_ = false;
  is_removed_ = false;
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    iter->SeekToFirst();
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::SeekToFirstGe(Slice key) {
  is_valid_ = false;
  is_removed_ = false;
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    iter->SeekToFirstGreaterEqual(key);
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::SeekToLast() {
  is_valid_ = false;
  is_removed_ = false;
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    iter->SeekToLast();
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::SeekToLastLe(Slice key) {
  is_valid_ = false;
  is_removed_ = false;
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    iter->SeekToLastLessEqual(key);
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::Next() {
  if (!is_valid_) {
    return false;
  }
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    if (is_removed_) {
      iter->SeekToFirstGreaterEqual(
          {reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()});
    } else {
      iter->SeekToEqual(
          {reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()});
      iter->Next();
    }
    is_valid_ = SeekToVisibleAsc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::Prev() {
  if (!is_valid_) {
    return false;
  }
  btree_->session_->ExecSync([&]() {
    auto iter = std::visit([&](auto* btree) { return btree->NewBTreeIter(); }, btree_->btree_);
    if (is_removed_) {
      iter->SeekToLastLessEqual(
          {reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()});
    } else {
      iter->SeekToEqual(
          {reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()});
      iter->Prev();
    }
    is_valid_ = SeekToVisibleDesc(iter.get());
  });
  return is_valid_;
}

bool LeanCursor::IsValid() const {
  return is_valid_;
}

Slice LeanCursor::CurrentKey() const {
  return {reinterpret_cast<const uint8_t*>(current_key_.data()), current_key_.size()};
}

Slice LeanCursor::CurrentValue() const {
  return {reinterpret_cast<const uint8_t*>(current_value_.data()), current_value_.size()};
}

Result<void> LeanCursor::RemoveCurrent() {
  if (!IsValid()) {
    return Error::General("Cursor not valid");
  }
  auto key = CurrentKey();
  auto res = btree_->Remove(key);
  if (res) {
    is_removed_ = true;
  }
  return res;
}

Result<void> LeanCursor::UpdateCurrent(Slice new_value) {
  (void)new_value;
  // Update via cursor is not supported; would require UpdatePartial with UpdateDesc
  return Error::General("Update via cursor not supported");
}

void LeanCursor::Close() {
  is_valid_ = false;
  is_removed_ = false;
  current_key_.clear();
  current_value_.clear();
}

void LeanCursor::RecordCurrent(BTreeIter* iter) {
  if (!iter->Valid()) {
    current_key_.clear();
    current_value_.clear();
    return;
  }
  iter->AssembleKey();
  auto key = iter->Key();
  auto value = iter->Val();
  current_key_.assign(reinterpret_cast<const char*>(key.data()), key.size());
  current_value_.assign(reinterpret_cast<const char*>(value.data()), value.size());
  is_removed_ = false;
}

auto LeanCursor::SeekToVisibleAsc(BTreeIter* iter) -> bool {
  bool found = false;
  std::visit(
      [&](auto* btree) {
        using T = std::decay_t<decltype(*btree)>;
        if constexpr (std::is_same_v<T, BasicKV>) {
          found = iter->Valid();
          if (found) {
            RecordCurrent(iter);
          }
        } else {
          assert(btree_->session_->session_->GetTxMgr()->IsTxStarted() &&
                 "LeanCursor MVCC operation should be in a transaction");
          for (; !found && iter->Valid(); iter->Next()) {
            btree->GetVisibleTuple(iter->Val(), [&](Slice visible_val) {
              found = true;
              iter->AssembleKey();
              auto key = iter->Key();
              current_key_.assign(reinterpret_cast<const char*>(key.data()), key.size());
              current_value_.assign(reinterpret_cast<const char*>(visible_val.data()),
                                    visible_val.size());
              is_removed_ = false;
            });
          }
        }
      },
      btree_->btree_);
  if (!found) {
    current_key_.clear();
    current_value_.clear();
  }
  return found;
}

auto LeanCursor::SeekToVisibleDesc(BTreeIter* iter) -> bool {
  bool found = false;
  std::visit(
      [&](auto* btree) {
        using T = std::decay_t<decltype(*btree)>;
        if constexpr (std::is_same_v<T, BasicKV>) {
          found = iter->Valid();
          if (found) {
            RecordCurrent(iter);
          }
        } else {
          assert(btree_->session_->session_->GetTxMgr()->IsTxStarted() &&
                 "LeanCursor MVCC operation should be in a transaction");
          for (; !found && iter->Valid(); iter->Prev()) {
            btree->GetVisibleTuple(iter->Val(), [&](Slice visible_val) {
              found = true;
              iter->AssembleKey();
              auto key = iter->Key();
              current_key_.assign(reinterpret_cast<const char*>(key.data()), key.size());
              current_value_.assign(reinterpret_cast<const char*>(visible_val.data()),
                                    visible_val.size());
              is_removed_ = false;
            });
          }
        }
      },
      btree_->btree_);
  if (!found) {
    current_key_.clear();
    current_value_.clear();
  }
  return found;
}

} // namespace leanstore
