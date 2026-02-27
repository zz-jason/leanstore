#ifdef LEAN_ENABLE_CORO

#include "leanstore/lean_cursor.hpp"

#include "leanstore/btree/btree_iter.hpp"
#include "leanstore/lean_btree.hpp"

namespace leanstore {

LeanCursor::LeanCursor(LeanBTree* btree, std::unique_ptr<BTreeIter> cursor)
    : btree_(btree),
      cursor_(std::move(cursor)) {
}

LeanCursor::~LeanCursor() {
  Close();
}

bool LeanCursor::SeekToFirst() {
  cursor_->SeekToFirst();
  return cursor_->Valid();
}

bool LeanCursor::SeekToFirstGe(Slice key) {
  cursor_->SeekToFirstGreaterEqual(key);
  return cursor_->Valid();
}

bool LeanCursor::SeekToLast() {
  cursor_->SeekToLast();
  return cursor_->Valid();
}

bool LeanCursor::SeekToLastLe(Slice key) {
  cursor_->SeekToLastLessEqual(key);
  return cursor_->Valid();
}

bool LeanCursor::Next() {
  cursor_->Next();
  return cursor_->Valid();
}

bool LeanCursor::Prev() {
  cursor_->Prev();
  return cursor_->Valid();
}

bool LeanCursor::IsValid() const {
  return cursor_->Valid();
}

Slice LeanCursor::CurrentKey() const {
  return cursor_->Key();
}

Slice LeanCursor::CurrentValue() const {
  return cursor_->Val();
}

Result<void> LeanCursor::RemoveCurrent() {
  if (!IsValid()) {
    return Error::General("Cursor not valid");
  }
  Slice key = CurrentKey();
  return btree_->Remove(key);
}

Result<void> LeanCursor::UpdateCurrent(Slice new_value) {
  (void)new_value;
  // Update via cursor is not supported; would require UpdatePartial with UpdateDesc
  return Error::General("Update via cursor not supported");
}

void LeanCursor::Close() {
  cursor_.reset();
}

} // namespace leanstore

#endif // LEAN_ENABLE_CORO
