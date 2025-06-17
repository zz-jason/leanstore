#include "leanstore/btree/core/btree_iter.hpp"

#include "leanstore/btree/core/btree_iter_mut.hpp"

namespace leanstore::storage::btree {

std::unique_ptr<BTreeIterMut> BTreeIter::IntoBtreeIterMut() {
  auto iter_mut = std::make_unique<BTreeIterMut>(btree_);
  if (Valid()) {
    AssembleKey();
    auto current_key = Key();
    Reset();
    iter_mut->SeekToFirstGreaterEqual(current_key);
    return iter_mut;
  }

  Reset();
  return iter_mut;
}

} // namespace leanstore::storage::btree