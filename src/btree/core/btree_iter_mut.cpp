#include "leanstore/btree/core/btree_iter_mut.hpp"

#include "leanstore/btree/core/btree_iter.hpp"

namespace leanstore::storage::btree {

std::unique_ptr<BTreeIter> BTreeIterMut::IntoBtreeIter() {
  auto iter = std::make_unique<BTreeIter>(btree_);
  if (Valid()) {
    AssembleKey();
    auto current_key = Key();
    Reset();
    iter->SeekToFirstGreaterEqual(current_key);
    return iter;
  }

  Reset();
  return iter;
}

} // namespace leanstore::storage::btree
