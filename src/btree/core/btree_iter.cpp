#include "leanstore/btree/core/btree_iter.hpp"

#include "leanstore/btree/core/btree_iter_mut.hpp"

namespace leanstore::storage::btree {

void BTreeIter::IntoBtreeIterMut(BTreeIterMut* iter_mut) {
  if (Valid()) {
    AssembleKey();
    auto current_key = Key();
    Reset();

    iter_mut->SeekToFirstGreaterEqual(current_key);
    return;
  }

  Reset();
}

} // namespace leanstore::storage::btree