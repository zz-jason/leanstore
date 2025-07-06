#include "leanstore/btree/core/btree_iter_mut.hpp"

#include "leanstore/btree/core/btree_iter.hpp"

namespace leanstore::storage::btree {

void BTreeIterMut::IntoBtreeIter(BTreeIter* iter) {
  if (Valid()) {
    AssembleKey();
    auto current_key = Key();
    Reset();

    iter->SeekToFirstGreaterEqual(current_key);
    return;
  }

  Reset();
}

} // namespace leanstore::storage::btree
