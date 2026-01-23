#include "leanstore/btree/btree_iter.hpp"

#include "leanstore/btree/btree_iter_mut.hpp"

namespace leanstore {

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

} // namespace leanstore