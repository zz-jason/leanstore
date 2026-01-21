#include "leanstore/btree/btree_iter_mut.hpp"

#include "leanstore/btree/btree_iter.hpp"

namespace leanstore {

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

} // namespace leanstore
