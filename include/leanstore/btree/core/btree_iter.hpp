#pragma once

#include "btree_iter_pessistic.hpp"

namespace leanstore::storage::btree {

class BTreeIterMut;

class BTreeIter : public BTreeIterPessistic {
public:
  BTreeIter(BTreeGeneric& btree) : BTreeIterPessistic(btree, LatchMode::kSharedPessimistic) {
  }

  void IntoBtreeIterMut(BTreeIterMut* iter_mut);
};

} // namespace leanstore::storage::btree
