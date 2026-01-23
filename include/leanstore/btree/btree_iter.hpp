#pragma once

#include "leanstore/btree/btree_iter_pessistic.hpp"

namespace leanstore {

class BTreeIterMut;

class BTreeIter : public BTreeIterPessistic {
public:
  explicit BTreeIter(BTreeGeneric& btree)
      : BTreeIterPessistic(btree, LatchMode::kSharedPessimistic) {
  }

  void IntoBtreeIterMut(BTreeIterMut* iter_mut);
};

} // namespace leanstore
