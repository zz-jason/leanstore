#pragma once

#include "btree_iter_pessistic.hpp"

#include <memory>

namespace leanstore::storage::btree {

class BTreeIterMut;

class BTreeIter : public BTreeIterPessistic {
public:
  BTreeIter(BTreeGeneric& btree) : BTreeIterPessistic(btree, LatchMode::kSharedPessimistic) {
  }

  std::unique_ptr<BTreeIterMut> IntoBtreeIterMut();
};

} // namespace leanstore::storage::btree
