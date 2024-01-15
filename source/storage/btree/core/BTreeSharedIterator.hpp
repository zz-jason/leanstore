#pragma once

#include "BTreePessimisticIterator.hpp"

namespace leanstore {
namespace storage {
namespace btree {

class BTreeSharedIterator : public BTreePessimisticIterator {
public:
  BTreeSharedIterator(BTreeGeneric& btree,
                      const LatchMode mode = LatchMode::kShared)
      : BTreePessimisticIterator(btree, mode) {
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
