#pragma once

#include "BTreePessimisticIterator.hpp"

namespace leanstore {
namespace storage {
namespace btree {

class BTreeSharedIterator : public BTreePessimisticIterator {
public:
  BTreeSharedIterator(BTreeGeneric& btree, const LATCH_FALLBACK_MODE mode =
                                               LATCH_FALLBACK_MODE::SHARED)
      : BTreePessimisticIterator(btree, mode) {
  }
};

} // namespace btree
} // namespace storage
} // namespace leanstore
