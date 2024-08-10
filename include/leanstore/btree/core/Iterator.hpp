#pragma once

#include "leanstore/Slice.hpp"

namespace leanstore::storage::btree {

class Iterator {
public:
  //------------------------------------------------------------------------------------------------
  // Constructor and Destructor
  //------------------------------------------------------------------------------------------------
  virtual ~Iterator() = default;

  //------------------------------------------------------------------------------------------------
  // Interfaces for exact key seeking
  //------------------------------------------------------------------------------------------------

  //! Seek for the exact key in the tree
  //! @return true if the key is found, false otherwise
  virtual bool SeekExact(Slice key) = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for ascending iteration
  //------------------------------------------------------------------------------------------------

  //! Seek for the first key in the tree
  //! @return true if the key is found, false otherwise
  virtual bool SeekForFirst() = 0;

  //! Seek for the first key in the tree which >= the given key
  //! @return true if the key is found, false otherwise
  virtual bool SeekForNext(Slice key) = 0;

  //! Whether a next key exists in the tree
  //! @return true if the next key exists, false otherwise
  virtual bool HasNext() = 0;

  //! Iterate to the next key in the tree
  //! @return true if the next key exists, false otherwise
  virtual bool Next() = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for descending iteration
  //------------------------------------------------------------------------------------------------

  //! Seek for the last key in the tree
  //! @return true if the key is found, false otherwise
  virtual bool SeekForLast() = 0;

  //! Seek for the last key in the tree which <= the given key
  //! @return true if the key is found, false otherwise
  virtual bool SeekForPrev(Slice key) = 0;

  //! Whether a previous key exists in the tree
  //! @return true if the previous key exists, false otherwise
  virtual bool HasPrev() = 0;

  //! Iterate to the previous key in the tree
  //! @return true if the previous key exists, false otherwise
  virtual bool Prev() = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for accessing the current iterator position
  //------------------------------------------------------------------------------------------------

  //! Get the key of the current iterator position, the key is read-only
  virtual Slice Key() = 0;

  //! Get the value of the current iterator position, the value is read-only
  virtual Slice Val() = 0;
};

} // namespace leanstore::storage::btree
