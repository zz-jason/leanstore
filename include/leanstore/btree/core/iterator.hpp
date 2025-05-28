#pragma once

#include "leanstore/slice.hpp"

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

  /// Seek to the position of the key which = the given key
  virtual void SeekToEqual(Slice key) = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for ascending iteration
  //------------------------------------------------------------------------------------------------

  /// Seek to the position of the first key
  virtual void SeekToFirst() = 0;

  /// Seek to the position of the first key which >= the given key
  virtual void SeekToFirstGreaterEqual(Slice key) = 0;

  /// Whether a next key exists in the tree
  /// @return true if the next key exists, false otherwise
  virtual bool HasNext() = 0;

  /// Iterate to the next key in the tree
  virtual void Next() = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for descending iteration
  //------------------------------------------------------------------------------------------------

  /// Seek to the position of the last key
  virtual void SeekToLast() = 0;

  /// Seek to the position of the last key which <= the given key
  virtual void SeekToLastLessEqual(Slice key) = 0;

  /// Whether a previous key exists in the tree
  /// @return true if the previous key exists, false otherwise
  virtual bool HasPrev() = 0;

  /// Iterate to the previous key in the tree
  virtual void Prev() = 0;

  //------------------------------------------------------------------------------------------------
  // Interfaces for accessing the current iterator position
  //------------------------------------------------------------------------------------------------

  /// Whether the iterator is valid
  /// @return true if the iterator is pointing to a valid key-value pair, false otherwise
  virtual bool Valid() = 0;

  /// Get the key of the current iterator position, the key is read-only
  virtual Slice Key() = 0;

  /// Get the value of the current iterator position, the value is read-only
  virtual Slice Val() = 0;
};

} // namespace leanstore::storage::btree
