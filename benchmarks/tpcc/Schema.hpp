#pragma once

#include "leanstore/Slice.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace leanstore::tpcc {

enum class FieldType : uint8_t {
  kBigint = 0,
  kDouble,
  kVarchar,
};

class Schema {
public:
  std::vector<FieldType> mFields;
};

class Record {
private:
  std::basic_string<uint8_t> mBuffer;

public:
  template <typename T> Record* AppendField(const T& val);

  template <typename T> T& GetField(size_t fieldId);

  Slice ToSlice() {
    return Slice(mBuffer);
  }

  static void Random(const Schema& schema, Record* record);
};

} // namespace leanstore::tpcc