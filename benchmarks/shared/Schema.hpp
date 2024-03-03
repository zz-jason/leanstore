#pragma once

#include "shared/Types.hpp"

template <typename KeyT, typename ValT> struct Relation {
public:
  // Entries: 1 to 1 160 000 * scale
  static constexpr int id = 0;

  struct Key {
    static constexpr int id = 0;
    KeyT mKey;
  };

public:
  ValT mValue;

public:
  template <class T> static unsigned foldKey(uint8_t* out, const T& key) {
    unsigned pos = 0;
    pos += Fold(out + pos, key.mKey);
    return pos;
  }

  template <class T> static unsigned unfoldKey(const uint8_t* in, T& key) {
    unsigned pos = 0;
    pos += Unfold(in + pos, key.mKey);
    return pos;
  }
  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::mKey);
  };
};

template <uint64_t size> struct BytesPayload {
  uint8_t value[size];

  BytesPayload() {
  }

  bool operator==(BytesPayload& other) {
    return (std::memcmp(value, other.value, sizeof(value)) == 0);
  }

  bool operator!=(BytesPayload& other) {
    return !(operator==(other));
  }

  BytesPayload(const BytesPayload& other) {
    std::memcpy(value, other.value, sizeof(value));
  }

  BytesPayload& operator=(const BytesPayload& other) {
    std::memcpy(value, other.value, sizeof(value));
    return *this;
  }
};
