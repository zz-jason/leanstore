#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <algorithm>
#include <cstring>
#include <format>
#include <limits>

namespace leanstore::column_store {

namespace {

class TruncationCompression final : public ColumnCompressionAlgorithm {
public:
  CompressionType Type() const override {
    return CompressionType::kTruncation;
  }

  bool SupportsType(ColumnType type) const override {
    switch (type) {
    case ColumnType::kBool:
    case ColumnType::kInt32:
    case ColumnType::kInt64:
    case ColumnType::kUInt64:
      return true;
    case ColumnType::kFloat32:
    case ColumnType::kFloat64:
    case ColumnType::kBinary:
    case ColumnType::kString:
      return false;
    }
    return false;
  }

  Result<bool> TryEncode(const ColumnCompressionInput& input, ColumnMeta& meta,
                         std::vector<uint8_t>& storage) const override {
    if (!SupportsType(input.type_)) {
      return false;
    }

    if (input.type_ == ColumnType::kUInt64) {
      // Phase 1: profile min/max on non-null values.
      uint64_t min_value = 0;
      uint64_t max_value = 0;
      bool has_value = false;
      for (uint32_t i = 0; i < input.row_count_; ++i) {
        if (input.nulls_[i] != 0) {
          continue;
        }
        const uint64_t v = input.uints_[i];
        if (!has_value) {
          min_value = max_value = v;
          has_value = true;
        } else {
          min_value = std::min(min_value, v);
          max_value = std::max(max_value, v);
        }
      }
      if (!has_value) {
        return false;
      }
      // Phase 2: decide whether truncation beats raw-width storage.
      const uint8_t type_width = sizeof(uint64_t);
      const uint64_t range = max_value - min_value;
      const uint8_t width = WidthForRange(range);
      if (width >= type_width) {
        return false;
      }
      // Phase 3: emit truncation metadata and delta payload.
      meta.compression_ = static_cast<uint8_t>(Type());
      meta.value_width_ = width;
      meta.base_.u64_ = min_value;
      meta.dict_offset_ = 0;
      meta.dict_entries_ = 0;
      meta.string_offset_ = 0;
      meta.data_offset_ = static_cast<uint32_t>(storage.size());
      meta.data_bytes_ = input.row_count_ * width;
      storage.resize(storage.size() + meta.data_bytes_);
      for (uint32_t i = 0; i < input.row_count_; ++i) {
        uint64_t delta = 0;
        if (input.nulls_[i] == 0) {
          delta = input.uints_[i] - min_value;
        }
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &delta, width);
      }
      return true;
    }

    // Phase 1: profile min/max on signed domain (bool/int32/int64 all use ints_).
    const uint8_t type_width = PhysicalTypeWidth(input.type_);
    int64_t min_value = 0;
    int64_t max_value = 0;
    bool has_value = false;
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      if (input.nulls_[i] != 0) {
        continue;
      }
      const int64_t v = input.ints_[i];
      if (!has_value) {
        min_value = max_value = v;
        has_value = true;
      } else {
        min_value = std::min(min_value, v);
        max_value = std::max(max_value, v);
      }
    }
    if (!has_value) {
      return false;
    }
    // Phase 2: decide whether delta width is strictly smaller than physical width.
    const __int128 range = static_cast<__int128>(max_value) - static_cast<__int128>(min_value);
    if (range < 0 || range > static_cast<__int128>(std::numeric_limits<uint64_t>::max())) {
      return false;
    }
    const uint8_t width = WidthForRange(static_cast<uint64_t>(range));
    if (width >= type_width) {
      return false;
    }

    // Phase 3: emit truncation metadata and row-wise deltas.
    meta.compression_ = static_cast<uint8_t>(Type());
    meta.value_width_ = width;
    meta.base_.i64_ = min_value;
    meta.dict_offset_ = 0;
    meta.dict_entries_ = 0;
    meta.string_offset_ = 0;
    meta.data_offset_ = static_cast<uint32_t>(storage.size());
    meta.data_bytes_ = input.row_count_ * width;
    storage.resize(storage.size() + meta.data_bytes_);
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      uint64_t delta = 0;
      if (input.nulls_[i] == 0) {
        const __int128 delta_i =
            static_cast<__int128>(input.ints_[i]) - static_cast<__int128>(min_value);
        delta = static_cast<uint64_t>(delta_i);
      }
      std::memcpy(storage.data() + meta.data_offset_ + i * width, &delta, width);
    }
    return true;
  }

  Result<void> Validate(const ColumnBlockHeader& header, const ColumnMeta& meta,
                        const std::vector<uint8_t>&, uint32_t col_idx) const override {
    const auto type = static_cast<ColumnType>(meta.type_);
    if (!SupportsType(type)) {
      return Error::General(std::format("truncation unsupported type for column {}", col_idx));
    }
    const uint8_t type_width = PhysicalTypeWidth(type);
    if (meta.value_width_ == 0 || meta.value_width_ >= type_width) {
      return Error::General(std::format("truncation width invalid for column {}", col_idx));
    }
    const uint64_t expected = static_cast<uint64_t>(header.row_count_) * meta.value_width_;
    if (meta.data_bytes_ != expected) {
      return Error::General(std::format("truncation bytes mismatch for column {}", col_idx));
    }
    return {};
  }

  Result<int> Compare(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      const Datum& target, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("truncation compare type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    const auto* delta_ptr = data_base + row_idx * meta.value_width_;
    switch (type) {
    case ColumnType::kBool:
    case ColumnType::kInt32:
    case ColumnType::kInt64: {
      const uint64_t delta_u = LoadUnsigned(delta_ptr, meta.value_width_);
      const int64_t value = meta.base_.i64_ + static_cast<int64_t>(delta_u);
      const int64_t target_val = (type == ColumnType::kInt32)
                                     ? target.i32
                                     : (type == ColumnType::kBool ? target.b : target.i64);
      if (value < target_val) {
        return -1;
      }
      if (value > target_val) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kUInt64: {
      const uint64_t delta_u = LoadUnsigned(delta_ptr, meta.value_width_);
      const uint64_t value = meta.base_.u64_ + delta_u;
      if (value < target.u64) {
        return -1;
      }
      if (value > target.u64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kFloat32:
    case ColumnType::kFloat64:
    case ColumnType::kBinary:
    case ColumnType::kString:
      break;
    }
    return Error::General("truncation compare unsupported type");
  }

  Result<void> Decode(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      Datum& out, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("truncation decode type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    const auto* delta_ptr = data_base + row_idx * meta.value_width_;
    const uint64_t delta_u = LoadUnsigned(delta_ptr, meta.value_width_);
    switch (type) {
    case ColumnType::kBool: {
      const int64_t value = meta.base_.i64_ + static_cast<int64_t>(delta_u);
      out.b = value != 0;
      return {};
    }
    case ColumnType::kInt32: {
      const int64_t value = meta.base_.i64_ + static_cast<int64_t>(delta_u);
      out.i32 = static_cast<int32_t>(value);
      return {};
    }
    case ColumnType::kInt64:
      out.i64 = meta.base_.i64_ + static_cast<int64_t>(delta_u);
      return {};
    case ColumnType::kUInt64:
      out.u64 = meta.base_.u64_ + delta_u;
      return {};
    case ColumnType::kFloat32:
    case ColumnType::kFloat64:
    case ColumnType::kBinary:
    case ColumnType::kString:
      break;
    }
    return Error::General("truncation decode unsupported type");
  }
};

} // namespace

const ColumnCompressionAlgorithm& TruncationAlgorithm() {
  static const TruncationCompression kAlgorithm;
  return kAlgorithm;
}

} // namespace leanstore::column_store
