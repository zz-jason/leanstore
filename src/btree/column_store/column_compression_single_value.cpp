#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <cstring>
#include <format>
#include <string_view>

namespace leanstore::column_store {

namespace {

Result<uint32_t> LoadSingleValueStringLengthChecked(const ColumnMeta& meta,
                                                    const uint8_t* data_base) {
  // Single-value strings are encoded as: uint32_t len + len bytes payload.
  if (meta.data_bytes_ < sizeof(uint32_t)) {
    return Error::General("single-value string payload too small");
  }
  uint32_t len = 0;
  std::memcpy(&len, data_base, sizeof(uint32_t));
  const uint32_t payload_bytes = meta.data_bytes_ - sizeof(uint32_t);
  // Reject malformed metadata where the inline length points past the payload.
  if (len > payload_bytes) {
    return Error::General("single-value string length out of range");
  }
  return len;
}

class SingleValueCompression final : public ColumnCompressionAlgorithm {
public:
  CompressionType Type() const override {
    return CompressionType::kSingleValue;
  }

  bool SupportsType(ColumnType type) const override {
    switch (type) {
    case ColumnType::kBool:
    case ColumnType::kInt32:
    case ColumnType::kInt64:
    case ColumnType::kUInt64:
    case ColumnType::kFloat32:
    case ColumnType::kFloat64:
    case ColumnType::kBinary:
    case ColumnType::kString:
      return true;
    }
    return false;
  }

  Result<bool> TryEncode(const ColumnCompressionInput& input, ColumnMeta& meta,
                         std::vector<uint8_t>& storage) const override {
    if (!SupportsType(input.type_)) {
      return false;
    }

    // Phase 1: profile non-null values and verify whether the column is constant.
    bool has_value = false;
    bool all_equal = true;
    uint32_t first_len = 0;
    int64_t first_i64 = 0;
    uint64_t first_u64 = 0;
    double first_f64 = 0;
    std::string_view first_str;
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      if (input.nulls_[i] != 0) {
        continue;
      }
      switch (input.type_) {
      case ColumnType::kBool:
      case ColumnType::kInt32:
      case ColumnType::kInt64: {
        const int64_t v = input.ints_[i];
        if (!has_value) {
          first_i64 = v;
          has_value = true;
        } else if (v != first_i64) {
          all_equal = false;
        }
        break;
      }
      case ColumnType::kUInt64: {
        const uint64_t v = input.uints_[i];
        if (!has_value) {
          first_u64 = v;
          has_value = true;
        } else if (v != first_u64) {
          all_equal = false;
        }
        break;
      }
      case ColumnType::kFloat32:
      case ColumnType::kFloat64: {
        const double v = input.doubles_[i];
        if (!has_value) {
          first_f64 = v;
          has_value = true;
        } else if (v != first_f64) {
          all_equal = false;
        }
        break;
      }
      case ColumnType::kBinary:
      case ColumnType::kString: {
        const std::string_view v = GetInputString(input, i);
        if (!has_value) {
          first_str = v;
          first_len = static_cast<uint32_t>(v.size());
          has_value = true;
        } else if (v != first_str) {
          all_equal = false;
        }
        break;
      }
      }
      if (!all_equal) {
        break;
      }
    }

    if (has_value && !all_equal) {
      return false;
    }

    // Phase 2: materialize algorithm-specific metadata for single-value layout.
    meta.compression_ = static_cast<uint8_t>(Type());
    meta.base_.u64_ = 0;
    meta.dict_offset_ = 0;
    meta.dict_entries_ = 0;
    meta.string_offset_ = 0;
    meta.data_offset_ = static_cast<uint32_t>(storage.size());
    // Phase 3: write the single representative payload (or type-default for all-null).
    switch (input.type_) {
    case ColumnType::kBool:
    case ColumnType::kInt32:
    case ColumnType::kInt64: {
      const uint8_t width = PhysicalTypeWidth(input.type_);
      const int64_t value = has_value ? first_i64 : 0;
      meta.value_width_ = width;
      meta.data_bytes_ = width;
      AppendBytes(storage, &value, width);
      return true;
    }
    case ColumnType::kUInt64: {
      const uint64_t value = has_value ? first_u64 : 0;
      meta.value_width_ = sizeof(uint64_t);
      meta.data_bytes_ = sizeof(uint64_t);
      AppendBytes(storage, &value, sizeof(uint64_t));
      return true;
    }
    case ColumnType::kFloat32: {
      const float value = static_cast<float>(has_value ? first_f64 : 0.0);
      meta.value_width_ = sizeof(float);
      meta.data_bytes_ = sizeof(float);
      AppendBytes(storage, &value, sizeof(float));
      return true;
    }
    case ColumnType::kFloat64: {
      const double value = has_value ? first_f64 : 0.0;
      meta.value_width_ = sizeof(double);
      meta.data_bytes_ = sizeof(double);
      AppendBytes(storage, &value, sizeof(double));
      return true;
    }
    case ColumnType::kBinary:
    case ColumnType::kString: {
      const uint32_t len = has_value ? first_len : 0;
      meta.value_width_ = 0;
      meta.data_bytes_ = sizeof(uint32_t) + len;
      AppendBytes(storage, &len, sizeof(uint32_t));
      if (len > 0) {
        AppendBytes(storage, first_str.data(), first_str.size());
      }
      return true;
    }
    }
    return Error::General("unsupported single-value type");
  }

  Result<void> Validate(const ColumnBlockHeader&, const ColumnMeta& meta,
                        const std::vector<uint8_t>& storage, uint32_t col_idx) const override {
    const auto type = static_cast<ColumnType>(meta.type_);
    if (!SupportsType(type)) {
      return Error::General(std::format("single-value unsupported type for column {}", col_idx));
    }
    if (type == ColumnType::kBinary || type == ColumnType::kString) {
      if (meta.data_bytes_ < sizeof(uint32_t)) {
        return Error::General(
            std::format("single-value string payload too small for column {}", col_idx));
      }
      if (meta.value_width_ != 0) {
        return Error::General(
            std::format("single-value string width must be zero for column {}", col_idx));
      }
      if (meta.data_offset_ > storage.size() ||
          meta.data_bytes_ > storage.size() - meta.data_offset_) {
        return Error::General(
            std::format("single-value string payload out of range for column {}", col_idx));
      }
      if (auto len = LoadSingleValueStringLengthChecked(meta, storage.data() + meta.data_offset_);
          !len) {
        return Error::General(
            std::format("single-value string length out of range for column {}", col_idx));
      }
      return {};
    }

    const uint8_t expected = PhysicalTypeWidth(type);
    if (expected == 0) {
      return Error::General(std::format("invalid physical width for column {}", col_idx));
    }
    if (meta.value_width_ != expected || meta.data_bytes_ != expected) {
      return Error::General(std::format("single-value width mismatch for column {}", col_idx));
    }
    return {};
  }

  Result<int> Compare(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t,
                      const Datum& target, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("single-value compare type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    switch (type) {
    case ColumnType::kBool: {
      const uint8_t v = *data_base;
      const uint8_t t = target.b ? 1 : 0;
      return v < t ? -1 : (v > t ? 1 : 0);
    }
    case ColumnType::kInt32: {
      const int32_t v = static_cast<int32_t>(LoadSigned(data_base, meta.value_width_));
      if (v < target.i32) {
        return -1;
      }
      if (v > target.i32) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kInt64: {
      const int64_t v = LoadSigned(data_base, meta.value_width_);
      if (v < target.i64) {
        return -1;
      }
      if (v > target.i64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kUInt64: {
      const uint64_t v = LoadUnsigned(data_base, meta.value_width_);
      if (v < target.u64) {
        return -1;
      }
      if (v > target.u64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kFloat32: {
      float v = 0;
      std::memcpy(&v, data_base, sizeof(float));
      if (v < target.f32) {
        return -1;
      }
      if (v > target.f32) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kFloat64: {
      double v = 0;
      std::memcpy(&v, data_base, sizeof(double));
      if (v < target.f64) {
        return -1;
      }
      if (v > target.f64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kBinary:
    case ColumnType::kString: {
      auto len = LoadSingleValueStringLengthChecked(meta, data_base);
      if (!len) {
        return std::move(len.error());
      }
      const char* ptr = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
      return CompareBytesByLength(ptr, len.value(), target.str.data, target.str.size);
    }
    }
    return Error::General("single-value compare unsupported type");
  }

  Result<void> Decode(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t,
                      Datum& out, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("single-value decode type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    switch (type) {
    case ColumnType::kBool:
      out.b = (*data_base) != 0;
      return {};
    case ColumnType::kInt32:
      out.i32 = static_cast<int32_t>(LoadSigned(data_base, meta.value_width_));
      return {};
    case ColumnType::kInt64:
      out.i64 = LoadSigned(data_base, meta.value_width_);
      return {};
    case ColumnType::kUInt64:
      out.u64 = LoadUnsigned(data_base, meta.value_width_);
      return {};
    case ColumnType::kFloat32: {
      float v = 0;
      std::memcpy(&v, data_base, sizeof(float));
      out.f32 = v;
      return {};
    }
    case ColumnType::kFloat64: {
      double v = 0;
      std::memcpy(&v, data_base, sizeof(double));
      out.f64 = v;
      return {};
    }
    case ColumnType::kBinary:
    case ColumnType::kString: {
      auto len = LoadSingleValueStringLengthChecked(meta, data_base);
      if (!len) {
        return std::move(len.error());
      }
      out.str.data = reinterpret_cast<const char*>(data_base + sizeof(uint32_t));
      out.str.size = len.value();
      return {};
    }
    }
    return Error::General("single-value decode unsupported type");
  }
};

} // namespace

const ColumnCompressionAlgorithm& SingleValueAlgorithm() {
  static const SingleValueCompression kAlgorithm;
  return kAlgorithm;
}

} // namespace leanstore::column_store
