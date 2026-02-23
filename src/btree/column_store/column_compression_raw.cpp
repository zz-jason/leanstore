#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <cstring>
#include <format>

namespace leanstore::column_store {

namespace {

class RawCompression final : public ColumnCompressionAlgorithm {
public:
  CompressionType Type() const override {
    return CompressionType::kRaw;
  }

  bool SupportsType(ColumnType type) const override {
    switch (type) {
    case ColumnType::kBool:
    case ColumnType::kInt32:
    case ColumnType::kInt64:
    case ColumnType::kUInt64:
    case ColumnType::kFloat32:
    case ColumnType::kFloat64:
      return true;
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

    // Phase 1: initialize fixed-width raw layout metadata.
    const uint8_t width = PhysicalTypeWidth(input.type_);
    meta.compression_ = static_cast<uint8_t>(Type());
    meta.value_width_ = width;
    meta.base_.u64_ = 0;
    meta.dict_offset_ = 0;
    meta.dict_entries_ = 0;
    meta.string_offset_ = 0;
    meta.data_offset_ = static_cast<uint32_t>(storage.size());
    meta.data_bytes_ = input.row_count_ * width;
    storage.resize(storage.size() + meta.data_bytes_);
    // Phase 2: materialize row-wise raw bytes (nulls are encoded as typed zero).
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      switch (input.type_) {
      case ColumnType::kBool: {
        const uint8_t v = input.nulls_[i] == 0 ? (input.ints_[i] != 0 ? 1 : 0) : 0;
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
        break;
      }
      case ColumnType::kInt32:
      case ColumnType::kInt64: {
        const int64_t v = input.nulls_[i] == 0 ? input.ints_[i] : 0;
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
        break;
      }
      case ColumnType::kUInt64: {
        const uint64_t v = input.nulls_[i] == 0 ? input.uints_[i] : 0;
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
        break;
      }
      case ColumnType::kFloat32: {
        const float v = input.nulls_[i] == 0 ? static_cast<float>(input.doubles_[i]) : 0.0f;
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
        break;
      }
      case ColumnType::kFloat64: {
        const double v = input.nulls_[i] == 0 ? input.doubles_[i] : 0.0;
        std::memcpy(storage.data() + meta.data_offset_ + i * width, &v, width);
        break;
      }
      case ColumnType::kBinary:
      case ColumnType::kString:
        return Error::General("raw encoding does not support string columns");
      }
    }
    return true;
  }

  Result<void> Validate(const ColumnBlockHeader& header, const ColumnMeta& meta,
                        const std::vector<uint8_t>&, uint32_t col_idx) const override {
    const auto type = static_cast<ColumnType>(meta.type_);
    if (!SupportsType(type)) {
      return Error::General(std::format("raw unsupported type for column {}", col_idx));
    }
    const uint8_t expected_width = PhysicalTypeWidth(type);
    if (meta.value_width_ != expected_width) {
      return Error::General(std::format("raw width mismatch for column {}", col_idx));
    }
    const uint64_t expected_bytes = static_cast<uint64_t>(header.row_count_) * expected_width;
    if (meta.data_bytes_ != expected_bytes) {
      return Error::General(std::format("raw data bytes mismatch for column {}", col_idx));
    }
    return {};
  }

  Result<int> Compare(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      const Datum& target, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("raw compare type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    switch (type) {
    case ColumnType::kBool: {
      const uint8_t v = *(data_base + row_idx * meta.value_width_);
      const uint8_t t = target.b ? 1 : 0;
      return v < t ? -1 : (v > t ? 1 : 0);
    }
    case ColumnType::kInt32: {
      const int32_t v = static_cast<int32_t>(
          LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_));
      if (v < target.i32) {
        return -1;
      }
      if (v > target.i32) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kInt64: {
      const int64_t v = LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_);
      if (v < target.i64) {
        return -1;
      }
      if (v > target.i64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kUInt64: {
      const uint64_t v = LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_);
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
      std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(float));
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
      std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(double));
      if (v < target.f64) {
        return -1;
      }
      if (v > target.f64) {
        return 1;
      }
      return 0;
    }
    case ColumnType::kBinary:
    case ColumnType::kString:
      break;
    }
    return Error::General("raw compare unsupported type");
  }

  Result<void> Decode(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      Datum& out, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("raw decode type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    switch (type) {
    case ColumnType::kBool: {
      const uint8_t v = *(data_base + row_idx * meta.value_width_);
      out.b = v != 0;
      return {};
    }
    case ColumnType::kInt32: {
      const int32_t v = static_cast<int32_t>(
          LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_));
      out.i32 = v;
      return {};
    }
    case ColumnType::kInt64:
      out.i64 = LoadSigned(data_base + row_idx * meta.value_width_, meta.value_width_);
      return {};
    case ColumnType::kUInt64:
      out.u64 = LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_);
      return {};
    case ColumnType::kFloat32: {
      float v = 0;
      std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(float));
      out.f32 = v;
      return {};
    }
    case ColumnType::kFloat64: {
      double v = 0;
      std::memcpy(&v, data_base + row_idx * meta.value_width_, sizeof(double));
      out.f64 = v;
      return {};
    }
    case ColumnType::kBinary:
    case ColumnType::kString:
      break;
    }
    return Error::General("raw decode unsupported type");
  }
};

} // namespace

const ColumnCompressionAlgorithm& RawAlgorithm() {
  static const RawCompression kAlgorithm;
  return kAlgorithm;
}

} // namespace leanstore::column_store
