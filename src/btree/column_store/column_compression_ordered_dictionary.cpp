#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <algorithm>
#include <cstring>
#include <format>
#include <numeric>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace leanstore::column_store {

namespace {

class OrderedDictionaryCompression final : public ColumnCompressionAlgorithm {
public:
  CompressionType Type() const override {
    return CompressionType::kOrderedDictionary;
  }

  bool SupportsType(ColumnType type) const override {
    return type == ColumnType::kBinary || type == ColumnType::kString;
  }

  Result<bool> TryEncode(const ColumnCompressionInput& input, ColumnMeta& meta,
                         std::vector<uint8_t>& storage) const override {
    if (!SupportsType(input.type_)) {
      return false;
    }

    // Phase 1: fast-path reject for all-null / single-value columns.
    bool has_value = false;
    bool all_equal = true;
    std::string_view first_value;
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      if (input.nulls_[i] != 0) {
        continue;
      }
      const std::string_view v = GetInputString(input, i);
      if (!has_value) {
        first_value = v;
        has_value = true;
      } else if (v != first_value) {
        all_equal = false;
      }
    }
    if (!has_value || all_equal) {
      return false;
    }

    // Phase 2: build unsorted dictionary IDs for rows and collect unique strings.
    std::vector<uint32_t> row_dict_idx(input.row_count_, 0);
    std::vector<std::string_view> unique_values;
    unique_values.reserve(input.row_count_);
    std::unordered_map<std::string_view, uint32_t> value_to_unsorted_idx;
    value_to_unsorted_idx.reserve(static_cast<size_t>(input.row_count_) * 2);
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      if (input.nulls_[i] != 0) {
        continue;
      }
      const std::string_view value = GetInputString(input, i);
      auto [it, inserted] =
          value_to_unsorted_idx.try_emplace(value, static_cast<uint32_t>(unique_values.size()));
      if (inserted) {
        unique_values.push_back(value);
      }
      row_dict_idx[i] = it->second;
    }

    const uint32_t dict_size = static_cast<uint32_t>(unique_values.size());
    if (dict_size == 0) {
      return false;
    }
    // Phase 3: order dictionary entries lexicographically and build remap table.
    std::vector<uint32_t> sorted_unique(dict_size);
    std::iota(sorted_unique.begin(), sorted_unique.end(), 0);
    std::sort(sorted_unique.begin(), sorted_unique.end(),
              [&](uint32_t lhs, uint32_t rhs) { return unique_values[lhs] < unique_values[rhs]; });

    std::vector<uint32_t> unsorted_to_sorted(dict_size);
    for (uint32_t sorted_idx = 0; sorted_idx < dict_size; ++sorted_idx) {
      unsorted_to_sorted[sorted_unique[sorted_idx]] = sorted_idx;
    }

    const uint8_t width = dict_size <= 0x100 ? 1 : (dict_size <= 0x10000 ? 2 : 4);
    meta.compression_ = static_cast<uint8_t>(Type());
    meta.value_width_ = width;
    meta.base_.u64_ = 0;
    meta.dict_offset_ = static_cast<uint32_t>(storage.size());
    meta.dict_entries_ = dict_size;

    // Phase 4: write dictionary offset/length arrays and concatenated string blob.
    std::vector<uint32_t> offsets(dict_size);
    std::vector<uint32_t> lengths(dict_size);
    uint32_t string_offset = 0;
    size_t dict_string_bytes = 0;
    for (uint32_t i = 0; i < dict_size; ++i) {
      const auto entry = unique_values[sorted_unique[i]];
      offsets[i] = string_offset;
      lengths[i] = static_cast<uint32_t>(entry.size());
      string_offset += lengths[i];
      dict_string_bytes += entry.size();
    }
    AppendBytes(storage, offsets.data(), offsets.size() * sizeof(uint32_t));
    AppendBytes(storage, lengths.data(), lengths.size() * sizeof(uint32_t));

    meta.string_offset_ = static_cast<uint32_t>(storage.size());
    if (dict_string_bytes > 0) {
      const size_t string_base = storage.size();
      storage.resize(storage.size() + dict_string_bytes);
      size_t write_offset = 0;
      for (uint32_t i = 0; i < dict_size; ++i) {
        const auto entry = unique_values[sorted_unique[i]];
        if (!entry.empty()) {
          std::memcpy(storage.data() + string_base + write_offset, entry.data(), entry.size());
          write_offset += entry.size();
        }
      }
    }

    // Phase 5: write row-wise dictionary indexes using the selected index width.
    meta.data_offset_ = static_cast<uint32_t>(storage.size());
    meta.data_bytes_ = input.row_count_ * width;
    storage.resize(storage.size() + meta.data_bytes_);
    for (uint32_t i = 0; i < input.row_count_; ++i) {
      uint32_t idx = 0;
      if (input.nulls_[i] == 0) {
        idx = unsorted_to_sorted[row_dict_idx[i]];
      }
      std::memcpy(storage.data() + meta.data_offset_ + i * width, &idx, width);
    }
    return true;
  }

  Result<void> Validate(const ColumnBlockHeader& header, const ColumnMeta& meta,
                        const std::vector<uint8_t>& storage, uint32_t col_idx) const override {
    const auto type = static_cast<ColumnType>(meta.type_);
    if (!SupportsType(type)) {
      return Error::General(
          std::format("ordered-dictionary unsupported type for column {}", col_idx));
    }
    if (meta.dict_entries_ == 0) {
      return Error::General(std::format("empty dictionary for column {}", col_idx));
    }
    if (!(meta.value_width_ == 1 || meta.value_width_ == 2 || meta.value_width_ == 4)) {
      return Error::General(std::format("invalid dictionary index width for column {}", col_idx));
    }
    const uint64_t expected_data = static_cast<uint64_t>(header.row_count_) * meta.value_width_;
    if (meta.data_bytes_ != expected_data) {
      return Error::General(std::format("dictionary data bytes mismatch for column {}", col_idx));
    }
    if (meta.data_offset_ > storage.size()) {
      return Error::General(
          std::format("dictionary data offset out of range for column {}", col_idx));
    }
    const uint64_t dict_bytes = static_cast<uint64_t>(meta.dict_entries_) * sizeof(uint32_t) * 2;
    if (meta.dict_offset_ > storage.size() || dict_bytes > storage.size() - meta.dict_offset_) {
      return Error::General(std::format("dictionary out of range for column {}", col_idx));
    }
    if (meta.string_offset_ > storage.size()) {
      return Error::General(std::format("string offset out of range for column {}", col_idx));
    }
    const uint64_t dict_end = static_cast<uint64_t>(meta.dict_offset_) + dict_bytes;
    if (dict_end > meta.string_offset_) {
      return Error::General(std::format("dictionary overlaps string area for column {}", col_idx));
    }
    if (meta.string_offset_ > meta.data_offset_) {
      return Error::General(std::format("string/data layout invalid for column {}", col_idx));
    }
    // Dictionary strings must live in the gap between metadata arrays and row index bytes.
    const uint64_t string_bytes = static_cast<uint64_t>(meta.data_offset_) - meta.string_offset_;
    for (uint32_t i = 0; i < meta.dict_entries_; ++i) {
      uint32_t off = 0;
      uint32_t len = 0;
      if (auto res = LoadDictionaryEntry(meta, storage, i, off, len); !res) {
        return std::move(res.error());
      }
      if (static_cast<uint64_t>(off) + len > string_bytes) {
        return Error::General(std::format("dictionary string out of range for column {}", col_idx));
      }
    }
    return {};
  }

  Result<int> Compare(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      const Datum& target, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("ordered-dictionary compare type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    const uint32_t idx = static_cast<uint32_t>(
        LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    uint32_t off = 0;
    uint32_t len = 0;
    if (auto res = LoadDictionaryEntry(meta, storage, idx, off, len); !res) {
      return std::move(res.error());
    }
    const char* ptr = reinterpret_cast<const char*>(storage.data() + meta.string_offset_ + off);
    return CompareBytesByLength(ptr, len, target.str.data, target.str.size);
  }

  Result<void> Decode(const ColumnMeta& meta, const std::vector<uint8_t>& storage, uint32_t row_idx,
                      Datum& out, ColumnType type) const override {
    if (!SupportsType(type)) {
      return Error::General("ordered-dictionary decode type mismatch");
    }
    const uint8_t* data_base = storage.data() + meta.data_offset_;
    const uint32_t idx = static_cast<uint32_t>(
        LoadUnsigned(data_base + row_idx * meta.value_width_, meta.value_width_));
    uint32_t off = 0;
    uint32_t len = 0;
    if (auto res = LoadDictionaryEntry(meta, storage, idx, off, len); !res) {
      return std::move(res.error());
    }
    out.str.data = reinterpret_cast<const char*>(storage.data() + meta.string_offset_ + off);
    out.str.size = len;
    return {};
  }
};

} // namespace

const ColumnCompressionAlgorithm& OrderedDictionaryAlgorithm() {
  static const OrderedDictionaryCompression kAlgorithm;
  return kAlgorithm;
}

} // namespace leanstore::column_store
