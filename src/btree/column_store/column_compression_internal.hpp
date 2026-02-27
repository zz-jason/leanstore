#pragma once

#include "leanstore/btree/column_store/column_compression.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace leanstore::column_store {

uint8_t WidthForRange(uint64_t range);
void AppendBytes(std::vector<uint8_t>& buffer, const void* data, size_t size);
int CompareBytesByLength(const char* lhs, size_t lhs_len, const char* rhs, size_t rhs_len);
int64_t LoadSigned(const uint8_t* ptr, uint8_t width);
uint64_t LoadUnsigned(const uint8_t* ptr, uint8_t width);
uint8_t PhysicalTypeWidth(ColumnType type);
std::string_view GetInputString(const ColumnCompressionInput& input, uint32_t row_idx);
Result<void> LoadDictionaryEntry(const ColumnMeta& meta, const std::vector<uint8_t>& storage,
                                 uint32_t idx, uint32_t& out_off, uint32_t& out_len);

const ColumnCompressionAlgorithm& SingleValueAlgorithm();
const ColumnCompressionAlgorithm& TruncationAlgorithm();
const ColumnCompressionAlgorithm& RawAlgorithm();
const ColumnCompressionAlgorithm& OrderedDictionaryAlgorithm();

} // namespace leanstore::column_store
