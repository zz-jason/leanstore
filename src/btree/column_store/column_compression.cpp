#include "btree/column_store/column_compression_internal.hpp"
#include "leanstore/base/error.hpp"

#include <format>

namespace leanstore::column_store {

const ColumnCompressionAlgorithm* FindCompressionAlgorithm(CompressionType type) {
  switch (type) {
  case CompressionType::kSingleValue:
    return &SingleValueAlgorithm();
  case CompressionType::kTruncation:
    return &TruncationAlgorithm();
  case CompressionType::kOrderedDictionary:
    return &OrderedDictionaryAlgorithm();
  case CompressionType::kRaw:
    return &RawAlgorithm();
  }
  return nullptr;
}

const std::vector<const ColumnCompressionAlgorithm*>& CompressionCandidates(ColumnType type) {
  static const std::vector<const ColumnCompressionAlgorithm*> kIntCandidates{
      &SingleValueAlgorithm(), &TruncationAlgorithm(), &RawAlgorithm()};
  static const std::vector<const ColumnCompressionAlgorithm*> kUIntCandidates{
      &SingleValueAlgorithm(), &TruncationAlgorithm(), &RawAlgorithm()};
  static const std::vector<const ColumnCompressionAlgorithm*> kFloatCandidates{
      &SingleValueAlgorithm(), &RawAlgorithm()};
  static const std::vector<const ColumnCompressionAlgorithm*> kStringCandidates{
      &SingleValueAlgorithm(), &OrderedDictionaryAlgorithm()};

  switch (type) {
  case ColumnType::kBool:
  case ColumnType::kInt32:
  case ColumnType::kInt64:
    return kIntCandidates;
  case ColumnType::kUInt64:
    return kUIntCandidates;
  case ColumnType::kFloat32:
  case ColumnType::kFloat64:
    return kFloatCandidates;
  case ColumnType::kBinary:
  case ColumnType::kString:
    return kStringCandidates;
  }

  static const std::vector<const ColumnCompressionAlgorithm*> kNone;
  return kNone;
}

Result<void> EncodeColumnWithCompression(const ColumnCompressionInput& input, ColumnMeta& meta,
                                         std::vector<uint8_t>& storage) {
  const auto& candidates = CompressionCandidates(input.type_);
  if (candidates.empty()) {
    return Error::General("no compression candidates for column type");
  }

  for (const auto* algorithm : candidates) {
    if (algorithm == nullptr || !algorithm->SupportsType(input.type_)) {
      continue;
    }
    const size_t storage_base = storage.size();
    ColumnMeta candidate = meta;
    candidate.compression_ = 0;
    candidate.value_width_ = 0;
    candidate.base_.u64_ = 0;
    candidate.data_offset_ = static_cast<uint32_t>(storage_base);
    candidate.data_bytes_ = 0;
    candidate.dict_offset_ = 0;
    candidate.dict_entries_ = 0;
    candidate.string_offset_ = 0;

    auto encoded = algorithm->TryEncode(input, candidate, storage);
    if (!encoded) {
      storage.resize(storage_base);
      return std::move(encoded.error());
    }
    if (encoded.value()) {
      meta = candidate;
      return {};
    }
    storage.resize(storage_base);
  }

  return Error::General(
      std::format("no compression algorithm accepted column type {}", ToString(input.type_)));
}

} // namespace leanstore::column_store
