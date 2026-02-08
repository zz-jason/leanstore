#include "leanstore/table/encoding.hpp"

#include "leanstore/base/error.hpp"

#include <cstring>
#include <limits>
#include <string>
#include <utility>

namespace leanstore {

namespace {

void AppendBigEndian(std::string& buf, uint64_t v, uint32_t bytes) {
  for (uint32_t i = 0; i < bytes; ++i) {
    const uint32_t shift = (bytes - 1 - i) * 8;
    buf.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

uint64_t EncodeSigned(int64_t v, uint32_t bits) {
  const uint64_t bias = 1ull << (bits - 1);
  return static_cast<uint64_t>(v) + bias;
}

uint32_t EncodeFloat32(float f) {
  uint32_t u = 0;
  std::memcpy(&u, &f, sizeof(u));
  if (u & 0x80000000U) {
    u = ~u;
  } else {
    u |= 0x80000000U;
  }
  return u;
}

uint64_t EncodeFloat64(double d) {
  uint64_t u = 0;
  std::memcpy(&u, &d, sizeof(double));
  if (u & 0x8000000000000000ULL) {
    u = ~u;
  } else {
    u |= 0x8000000000000000ULL;
  }
  return u;
}

uint32_t ReadBigEndian32(const uint8_t* ptr) {
  return (static_cast<uint32_t>(ptr[0]) << 24) | (static_cast<uint32_t>(ptr[1]) << 16) |
         (static_cast<uint32_t>(ptr[2]) << 8) | static_cast<uint32_t>(ptr[3]);
}

uint64_t ReadBigEndian64(const uint8_t* ptr) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<uint64_t>(ptr[i]);
  }
  return value;
}

uint32_t DecodeFloat32Key(uint32_t v) {
  if (v & 0x80000000U) {
    v &= ~0x80000000U;
  } else {
    v = ~v;
  }
  return v;
}

uint64_t DecodeFloat64Key(uint64_t v) {
  if (v & 0x8000000000000000ULL) {
    v &= ~0x8000000000000000ULL;
  } else {
    v = ~v;
  }
  return v;
}

std::pair<uint8_t, bool> ValueFieldLayout(ColumnType type) {
  switch (type) {
  case ColumnType::kBinary:
  case ColumnType::kString:
    return {sizeof(uint64_t), true};
  case ColumnType::kBool:
    return {sizeof(uint8_t), false};
  case ColumnType::kInt32:
  case ColumnType::kFloat32:
    return {sizeof(uint32_t), false};
  case ColumnType::kInt64:
  case ColumnType::kUInt64:
  case ColumnType::kFloat64:
    return {sizeof(uint64_t), false};
  default:
    return {0, false};
  }
}

Result<void> EncodeFixedDatum(const Datum& datum, ColumnType type, std::string& dest,
                              bool key_encoding) {
  switch (type) {
  case ColumnType::kBool: {
    const uint8_t v = datum.b ? 1 : 0;
    dest.push_back(static_cast<char>(v));
    break;
  }
  case ColumnType::kInt32: {
    if (key_encoding) {
      const uint64_t enc = EncodeSigned(static_cast<int32_t>(datum.i32), 32);
      AppendBigEndian(dest, enc, 4);
    } else {
      const int32_t v = datum.i32;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kInt64: {
    if (key_encoding) {
      const uint64_t enc = EncodeSigned(static_cast<int64_t>(datum.i64), 64);
      AppendBigEndian(dest, enc, 8);
    } else {
      const int64_t v = datum.i64;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kUInt64: {
    const uint64_t v = datum.u64;
    if (key_encoding) {
      AppendBigEndian(dest, v, 8);
    } else {
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kFloat32: {
    if (key_encoding) {
      const uint32_t enc = EncodeFloat32(datum.f32);
      AppendBigEndian(dest, enc, 4);
    } else {
      const float v = datum.f32;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kFloat64: {
    if (key_encoding) {
      const uint64_t enc = EncodeFloat64(datum.f64);
      AppendBigEndian(dest, enc, 8);
    } else {
      const double v = datum.f64;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  default:
    return Error::General("unsupported fixed datum encode");
  }
  return {};
}

std::string ColumnNullError(size_t column_idx) {
  return "column cannot be null: " + std::to_string(column_idx);
}

} // namespace

RowEncodingLayout::RowEncodingLayout(std::vector<CodecColumnDesc> columns,
                                     std::vector<uint32_t> key_columns)
    : columns_(std::move(columns)),
      key_columns_(std::move(key_columns)) {
  value_field_widths_.reserve(columns_.size());
  is_varlen_.reserve(columns_.size());
  for (const auto& col : columns_) {
    const auto [width, is_varlen] = ValueFieldLayout(col.type_);
    value_field_widths_.push_back(width);
    is_varlen_.push_back(is_varlen ? 1 : 0);
  }
}

Result<void> EncodeValueWithLayout(const RowEncodingLayout& layout, const lean_row* row,
                                   std::string& out_value) {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  const auto& cols = layout.Columns();
  if (row->num_columns < cols.size()) {
    return Error::General("row size does not match schema");
  }

  const uint32_t null_bitmap_bytes = static_cast<uint32_t>((cols.size() + 7) / 8);
  size_t data_section_len = 0;
  size_t payload_len = 0;

  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& col = cols[i];
    const auto& datum = row->columns[i];
    const bool is_null = row->nulls != nullptr ? row->nulls[i] : false;
    if (is_null) {
      if (!col.nullable_) {
        return Error::General(ColumnNullError(i));
      }
      continue;
    }

    if (layout.ValueFieldWidths()[i] == 0) {
      return Error::General("unsupported type encode");
    }
    data_section_len += layout.ValueFieldWidths()[i];

    if (layout.IsVarlen()[i] != 0) {
      if (datum.str.size > std::numeric_limits<uint32_t>::max()) {
        return Error::General("varlen too large to encode");
      }
      if (datum.str.size > 0 && datum.str.data == nullptr) {
        return Error::General("varlen data is null");
      }
      payload_len += datum.str.size;
    }
  }

  out_value.assign(null_bitmap_bytes + data_section_len + payload_len, '\0');
  auto* out_bytes = reinterpret_cast<uint8_t*>(out_value.data());
  auto* data_ptr = out_bytes + null_bitmap_bytes;
  auto* payload_ptr = data_ptr + data_section_len;
  uint32_t payload_offset = 0;

  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& datum = row->columns[i];
    const bool is_null = row->nulls != nullptr ? row->nulls[i] : false;
    if (is_null) {
      out_bytes[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
      continue;
    }

    if (layout.IsVarlen()[i] != 0) {
      const uint32_t len = static_cast<uint32_t>(datum.str.size);
      const uint64_t encoded_slot = (static_cast<uint64_t>(len) << 32) | payload_offset;
      std::memcpy(data_ptr, &encoded_slot, sizeof(encoded_slot));
      data_ptr += sizeof(encoded_slot);
      if (len > 0) {
        std::memcpy(payload_ptr + payload_offset, datum.str.data, len);
        payload_offset += len;
      }
      continue;
    }

    switch (cols[i].type_) {
    case ColumnType::kBool: {
      *data_ptr = datum.b ? 1 : 0;
      data_ptr += sizeof(uint8_t);
      break;
    }
    case ColumnType::kInt32: {
      const int32_t v = datum.i32;
      std::memcpy(data_ptr, &v, sizeof(v));
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kInt64: {
      const int64_t v = datum.i64;
      std::memcpy(data_ptr, &v, sizeof(v));
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kUInt64: {
      const uint64_t v = datum.u64;
      std::memcpy(data_ptr, &v, sizeof(v));
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kFloat32: {
      const float v = datum.f32;
      std::memcpy(data_ptr, &v, sizeof(v));
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kFloat64: {
      const double v = datum.f64;
      std::memcpy(data_ptr, &v, sizeof(v));
      data_ptr += sizeof(v);
      break;
    }
    default:
      return Error::General("unsupported fixed datum encode");
    }
  }

  return {};
}

Result<void> EncodeKeyWithLayout(const RowEncodingLayout& layout, const lean_row* row,
                                 std::string& out_key) {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  const auto& cols = layout.Columns();
  if (row->num_columns < cols.size()) {
    return Error::General("row size does not match schema");
  }

  size_t key_bytes = 0;
  for (const auto pk_idx : layout.KeyColumns()) {
    const auto& col = cols[pk_idx];
    const auto& datum = row->columns[pk_idx];
    const bool is_null = row->nulls != nullptr ? row->nulls[pk_idx] : false;
    if (is_null) {
      return Error::General("primary key column cannot be null");
    }

    if (layout.IsVarlen()[pk_idx] != 0) {
      if (datum.str.size > std::numeric_limits<uint32_t>::max()) {
        return Error::General("primary key varlen too large");
      }
      key_bytes += sizeof(uint32_t) + datum.str.size;
      continue;
    }

    switch (col.type_) {
    case ColumnType::kBool:
      key_bytes += sizeof(uint8_t);
      break;
    case ColumnType::kInt32:
    case ColumnType::kFloat32:
      key_bytes += sizeof(uint32_t);
      break;
    case ColumnType::kInt64:
    case ColumnType::kUInt64:
    case ColumnType::kFloat64:
      key_bytes += sizeof(uint64_t);
      break;
    default:
      return Error::General("unsupported fixed datum encode");
    }
  }

  out_key.clear();
  out_key.reserve(key_bytes);

  for (const auto pk_idx : layout.KeyColumns()) {
    const auto& datum = row->columns[pk_idx];
    const bool is_null = row->nulls != nullptr ? row->nulls[pk_idx] : false;
    if (is_null) {
      return Error::General("primary key column cannot be null");
    }

    if (layout.IsVarlen()[pk_idx] != 0) {
      if (datum.str.size > std::numeric_limits<uint32_t>::max()) {
        return Error::General("primary key varlen too large");
      }
      AppendBigEndian(out_key, datum.str.size, 4);
      if (datum.str.size > 0 && datum.str.data != nullptr) {
        out_key.append(datum.str.data, datum.str.data + datum.str.size);
      }
      continue;
    }

    if (auto res = EncodeFixedDatum(datum, layout.Columns()[pk_idx].type_, out_key, true); !res) {
      return std::move(res.error());
    }
  }

  return {};
}

Result<void> DecodeValueWithLayout(const RowEncodingLayout& layout, Slice value,
                                   lean_row* out_row) {
  if (out_row == nullptr || out_row->columns == nullptr) {
    return Error::General("output row null");
  }
  if (out_row->nulls == nullptr) {
    return Error::General("output row null bitmap missing");
  }

  const auto& cols = layout.Columns();
  const uint32_t null_bitmap_bytes = static_cast<uint32_t>((cols.size() + 7) / 8);
  if (value.size() < null_bitmap_bytes) {
    return Error::General("value too small for schema");
  }
  if (out_row->num_columns < cols.size()) {
    return Error::General("output row too small");
  }

  const uint8_t* nulls = value.data();
  size_t data_section_len = 0;
  for (size_t i = 0; i < cols.size(); ++i) {
    const bool is_null = ((nulls[i / 8] >> (i % 8)) & 1u) != 0;
    if (is_null) {
      continue;
    }
    if (layout.ValueFieldWidths()[i] == 0) {
      return Error::General("unsupported type decode");
    }
    data_section_len += layout.ValueFieldWidths()[i];
  }

  if (null_bitmap_bytes + data_section_len > value.size()) {
    return Error::General("value too small for data section");
  }

  const uint8_t* data_ptr = value.data() + null_bitmap_bytes;
  const uint8_t* payload_ptr = data_ptr + data_section_len;
  const size_t payload_size = value.size() - null_bitmap_bytes - data_section_len;

  out_row->num_columns = static_cast<uint32_t>(cols.size());

  for (size_t i = 0; i < cols.size(); ++i) {
    auto* dest = &out_row->columns[i];
    const bool is_null = ((nulls[i / 8] >> (i % 8)) & 1u) != 0;
    out_row->nulls[i] = is_null;

    if (layout.IsVarlen()[i] != 0) {
      if (!is_null) {
        uint64_t encoded = 0;
        std::memcpy(&encoded, data_ptr, sizeof(encoded));
        data_ptr += sizeof(encoded);

        const uint32_t len = static_cast<uint32_t>(encoded >> 32);
        const uint32_t offset = static_cast<uint32_t>(encoded & 0xFFFFFFFFu);
        if (static_cast<size_t>(offset) + static_cast<size_t>(len) > payload_size) {
          return Error::General("varlen decode out of range");
        }
        dest->str.data = reinterpret_cast<const char*>(payload_ptr) + offset;
        dest->str.size = len;
      } else {
        dest->str.data = nullptr;
        dest->str.size = 0;
      }
      continue;
    }

    if (is_null) {
      continue;
    }

    switch (cols[i].type_) {
    case ColumnType::kBool:
      dest->b = (*data_ptr) != 0;
      data_ptr += sizeof(uint8_t);
      break;
    case ColumnType::kInt32: {
      int32_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->i32 = v;
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kInt64: {
      int64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->i64 = v;
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kUInt64: {
      uint64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->u64 = v;
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kFloat32: {
      float v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->f32 = v;
      data_ptr += sizeof(v);
      break;
    }
    case ColumnType::kFloat64: {
      double v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->f64 = v;
      data_ptr += sizeof(v);
      break;
    }
    default:
      return Error::General("unsupported fixed datum decode");
    }
  }

  return {};
}

Result<void> DecodeKeyDatumsWithLayout(const RowEncodingLayout& layout, Slice key,
                                       std::vector<Datum>& out_key_datums) {
  out_key_datums.clear();
  out_key_datums.resize(layout.KeyColumns().size());

  const auto& cols = layout.Columns();
  const uint8_t* ptr = key.data();
  size_t remaining = key.size();

  for (size_t i = 0; i < layout.KeyColumns().size(); ++i) {
    const uint32_t col_idx = layout.KeyColumns()[i];
    const ColumnType type = cols[col_idx].type_;
    auto* out = &out_key_datums[i];

    switch (type) {
    case ColumnType::kBinary:
    case ColumnType::kString: {
      if (remaining < sizeof(uint32_t)) {
        return Error::General("key too small for varlen length");
      }
      const uint32_t len = ReadBigEndian32(ptr);
      ptr += sizeof(uint32_t);
      remaining -= sizeof(uint32_t);
      if (remaining < len) {
        return Error::General("key too small for varlen payload");
      }
      out->str.data = reinterpret_cast<const char*>(ptr);
      out->str.size = len;
      ptr += len;
      remaining -= len;
      break;
    }
    case ColumnType::kBool:
      if (remaining < sizeof(uint8_t)) {
        return Error::General("key too small for bool");
      }
      out->b = (*ptr) != 0;
      ptr += sizeof(uint8_t);
      remaining -= sizeof(uint8_t);
      break;
    case ColumnType::kInt32: {
      if (remaining < sizeof(uint32_t)) {
        return Error::General("key too small for int32");
      }
      const uint32_t enc = ReadBigEndian32(ptr);
      ptr += sizeof(uint32_t);
      remaining -= sizeof(uint32_t);
      out->i32 = static_cast<int32_t>(enc - (1U << 31));
      break;
    }
    case ColumnType::kInt64: {
      if (remaining < sizeof(uint64_t)) {
        return Error::General("key too small for int64");
      }
      const uint64_t enc = ReadBigEndian64(ptr);
      ptr += sizeof(uint64_t);
      remaining -= sizeof(uint64_t);
      out->i64 = static_cast<int64_t>(enc - (1ULL << 63));
      break;
    }
    case ColumnType::kUInt64:
      if (remaining < sizeof(uint64_t)) {
        return Error::General("key too small for uint64");
      }
      out->u64 = ReadBigEndian64(ptr);
      ptr += sizeof(uint64_t);
      remaining -= sizeof(uint64_t);
      break;
    case ColumnType::kFloat32: {
      if (remaining < sizeof(uint32_t)) {
        return Error::General("key too small for float32");
      }
      const uint32_t enc = ReadBigEndian32(ptr);
      ptr += sizeof(uint32_t);
      remaining -= sizeof(uint32_t);
      const uint32_t raw = DecodeFloat32Key(enc);
      std::memcpy(&out->f32, &raw, sizeof(float));
      break;
    }
    case ColumnType::kFloat64: {
      if (remaining < sizeof(uint64_t)) {
        return Error::General("key too small for float64");
      }
      const uint64_t enc = ReadBigEndian64(ptr);
      ptr += sizeof(uint64_t);
      remaining -= sizeof(uint64_t);
      const uint64_t raw = DecodeFloat64Key(enc);
      std::memcpy(&out->f64, &raw, sizeof(double));
      break;
    }
    default:
      return Error::General("unsupported key decode type");
    }
  }

  return {};
}

Result<void> DecodeKeyWithLayout(const RowEncodingLayout& layout, Slice key, lean_row* out_row) {
  if (out_row == nullptr || out_row->columns == nullptr) {
    return Error::General("output row null");
  }
  if (out_row->nulls == nullptr) {
    return Error::General("output row null bitmap missing");
  }
  const auto& cols = layout.Columns();
  if (out_row->num_columns < cols.size()) {
    return Error::General("output row too small");
  }

  for (size_t i = 0; i < cols.size(); ++i) {
    out_row->nulls[i] = true;
  }
  out_row->num_columns = static_cast<uint32_t>(cols.size());

  std::vector<Datum> key_datums;
  if (auto res = DecodeKeyDatumsWithLayout(layout, key, key_datums); !res) {
    return std::move(res.error());
  }

  for (size_t i = 0; i < layout.KeyColumns().size(); ++i) {
    const uint32_t col_idx = layout.KeyColumns()[i];
    out_row->columns[col_idx] = key_datums[i];
    out_row->nulls[col_idx] = false;
  }
  return {};
}

TableCodec::TableCodec(const TableDefinition& def) {
  std::vector<CodecColumnDesc> columns;
  columns.reserve(def.schema_.Columns().size());
  for (const auto& col : def.schema_.Columns()) {
    columns.push_back({.type_ = col.type_, .nullable_ = col.nullable_});
  }

  std::vector<uint32_t> key_columns;
  key_columns.reserve(def.schema_.PrimaryKeyColumns().size());
  for (const auto key_col : def.schema_.PrimaryKeyColumns()) {
    key_columns.push_back(key_col);
  }

  layout_ = RowEncodingLayout(std::move(columns), std::move(key_columns));
}

Result<EncodedRow> TableCodec::Encode(const lean_row* row) const {
  EncodedRow encoded;
  if (auto res = EncodeKeyWithLayout(layout_, row, encoded.key_); !res) {
    return std::move(res.error());
  }
  if (auto res = EncodeValueWithLayout(layout_, row, encoded.value_); !res) {
    return std::move(res.error());
  }
  return encoded;
}

Result<std::string> TableCodec::EncodeValue(const lean_row* row) const {
  std::string out_value;
  if (auto res = EncodeValueWithLayout(layout_, row, out_value); !res) {
    return std::move(res.error());
  }
  return out_value;
}

Result<void> TableCodec::EncodeValue(const lean_row* row, std::string& out_value) const {
  return EncodeValueWithLayout(layout_, row, out_value);
}

Result<std::string> TableCodec::EncodeKey(const lean_row* row) const {
  std::string out_key;
  if (auto res = EncodeKeyWithLayout(layout_, row, out_key); !res) {
    return std::move(res.error());
  }
  return out_key;
}

Result<void> TableCodec::EncodeKey(const lean_row* row, std::string& out_key) const {
  return EncodeKeyWithLayout(layout_, row, out_key);
}

Result<void> TableCodec::DecodeValue(Slice value, lean_row* out_row) const {
  return DecodeValueWithLayout(layout_, value, out_row);
}

Result<void> TableCodec::DecodeKey(Slice key, lean_row* out_row) const {
  return DecodeKeyWithLayout(layout_, key, out_row);
}

} // namespace leanstore
