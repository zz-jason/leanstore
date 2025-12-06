#include "leanstore/table/encoding.hpp"

#include "leanstore/cpp/base/error.hpp"

#include <cstring>
#include <limits>
#include <utility>

namespace leanstore {

namespace {

inline lean_column_type ToLeanColumnType(ColumnType type) {
  switch (type) {
  case ColumnType::kBool:
    return LEAN_COLUMN_TYPE_BOOL;
  case ColumnType::kInt32:
    return LEAN_COLUMN_TYPE_INT32;
  case ColumnType::kInt64:
    return LEAN_COLUMN_TYPE_INT64;
  case ColumnType::kUInt64:
    return LEAN_COLUMN_TYPE_UINT64;
  case ColumnType::kFloat32:
    return LEAN_COLUMN_TYPE_FLOAT32;
  case ColumnType::kFloat64:
    return LEAN_COLUMN_TYPE_FLOAT64;
  case ColumnType::kString:
    return LEAN_COLUMN_TYPE_STRING;
  case ColumnType::kBinary:
  default:
    return LEAN_COLUMN_TYPE_BINARY;
  }
}

inline void AppendBigEndian(std::string& buf, uint64_t v, uint32_t bytes) {
  for (uint32_t i = 0; i < bytes; ++i) {
    uint32_t shift = (bytes - 1 - i) * 8;
    buf.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

inline uint64_t FromBigEndian(const uint8_t* data, uint32_t bytes) {
  uint64_t v = 0;
  for (uint32_t i = 0; i < bytes; ++i) {
    v = (v << 8) | data[i];
  }
  return v;
}

inline uint64_t EncodeSigned(int64_t v, uint32_t bits) {
  const uint64_t bias = 1ull << (bits - 1);
  return static_cast<uint64_t>(v) + bias;
}

inline uint32_t EncodeFloat32(float f) {
  uint32_t u = 0;
  std::memcpy(&u, &f, sizeof(u));
  if (u & 0x80000000) {
    u = ~u;
  } else {
    u |= 0x80000000;
  }
  return u;
}

inline uint64_t EncodeFloat64(double d) {
  uint64_t u;
  std::memcpy(&u, &d, sizeof(double));
  if (u & 0x8000000000000000ULL) {
    u = ~u;
  } else {
    u |= 0x8000000000000000ULL;
  }
  return u;
}

} // namespace

Result<void> TableCodec::EncodeFixedDatum(const Datum& datum, ColumnType type, std::string& dest,
                                          bool key_encoding) const {
  if (datum.is_null) {
    return Error::General("null value not allowed for fixed length encode");
  }
  if (datum.type != ToLeanColumnType(type)) {
    return Error::General("datum type mismatch with column definition");
  }
  switch (type) {
  case ColumnType::kBool: {
    uint8_t v = datum.value.b ? 1 : 0;
    dest.push_back(static_cast<char>(v));
    break;
  }
  case ColumnType::kInt32: {
    if (key_encoding) {
      auto enc = EncodeSigned(static_cast<int32_t>(datum.value.i32), 32);
      AppendBigEndian(dest, enc, 4);
    } else {
      int32_t v = datum.value.i32;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kInt64: {
    auto enc = EncodeSigned(static_cast<int64_t>(datum.value.i64), 64);
    if (key_encoding) {
      AppendBigEndian(dest, enc, 8);
    } else {
      int64_t raw = datum.value.i64;
      dest.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
    }
    break;
  }
  case ColumnType::kUInt64: {
    auto enc = datum.value.u64;
    if (key_encoding) {
      AppendBigEndian(dest, enc, 8);
    } else {
      dest.append(reinterpret_cast<const char*>(&enc), sizeof(enc));
    }
    break;
  }
  case ColumnType::kFloat32: {
    float f = datum.value.f32;
    if (key_encoding) {
      auto enc = EncodeFloat32(f);
      AppendBigEndian(dest, enc, 4);
    } else {
      dest.append(reinterpret_cast<const char*>(&f), sizeof(f));
    }
    break;
  }
  case ColumnType::kFloat64: {
    double f = datum.value.f64;
    if (key_encoding) {
      auto enc = EncodeFloat64(f);
      AppendBigEndian(dest, enc, 8);
    } else {
      dest.append(reinterpret_cast<const char*>(&f), sizeof(f));
    }
    break;
  }
  default:
    return Error::General("unsupported fixed datum encode");
  }
  return {};
}

Result<void> TableCodec::EncodeVarlenDatum(const Datum& datum, ColumnType type,
                                           std::string& payload, uint64_t& encoded) const {
  if (datum.is_null) {
    encoded = std::numeric_limits<uint64_t>::max();
    return {};
  }
  if (type != ColumnType::kBinary && type != ColumnType::kString) {
    return Error::General("unexpected varlen type");
  }
  if (datum.type != ToLeanColumnType(type)) {
    return Error::General("datum type mismatch with column definition");
  }
  if (datum.value.str.size > std::numeric_limits<uint32_t>::max()) {
    return Error::General("varlen too large to encode");
  }
  uint32_t offset = static_cast<uint32_t>(payload.size());
  uint32_t len = static_cast<uint32_t>(datum.value.str.size);
  encoded = (static_cast<uint64_t>(len) << 32) | offset;
  if (len > 0 && datum.value.str.data != nullptr) {
    payload.append(datum.value.str.data, datum.value.str.data + len);
  }
  return {};
}

Result<EncodedRow> TableCodec::Encode(const lean_row* row) const {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  if (row->num_columns < def_.schema.columns().size()) {
    return Error::General("row size does not match schema");
  }

  EncodedRow encoded;
  auto key_res = EncodeKey(row);
  if (!key_res) {
    return std::move(key_res.error());
  }
  encoded.key = std::move(key_res.value());
  encoded.value.clear();

  const auto& cols = def_.schema.columns();
  const uint32_t null_bitmap_bytes = (cols.size() + 7) / 8;
  encoded.value.resize(null_bitmap_bytes, 0);

  std::string value_bytes;
  value_bytes.reserve(cols.size() * sizeof(uint64_t)); // rough estimate
  std::string payload;

  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& col = cols[i];
    const auto& datum = row->columns[i];
    if (datum.is_null && !col.nullable) {
      return Error::General(std::string("column cannot be null: ") + col.name);
    }
    if (datum.is_null) {
      encoded.value[i / 8] |= (1u << (i % 8));
      continue;
    }

    if (col.type != ColumnType::kBinary && col.type != ColumnType::kString &&
        datum.type != ToLeanColumnType(col.type)) {
      return Error::General("datum type mismatch with column definition");
    }

    if (col.type == ColumnType::kBinary || col.type == ColumnType::kString) {
      uint64_t encoded_slot = 0;
      if (auto res = EncodeVarlenDatum(datum, col.type, payload, encoded_slot); !res) {
        return std::move(res.error());
      }
      value_bytes.append(reinterpret_cast<const char*>(&encoded_slot), sizeof(encoded_slot));
      continue;
    }

    // fixed-size, append only the necessary bytes
    switch (col.type) {
    case ColumnType::kBool: {
      uint8_t v = datum.value.b ? 1 : 0;
      value_bytes.push_back(static_cast<char>(v));
      break;
    }
    case ColumnType::kInt32: {
      int32_t v = datum.value.i32;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kInt64: {
      int64_t v = datum.value.i64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kUInt64: {
      uint64_t v = datum.value.u64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kFloat32: {
      float v = datum.value.f32;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kFloat64: {
      double v = datum.value.f64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    default:
      return Error::General("unsupported fixed datum encode");
    }
  }

  encoded.value.append(value_bytes);
  if (!payload.empty()) {
    encoded.value.append(payload);
  }
  return encoded;
}

Result<std::string> TableCodec::EncodeKey(const lean_row* row) const {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  if (row->num_columns < def_.schema.columns().size()) {
    return Error::General("row size does not match schema");
  }
  std::string key_buf;
  for (auto pk_idx : def_.schema.primary_key_columns()) {
    const auto& col = def_.schema.columns()[pk_idx];
    const auto& datum = row->columns[pk_idx];
    if (datum.is_null) {
      return Error::General("primary key column cannot be null");
    }
    if (col.type == ColumnType::kBinary || col.type == ColumnType::kString) {
      if (datum.type != ToLeanColumnType(col.type)) {
        return Error::General("datum type mismatch with column definition");
      }
      if (datum.value.str.size > std::numeric_limits<uint32_t>::max()) {
        return Error::General("primary key varlen too large");
      }
      AppendBigEndian(key_buf, datum.value.str.size, 4);
      if (datum.value.str.size > 0 && datum.value.str.data != nullptr) {
        key_buf.append(datum.value.str.data, datum.value.str.data + datum.value.str.size);
      }
    } else {
      if (auto res = EncodeFixedDatum(datum, col.type, key_buf, true); !res) {
        return std::move(res.error());
      }
    }
  }
  return key_buf;
}

Result<void> TableCodec::DecodeValue(Slice value, lean_row* out_row) const {
  if (out_row == nullptr || out_row->columns == nullptr) {
    return Error::General("output row null");
  }
  const auto& cols = def_.schema.columns();
  uint32_t null_bitmap_bytes = (cols.size() + 7) / 8;
  if (value.size() < null_bitmap_bytes) {
    return Error::General("value too small for schema");
  }
  if (out_row->num_columns < cols.size()) {
    return Error::General("output row too small");
  }
  const uint8_t* nulls = value.data();

  // First pass to compute where the payload starts
  size_t data_section_len = 0;
  for (size_t i = 0; i < cols.size(); ++i) {
    bool is_null = (nulls[i / 8] >> (i % 8)) & 1u;
    if (is_null) {
      continue;
    }
    switch (cols[i].type) {
    case ColumnType::kBinary:
    case ColumnType::kString:
      data_section_len += sizeof(uint64_t);
      break;
    case ColumnType::kBool:
      data_section_len += sizeof(uint8_t);
      break;
    case ColumnType::kInt32:
    case ColumnType::kFloat32:
      data_section_len += sizeof(uint32_t);
      break;
    case ColumnType::kInt64:
    case ColumnType::kUInt64:
    case ColumnType::kFloat64:
      data_section_len += sizeof(uint64_t);
      break;
    default:
      return Error::General("unsupported type decode");
    }
  }

  if (null_bitmap_bytes + data_section_len > value.size()) {
    return Error::General("value too small for data section");
  }

  const uint8_t* data_ptr = value.data() + null_bitmap_bytes;
  const uint8_t* payload_ptr = data_ptr + data_section_len;
  size_t payload_size = value.size() - null_bitmap_bytes - data_section_len;

  lean_row_deinit(out_row);
  out_row->num_columns = cols.size();

  for (size_t i = 0; i < cols.size(); ++i) {
    auto* dest = &out_row->columns[i];
    const auto& col = cols[i];
    bool is_null = (nulls[i / 8] >> (i % 8)) & 1u;
    dest->type = ToLeanColumnType(col.type);
    dest->is_null = is_null;

    if (col.type == ColumnType::kBinary || col.type == ColumnType::kString) {
      if (!is_null) {
        if (data_ptr + sizeof(uint64_t) > value.data() + value.size()) {
          return Error::General("varlen metadata out of range");
        }
        uint64_t encoded = 0;
        std::memcpy(&encoded, data_ptr, sizeof(uint64_t));
        data_ptr += sizeof(uint64_t);

        uint32_t len = static_cast<uint32_t>(encoded >> 32);
        uint32_t offset = static_cast<uint32_t>(encoded & 0xFFFFFFFFu);
        if (offset + len > payload_size) {
          return Error::General("varlen decode out of range");
        }
        lean_str_assign(&dest->value.str, reinterpret_cast<const char*>(payload_ptr) + offset, len);
      } else {
        dest->value.str.data = nullptr;
        dest->value.str.size = 0;
        dest->value.str.capacity = 0;
      }
      continue;
    }

    if (is_null) {
      continue;
    }

    switch (col.type) {
    case ColumnType::kBool: {
      if (data_ptr + sizeof(uint8_t) > value.data() + value.size()) {
        return Error::General("bool decode out of range");
      }
      dest->value.b = (*data_ptr) != 0;
      data_ptr += sizeof(uint8_t);
      break;
    }
    case ColumnType::kInt32: {
      if (data_ptr + sizeof(int32_t) > value.data() + value.size()) {
        return Error::General("int32 decode out of range");
      }
      int32_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->value.i32 = v;
      data_ptr += sizeof(int32_t);
      break;
    }
    case ColumnType::kInt64: {
      if (data_ptr + sizeof(int64_t) > value.data() + value.size()) {
        return Error::General("int64 decode out of range");
      }
      int64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->value.i64 = v;
      data_ptr += sizeof(int64_t);
      break;
    }
    case ColumnType::kUInt64: {
      if (data_ptr + sizeof(uint64_t) > value.data() + value.size()) {
        return Error::General("uint64 decode out of range");
      }
      uint64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->value.u64 = v;
      data_ptr += sizeof(uint64_t);
      break;
    }
    case ColumnType::kFloat32: {
      if (data_ptr + sizeof(float) > value.data() + value.size()) {
        return Error::General("float32 decode out of range");
      }
      float v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->value.f32 = v;
      data_ptr += sizeof(float);
      break;
    }
    case ColumnType::kFloat64: {
      if (data_ptr + sizeof(double) > value.data() + value.size()) {
        return Error::General("float64 decode out of range");
      }
      double v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->value.f64 = v;
      data_ptr += sizeof(double);
      break;
    }
    default:
      return Error::General("unsupported fixed datum decode");
    }
  }
  return {};
}

} // namespace leanstore
