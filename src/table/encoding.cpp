#include "leanstore/table/encoding.hpp"

#include "leanstore/base/error.hpp"

#include <cstring>
#include <limits>
#include <utility>

namespace leanstore {

namespace {

void AppendBigEndian(std::string& buf, uint64_t v, uint32_t bytes) {
  for (uint32_t i = 0; i < bytes; ++i) {
    uint32_t shift = (bytes - 1 - i) * 8;
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
  if (u & 0x80000000) {
    u = ~u;
  } else {
    u |= 0x80000000;
  }
  return u;
}

uint64_t EncodeFloat64(double d) {
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
  switch (type) {
  case ColumnType::kBool: {
    uint8_t v = datum.b ? 1 : 0;
    dest.push_back(static_cast<char>(v));
    break;
  }
  case ColumnType::kInt32: {
    if (key_encoding) {
      auto enc = EncodeSigned(static_cast<int32_t>(datum.i32), 32);
      AppendBigEndian(dest, enc, 4);
    } else {
      int32_t v = datum.i32;
      dest.append(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    break;
  }
  case ColumnType::kInt64: {
    auto enc = EncodeSigned(static_cast<int64_t>(datum.i64), 64);
    if (key_encoding) {
      AppendBigEndian(dest, enc, 8);
    } else {
      int64_t raw = datum.i64;
      dest.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
    }
    break;
  }
  case ColumnType::kUInt64: {
    auto enc = datum.u64;
    if (key_encoding) {
      AppendBigEndian(dest, enc, 8);
    } else {
      dest.append(reinterpret_cast<const char*>(&enc), sizeof(enc));
    }
    break;
  }
  case ColumnType::kFloat32: {
    float f = datum.f32;
    if (key_encoding) {
      auto enc = EncodeFloat32(f);
      AppendBigEndian(dest, enc, 4);
    } else {
      dest.append(reinterpret_cast<const char*>(&f), sizeof(f));
    }
    break;
  }
  case ColumnType::kFloat64: {
    double f = datum.f64;
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
  if (type != ColumnType::kBinary && type != ColumnType::kString) {
    return Error::General("unexpected varlen type");
  }
  if (datum.str.size > std::numeric_limits<uint32_t>::max()) {
    return Error::General("varlen too large to encode");
  }
  uint32_t offset = static_cast<uint32_t>(payload.size());
  uint32_t len = static_cast<uint32_t>(datum.str.size);
  encoded = (static_cast<uint64_t>(len) << 32) | offset;
  if (len > 0 && datum.str.data != nullptr) {
    payload.append(datum.str.data, datum.str.data + len);
  }
  return {};
}

Result<EncodedRow> TableCodec::Encode(const lean_row* row) const {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  const auto& cols = def_.schema_.Columns();
  if (row->num_columns < cols.size()) {
    return Error::General("row size does not match schema");
  }

  EncodedRow encoded;
  auto key_res = EncodeKey(row);
  if (!key_res) {
    return std::move(key_res.error());
  }
  encoded.key_ = std::move(key_res.value());
  const uint32_t null_bitmap_bytes = (cols.size() + 7) / 8;
  encoded.value_.assign(null_bitmap_bytes, 0);

  std::string value_bytes;
  value_bytes.reserve(cols.size() * sizeof(uint64_t)); // rough estimate
  std::string payload;

  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& col = cols[i];
    const auto& datum = row->columns[i];
    bool is_null = row->nulls != nullptr ? row->nulls[i] : false;
    if (is_null) {
      if (!col.nullable_) {
        return Error::General(std::string("column cannot be null: ") + col.name_);
      }
      encoded.value_[i / 8] |= (1u << (i % 8));
      continue;
    }

    if (col.type_ == ColumnType::kBinary || col.type_ == ColumnType::kString) {
      uint64_t encoded_slot = 0;
      if (auto res = EncodeVarlenDatum(datum, col.type_, payload, encoded_slot); !res) {
        return std::move(res.error());
      }
      value_bytes.append(reinterpret_cast<const char*>(&encoded_slot), sizeof(encoded_slot));
      continue;
    }

    // fixed-size, append only the necessary bytes
    switch (col.type_) {
    case ColumnType::kBool: {
      uint8_t v = datum.b ? 1 : 0;
      value_bytes.push_back(static_cast<char>(v));
      break;
    }
    case ColumnType::kInt32: {
      int32_t v = datum.i32;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kInt64: {
      int64_t v = datum.i64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kUInt64: {
      uint64_t v = datum.u64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kFloat32: {
      float v = datum.f32;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    case ColumnType::kFloat64: {
      double v = datum.f64;
      value_bytes.append(reinterpret_cast<const char*>(&v), sizeof(v));
      break;
    }
    default:
      return Error::General("unsupported fixed datum encode");
    }
  }

  encoded.value_.append(value_bytes);
  if (!payload.empty()) {
    encoded.value_.append(payload);
  }
  return encoded;
}

Result<std::string> TableCodec::EncodeKey(const lean_row* row) const {
  if (row == nullptr || row->columns == nullptr) {
    return Error::General("row is null");
  }
  if (row->num_columns < def_.schema_.Columns().size()) {
    return Error::General("row size does not match schema");
  }
  std::string key_buf;
  for (auto pk_idx : def_.schema_.PrimaryKeyColumns()) {
    const auto& col = def_.schema_.Columns()[pk_idx];
    const auto& datum = row->columns[pk_idx];
    bool is_null = row->nulls != nullptr ? row->nulls[pk_idx] : false;
    if (is_null) {
      return Error::General("primary key column cannot be null");
    }
    if (col.type_ == ColumnType::kBinary || col.type_ == ColumnType::kString) {
      if (datum.str.size > std::numeric_limits<uint32_t>::max()) {
        return Error::General("primary key varlen too large");
      }
      AppendBigEndian(key_buf, datum.str.size, 4);
      if (datum.str.size > 0 && datum.str.data != nullptr) {
        key_buf.append(datum.str.data, datum.str.data + datum.str.size);
      }
    } else {
      if (auto res = EncodeFixedDatum(datum, col.type_, key_buf, true); !res) {
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
  if (out_row->nulls == nullptr) {
    return Error::General("output row null bitmap missing");
  }
  const auto& cols = def_.schema_.Columns();
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
    switch (cols[i].type_) {
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

  out_row->num_columns = cols.size();

  for (size_t i = 0; i < cols.size(); ++i) {
    auto* dest = &out_row->columns[i];
    const auto& col = cols[i];
    bool is_null = (nulls[i / 8] >> (i % 8)) & 1u;
    out_row->nulls[i] = is_null;

    if (col.type_ == ColumnType::kBinary || col.type_ == ColumnType::kString) {
      if (!is_null) {
        if (data_ptr + sizeof(uint64_t) > value.data() + value.size()) {
          return Error::General("varlen metadata out of range");
        }
        uint64_t encoded = 0;
        std::memcpy(&encoded, data_ptr, sizeof(uint64_t));
        data_ptr += sizeof(uint64_t);

        uint32_t len = static_cast<uint32_t>(encoded >> 32);
        uint32_t offset = static_cast<uint32_t>(encoded & 0xFFFFFFFFu);
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

    switch (col.type_) {
    case ColumnType::kBool: {
      if (data_ptr + sizeof(uint8_t) > value.data() + value.size()) {
        return Error::General("bool decode out of range");
      }
      dest->b = (*data_ptr) != 0;
      data_ptr += sizeof(uint8_t);
      break;
    }
    case ColumnType::kInt32: {
      if (data_ptr + sizeof(int32_t) > value.data() + value.size()) {
        return Error::General("int32 decode out of range");
      }
      int32_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->i32 = v;
      data_ptr += sizeof(int32_t);
      break;
    }
    case ColumnType::kInt64: {
      if (data_ptr + sizeof(int64_t) > value.data() + value.size()) {
        return Error::General("int64 decode out of range");
      }
      int64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->i64 = v;
      data_ptr += sizeof(int64_t);
      break;
    }
    case ColumnType::kUInt64: {
      if (data_ptr + sizeof(uint64_t) > value.data() + value.size()) {
        return Error::General("uint64 decode out of range");
      }
      uint64_t v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->u64 = v;
      data_ptr += sizeof(uint64_t);
      break;
    }
    case ColumnType::kFloat32: {
      if (data_ptr + sizeof(float) > value.data() + value.size()) {
        return Error::General("float32 decode out of range");
      }
      float v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->f32 = v;
      data_ptr += sizeof(float);
      break;
    }
    case ColumnType::kFloat64: {
      if (data_ptr + sizeof(double) > value.data() + value.size()) {
        return Error::General("float64 decode out of range");
      }
      double v = 0;
      std::memcpy(&v, data_ptr, sizeof(v));
      dest->f64 = v;
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
