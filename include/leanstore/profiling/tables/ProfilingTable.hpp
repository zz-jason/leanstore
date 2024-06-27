#pragma once

#include "leanstore/Exceptions.hpp"
#include "leanstore/Units.hpp"

#include <functional>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace leanstore {
namespace profiling {

struct Column {
  std::function<void(Column& col)> generator;

  std::vector<std::string> values;

  Column(std::function<void(Column& col)>&& g) : generator(g) {
  }

  std::string to_string(int8_t x) {
    return std::to_string(x);
  }
  std::string to_string(int16_t x) {
    return std::to_string(x);
  }
  std::string to_string(int32_t x) {
    return std::to_string(x);
  }
  std::string to_string(int64_t x) {
    return std::to_string(x);
  }
  std::string to_string(uint8_t x) {
    return std::to_string(x);
  }
  std::string to_string(uint16_t x) {
    return std::to_string(x);
  }
  std::string to_string(uint32_t x) {
    return std::to_string(x);
  }
  std::string to_string(uint64_t x) {
    return std::to_string(x);
  }
  std::string to_string(double x) {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(1) << x;
    return stream.str();
  }
  std::string to_string(float x) {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(1) << x;
    return stream.str();
  }
  std::string to_string(std::string x) {
    return x;
  }

  template <typename T>
  Column& operator<<(T x) {
    values.push_back(to_string(x));
    return *this;
  }
};

using ColumnGenerator = std::function<void(Column& col)>;
using ColumnValues = std::vector<std::string>;

class ProfilingTable {
protected:
  std::unordered_map<std::string, Column> columns;

public:
  // Open -> getColumns() -> next -> getColumns()
  void clear() {
    for (auto& column : columns) {
      column.second.values.clear();
    }
  }
  uint64_t size() {
    return columns.begin()->second.values.size();
  }
  std::string get(std::string key, std::string column) {
    auto& c = columns.at("key");
    for (uint64_t r_i = 0; r_i < size(); r_i++) {
      if (c.values[r_i] == key) {
        return columns.at(column).values[r_i];
      }
    }
    ENSURE(false);
  }
  double getDouble(std::string key, std::string column) {
    return std::stod(get(key, column));
  }
  virtual std::string getName() {
    return "null";
  }
  virtual void open() {
  }
  virtual void next() {
  }
  virtual std::unordered_map<std::string, Column>& getColumns() {
    return columns;
  }
  Column& operator[](std::string name) {
    return columns.at(name);
  }
};

} // namespace profiling
} // namespace leanstore
