#include <iostream>

template <typename T>
unique_generator<T> range(T fromInclusive, T toExclusive) {
  for (T v = fromInclusive; v < toExclusive; ++v) {
    co_yield v;
  }
}

int main() {
  for (auto val : range(1, 10)) {
    std::cout << val << std::endl;
  }
}