#pragma once

#include <cstdint>

namespace leanstore::ycsb {

enum class YcsbWorkloadType : uint8_t {
  kA = 0,
  kB,
  kC,
  kD,
  kE,
  kF,
};

struct YcsbWorkloadSpec {
  double read_proportion_;
  double update_proportion_;
  double scan_proportion_;
  double insert_proportion_;
};

inline constexpr YcsbWorkloadSpec kYcsbWorkloadSpecs[] = {
    // YcsbWorkloadType A
    {
        .read_proportion_ = 0.5,
        .update_proportion_ = 0.5,
        .scan_proportion_ = 0.0,
        .insert_proportion_ = 0.0,
    },

    // YcsbWorkloadType B
    {
        .read_proportion_ = 0.95,
        .update_proportion_ = 0.05,
        .scan_proportion_ = 0.0,
        .insert_proportion_ = 0.0,
    },

    // YcsbWorkloadType C
    {
        .read_proportion_ = 1.0,
        .update_proportion_ = 0.0,
        .scan_proportion_ = 0.0,
        .insert_proportion_ = 0.0,
    },

    // YcsbWorkloadType D
    {
        .read_proportion_ = 0.95,
        .update_proportion_ = 0.0,
        .scan_proportion_ = 0.0,
        .insert_proportion_ = 0.05,
    },

    // YcsbWorkloadType E
    {
        .read_proportion_ = 0.0,
        .update_proportion_ = 0.0,
        .scan_proportion_ = 0.95,
        .insert_proportion_ = 0.05,
    },

    // YcsbWorkloadType F
    {
        .read_proportion_ = 0.5,
        .update_proportion_ = 0.0,
        .scan_proportion_ = 0.0,
        .insert_proportion_ = 0.5,
    },
};

} // namespace leanstore::ycsb