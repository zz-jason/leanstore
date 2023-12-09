#pragma once

#include "BufferFrame.hpp"
#include "Config.hpp"
#include "FreeList.hpp"
#include "Units.hpp"

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace leanstore {
namespace storage {

class Tracing {
public:
  static std::mutex mutex;
  static std::unordered_map<PID, std::tuple<TREEID, u64>> ht;
  static void printStatus(PID pageId) {
    mutex.lock();
    if (ht.contains(pageId)) {
      cout << pageId << " was written out: " << std::get<1>(ht[pageId])
           << " times form DT: " << std::get<0>(ht[pageId]) << endl;
    } else {
      cout << pageId << " was never written out" << endl;
    }
    mutex.unlock();
  }
};

} // namespace storage
} // namespace leanstore
