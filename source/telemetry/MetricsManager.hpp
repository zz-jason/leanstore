#pragma once

#include "prometheus/family.h"
#include "prometheus/gauge.h"
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <memory>
#include <string>

namespace leanstore {

class MetricsManager {
public:
  static std::unique_ptr<MetricsManager> sInstance;

private:
  std::unique_ptr<prometheus::Exposer> mExposer;

  std::shared_ptr<prometheus::Registry> mRegistry;

  prometheus::Counter* mPageWrite = nullptr;

public:
  MetricsManager(const std::string& url = "") : mExposer(nullptr), mRegistry() {
    // create a http server on the target address to expose metrics
    if (!url.empty()) {
      mExposer = std::make_unique<prometheus::Exposer>(url);
    }

    // register predefined metrics
    registerBufferPoolCounters();
    registerBufferPoolGuages();
    registerBufferPoolHistograms();

    // ask the exposer to scrape the registry on incoming HTTP requests
    if (mExposer) {
      mExposer->RegisterCollectable(mRegistry);
    }
  }

  ~MetricsManager() {
  }

  prometheus::Counter& PageWrites() {
    return *mPageWrite;
  }

private:
  void registerBufferPoolCounters() {
    auto& pageWriteFamily = prometheus::BuildCounter()
                                .Name("page_writes")
                                .Help("Number of pages written")
                                .Register(*mRegistry);
    mPageWrite = &pageWriteFamily.Add({});
  }

  void registerBufferPoolGuages() {
  }

  void registerBufferPoolHistograms() {
  }
};

} // namespace leanstore