#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace leanstore {

struct WalAnalysisOptions {
  std::string wal_dir_;
};

class WalAnalysis {
public:
  static WalAnalysis New(int argc, char** argv);

  ~WalAnalysis() = default;

  void Run();

private:
  explicit WalAnalysis(const WalAnalysisOptions&& options) : options_(std::move(options)) {
  }

  static std::vector<std::string> ListWalFiles(std::string_view wal_dir);

  const WalAnalysisOptions options_;
};

} // namespace leanstore