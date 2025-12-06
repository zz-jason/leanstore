#include "wal_analysis.hpp"

#include "leanstore/cpp/recovery/recovery_analyzer.hpp"
#include "leanstore/cpp/recovery/recovery_context.hpp"

#include <tanakh-cmdline/cmdline.h>

#include <filesystem>
#include <format>
#include <iostream>

int main(int argc, char** argv) {
  leanstore::WalAnalysis::New(argc, argv).Run();
  return 0;
}

namespace leanstore {

WalAnalysis WalAnalysis::New(int argc, char** argv) {
  constexpr auto kArgWalDir = "wal_dir";
  cmdline::parser args;
  args.add<std::string>(kArgWalDir, 0, "Directory containing WAL files", true, "");
  args.parse_check(argc, argv);

  return WalAnalysis(leanstore::WalAnalysisOptions{
      .wal_dir_ = args.get<std::string>(kArgWalDir),
  });
}

void WalAnalysis::Run() {
  std::cout << std::format("Starting WAL analysis on directory: {}\n", options_.wal_dir_);

  auto wal_files = ListWalFiles(options_.wal_dir_);
  std::cout << std::format("Found {} WAL files\n", wal_files.size());
  for (const auto& wal_file : wal_files) {
    std::cout << std::format(" - {}\n", wal_file);
  }

  RecoveryContext recovery_ctx{0, std::move(wal_files)};
  auto recovery_analyzer = RecoveryAnalyzer(recovery_ctx);
  if (auto err = recovery_analyzer.Run(); err) {
    std::cerr << std::format("WAL analysis failed: {}\n", err->ToString());
    return;
  }

  auto& dpt = recovery_analyzer.GetDirtyPageTable();
  std::cout << std::format("Dirty Page Table (DPT) size: {}\n", dpt.size());
  for (const auto& [page_id, lid] : dpt) {
    std::cout << std::format(" - Page ID: {}, First Dirty GSN: {}\n", page_id, lid);
  }

  auto& att = recovery_analyzer.GetActiveTxTable();
  std::cout << std::format("Active Transaction Table (ATT) size: {}\n", att.size());
  for (const auto& [tx_id, lid] : att) {
    std::cout << std::format(" - Transaction ID: {}, Last LSN: {}\n", tx_id, lid);
  }
};

std::vector<std::string> WalAnalysis::ListWalFiles(std::string_view wal_dir) {
  std::vector<std::string> wal_files;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file()) {
      wal_files.push_back(entry.path().string());
    }
  }
  return wal_files;
}

} // namespace leanstore