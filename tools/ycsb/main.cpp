#include "tools/ycsb/ycsb_executor.hpp"
#include "tools/ycsb/ycsb_options.hpp"

int main(int argc, char** argv) {
  auto ycsb_options = leanstore::ycsb::YcsbOptions::FromCmdLine(argc, argv);
  leanstore::ycsb::YcsbExecutor cmd_handler(ycsb_options);
  cmd_handler.Execute();
  return 0;
}
