add_executable(PrimitiveBench PrimitiveBench.cpp)
add_executable(InsertUpdateBench InsertUpdateBench.cpp)

target_link_libraries(PrimitiveBench benchmark::benchmark benchmark::benchmark_main leanstore atomic)
target_link_libraries(InsertUpdateBench benchmark::benchmark benchmark::benchmark_main GTest::gtest leanstore atomic)
