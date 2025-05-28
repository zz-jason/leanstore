add_executable(PrimitiveBench primitive_bench.cpp)
add_executable(InsertUpdateBench insert_update_bench.cpp)

target_link_libraries(PrimitiveBench benchmark::benchmark benchmark::benchmark_main leanstore atomic)
target_link_libraries(InsertUpdateBench benchmark::benchmark benchmark::benchmark_main GTest::gtest leanstore atomic)
