add_executable(primitive_bench primitive_bench.cpp)
add_executable(insert_update_bench insert_update_bench.cpp)

target_link_libraries(primitive_bench benchmark::benchmark benchmark::benchmark_main leanstore atomic)
target_link_libraries(insert_update_bench benchmark::benchmark benchmark::benchmark_main GTest::gtest leanstore atomic)
