add_executable(insert_update_bench insert_update_bench.cpp)

target_link_libraries(insert_update_bench benchmark::benchmark benchmark::benchmark_main GTest::gtest leanstore atomic)
