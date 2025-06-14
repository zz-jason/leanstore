add_executable(PrimitiveBench primitive_bench.cpp)
add_executable(InsertUpdateBench insert_update_bench.cpp)

set(TARGET_NAME PrimitiveBench)
target_compile_options(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_OPTIONS})
target_compile_definitions(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_DEFINITIONS})
target_compile_features(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_FEATURES})

set(TARGET_NAME InsertUpdateBench)
target_compile_options(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_OPTIONS})
target_compile_definitions(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_DEFINITIONS})
target_compile_features(${TARGET_NAME} PRIVATE ${LEANSTORE_COMPILE_FEATURES})

find_package(GTest CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)

target_link_libraries(PrimitiveBench benchmark::benchmark benchmark::benchmark_main leanstore atomic)
target_link_libraries(InsertUpdateBench benchmark::benchmark benchmark::benchmark_main GTest::gtest leanstore atomic)
