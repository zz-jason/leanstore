add_executable(CastBench CastBench.cpp)
add_executable(InsertUpdateBench InsertUpdateBench.cpp)

target_link_libraries(CastBench gbench leanstore)
target_link_libraries(InsertUpdateBench gbench gtest leanstore)
