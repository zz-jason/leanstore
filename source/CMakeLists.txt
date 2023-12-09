# ------------------------------------------------------------------------------
# Files for the lib
# ------------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_SRC **.cpp **/**.cpp **.cc **/**.cc **.hpp **/**.hpp **.h **/**.h)


# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

add_library(leanstore STATIC ${LEANSTORE_SRC})

# dependencies
target_link_libraries(leanstore gflags glog profiler Threads::Threads aio tbb atomic tabluate rapidjson ${Boost_LIBRARIES} ) #tbb

# include dirs
target_include_directories(leanstore PUBLIC ${CMAKE_SOURCE_DIR}/include/shared-headers)
# target_include_directories(leanstore PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})

# export header files for lib consumers
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})

# Get the include directories for the target.
get_target_property(LIBA_INCLUDES leanstore INCLUDE_DIRECTORIES)
foreach(dir ${LIBA_INCLUDES})
    message(STATUS "include dir: ${dir}")
endforeach()


# ---------------------------------------------------------------------------
# Set compile options for the lib
# ---------------------------------------------------------------------------

option(SANI "Compile leanstore with sanitizers" OFF)
if(SANI)
  target_compile_options(leanstore PUBLIC -fsanitize=address)
  target_link_libraries(leanstore asan)
endif(SANI)

option(PARANOID "Enable sanity checks in release mode" OFF)
if(PARANOID)
  target_compile_definitions(leanstore PUBLIC PARANOID)
endif(PARANOID)

set(COUNTERS_LEVEL "all" CACHE STRING "Which counters to leave in leanstore build")
if(COUNTERS_LEVEL STREQUAL "all")
  target_compile_definitions(leanstore PUBLIC MACRO_COUNTERS_ALL)
endif()

set(CHECKS_LEVEL "default" CACHE STRING "Which checks to leave in leanstore build")
if(CHECKS_LEVEL STREQUAL "default")
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
  elseif(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
  endif()
elseif(CHECKS_LEVEL STREQUAL "debug")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
elseif(CHECKS_LEVEL STREQUAL "release")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
elseif(CHECKS_LEVEL STREQUAL "benchmark")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_BENCHMARK)
endif()


# ---------------------------------------------------------------------------
# clang-tidy
# ---------------------------------------------------------------------------

# clang-tidy
# add_custom_target(run-clang-tidy
#     COMMAND clang-tidy -p=${CMAKE_BINARY_DIR} --config-file=${CMAKE_SOURCE_DIR}/.clang-tidy ${LEANSTORE_SRC}
#     COMMENT "Running Clang-Tidy"
#     VERBATIM
# )
#
# add_dependencies(leanstore run-clang-tidy)
