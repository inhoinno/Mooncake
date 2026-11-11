cmake_minimum_required(VERSION 3.20)
project(callosum_store_adapter_tests LANGUAGES CXX)
enable_testing()
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)
# Compile the production bridge with deterministic engine/Store test doubles.
# This is T0 adapter evidence, not a Store binding or RDMA build.
configure_file(../../include/callosum_store.h fixtures/callosum_store.h COPYONLY)
add_executable(callosum_store_adapter_test
  test.cpp ../../src/callosum_store.cpp ../../src/pool_relay.cpp
  ../../../mooncake-common/src/cxl_pool_backend.cpp
  ../../../mooncake-common/src/crc_checksum.cpp)
target_compile_features(callosum_store_adapter_test PRIVATE cxx_std_20)
target_include_directories(callosum_store_adapter_test BEFORE PRIVATE
  "${CMAKE_CURRENT_BINARY_DIR}/fixtures" fixtures ../../include
  ../../../mooncake-common/include
  ../../../extern/yalantinglibs/include
  ../../../extern/yalantinglibs/include/ylt/thirdparty
  ../../../extern/yalantinglibs/include/ylt/standalone)
target_link_libraries(callosum_store_adapter_test PRIVATE Threads::Threads)
foreach(category preflight function request_lifecycle store_mux_integration native_mux_function native_mux_fifo native_mux_progress_cancel queued_target_revalidation failure status)
  add_test(NAME callosum_store_${category}
    COMMAND callosum_store_adapter_test ${category})
  set_tests_properties(callosum_store_${category} PROPERTIES
    LABELS "callosum;store;adapter;t0;${category}" TIMEOUT 30)
endforeach()
add_test(NAME callosum_store_probe_contract
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../../scripts/test_callosum_store_probe.py")
set_tests_properties(callosum_store_probe_contract PROPERTIES
  LABELS "callosum;store;adapter;t0;integration-harness" TIMEOUT 30)
