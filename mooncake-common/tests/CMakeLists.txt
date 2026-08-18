add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC mooncake_common gtest pthread)
add_test(NAME default_config_test COMMAND default_config_test)

add_executable(environ_test environ_test.cpp)
target_link_libraries(environ_test PUBLIC mooncake_common gtest pthread)
add_test(NAME environ_test COMMAND environ_test)

add_executable(crc_checksum_test crc_checksum_test.cpp)
target_link_libraries(crc_checksum_test PUBLIC mooncake_common gtest gtest_main
                                               pthread)
add_test(NAME crc_checksum_test COMMAND crc_checksum_test)

add_executable(rpc_client_io_threads_test rpc_client_io_threads_test.cpp)
target_link_libraries(rpc_client_io_threads_test PUBLIC mooncake_common gtest
                                                        pthread)
add_test(NAME rpc_client_io_threads_test COMMAND rpc_client_io_threads_test)

add_executable(rpc_client_io_context_test rpc_client_io_context_test.cpp)
target_link_libraries(rpc_client_io_context_test PUBLIC mooncake_common gtest
                                                        ibverbs pthread)
add_test(NAME rpc_client_io_context_test COMMAND rpc_client_io_context_test)

foreach(parser_test ascii_string bool_parser integer_parser)
  add_executable(${parser_test}_test ${parser_test}_test.cpp)
  target_link_libraries(${parser_test}_test PUBLIC mooncake_common gtest
                                                   gtest_main pthread)
  add_test(NAME ${parser_test}_test COMMAND ${parser_test}_test)
endforeach()

# TODO 1 CXL pool backend gates are separate executables so preflight,
# functionality, failure cleanup, and status remain independently reportable.
foreach(cxl_backend_test preflight functional failure status)
  set(target_name cxl_pool_backend_${cxl_backend_test}_test)
  add_executable(
    ${target_name}
    cxl_pool_backend/${target_name}.cpp)
  target_include_directories(
    ${target_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cxl_pool_backend)
  target_link_libraries(${target_name} PUBLIC mooncake_common pthread)
  add_test(NAME ${target_name} COMMAND ${target_name})
  set_tests_properties(${target_name} PROPERTIES LABELS "cxl;todo1;t0")
endforeach()

add_executable(
  cxl_pool_backend_devdax_test
  cxl_pool_backend/cxl_pool_backend_devdax_test.cpp)
target_include_directories(
  cxl_pool_backend_devdax_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cxl_pool_backend)
target_link_libraries(cxl_pool_backend_devdax_test PUBLIC mooncake_common
                                                           pthread)
add_test(NAME cxl_pool_backend_devdax_test
         COMMAND cxl_pool_backend_devdax_test)
set_tests_properties(cxl_pool_backend_devdax_test
                     PROPERTIES LABELS "cxl;todo1;t2;hardware")

add_executable(
  cxl_native_backend_preflight_test
  cxl_pool_backend/cxl_native_backend_preflight_test.cpp)
target_include_directories(
  cxl_native_backend_preflight_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cxl_pool_backend)
target_link_libraries(cxl_native_backend_preflight_test PUBLIC mooncake_common
                                                        pthread)
add_test(NAME todo2_native_cxl_preflight_test
         COMMAND cxl_native_backend_preflight_test)
set_tests_properties(todo2_native_cxl_preflight_test
                     PROPERTIES LABELS "cxl;todo2;t0;preflight")
