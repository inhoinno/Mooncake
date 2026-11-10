cmake_minimum_required(VERSION 3.16)
project(callosum_relay_native LANGUAGES CXX)
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
set(PYBIND11_FINDPYTHON ON)
add_subdirectory(../../extern/pybind11 pybind11-build)
find_package(Threads REQUIRED)
pybind11_add_module(callosum_relay_native bindings.cpp ../../mooncake-store/src/pool_relay.cpp)
target_include_directories(callosum_relay_native PRIVATE ../../mooncake-store/include ../../mooncake-common/include)
target_compile_features(callosum_relay_native PRIVATE cxx_std_17)
target_link_libraries(callosum_relay_native PRIVATE Threads::Threads)
enable_testing()
add_test(NAME callosum_benchmark_preparation
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_benchmark_prepare.py")
set_tests_properties(callosum_benchmark_preparation PROPERTIES LABELS "relay;benchmark;preflight;t0" TIMEOUT 60)
add_test(NAME callosum_write_matrix
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_write_matrix.py")
set_tests_properties(callosum_write_matrix PROPERTIES LABELS "relay;benchmark;t0" TIMEOUT 60)
add_test(NAME callosum_write_mini
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_write_mini.py")
set_tests_properties(callosum_write_mini PROPERTIES LABELS "relay;benchmark;t0" TIMEOUT 60)
add_test(NAME callosum_benchmark_reporting
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_benchmark_report.py")
set_tests_properties(callosum_benchmark_reporting PROPERTIES LABELS "relay;benchmark;t0" TIMEOUT 60)
add_test(NAME callosum_dram_write_baseline_control
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_dram_benchmark.py")
set_tests_properties(callosum_dram_write_baseline_control PROPERTIES
  LABELS "relay;benchmark;t0" TIMEOUT 60)
set(relay_test_script "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_runtime.py")
foreach(test_method
    preflight_empty_startup_abort
    initiation_native_file_path
    status_progress_during_fetch
    transport_baseline_exact_bytes_and_units
    transport_baseline_reservation_and_final_validation
    receiver_rejects_cross_plan_alias
    scatter_tensor_projection_and_hybrid_readiness
    source_dispatch_fcfs_and_idempotent_completion
    delivery_policy_fixtures
    policy_rejection_before_reservation
    native_batch_projection
    cxl_mapping_identity
    sender_batch_cardinality_and_stale_target
    two_workers_forward_reverse_batch_and_reuse
    preflight_rejects_duplicate_members
    preflight_geometry_and_modes
    four_workers_transfer_reuse_and_journal
    failure_restart_quarantines_uncertain_operations
    timeout_late_writer_quarantine
    status_preserves_coordinator_and_sink_clocks)
  add_test(NAME callosum_runtime_${test_method}
    COMMAND "${CMAKE_COMMAND}" -E env
      "PYTHONPATH=$<TARGET_FILE_DIR:callosum_relay_native>"
      "${Python3_EXECUTABLE}" "${relay_test_script}" "RuntimeTest.test_${test_method}")
  set_tests_properties(callosum_runtime_${test_method}
    PROPERTIES LABELS "relay;multisource;t0" TIMEOUT 60)
  if(test_method MATCHES "^preflight_")
    set_property(TEST callosum_runtime_${test_method} APPEND PROPERTY LABELS preflight)
  elseif(test_method MATCHES "^status_")
    set_property(TEST callosum_runtime_${test_method} APPEND PROPERTY LABELS status)
  elseif(test_method MATCHES "^(failure_|timeout_)")
    set_property(TEST callosum_runtime_${test_method} APPEND PROPERTY LABELS failure)
  else()
    set_property(TEST callosum_runtime_${test_method} APPEND PROPERTY LABELS functional)
  endif()
endforeach()
set_property(TEST callosum_runtime_initiation_native_file_path APPEND
  PROPERTY LABELS "adapter;initiation")

# These validate a Callosum-facing contract, not an installed Dynamo integration.
foreach(category preflight function failure status)
  add_test(NAME callosum_initiation_${category}
    COMMAND "${Python3_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/test_callosum_initiation.py"
      -k "test_${category}_")
  set_tests_properties(callosum_initiation_${category} PROPERTIES
    LABELS "relay;initiation;adapter;t0;${category}" TIMEOUT 30)
endforeach()
