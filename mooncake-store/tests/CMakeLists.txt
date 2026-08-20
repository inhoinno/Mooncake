function(add_store_test name)
  add_executable(${name} ${ARGN})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(
    ${name}
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog
           ibverbs
           gtest
           gtest_main
           pthread)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

function(add_ha_test name)
  add_executable(${name} ${ARGN})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(
    ${name}
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog
           gflags
           ibverbs
           gtest
           gtest_main
           pthread)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

add_store_test(buffer_allocator_test buffer_allocator_test.cpp)
add_store_test(runtime_accelerator_test runtime_accelerator_test.cpp)
add_store_test(registered_pinned_memory_test registered_pinned_memory_test.cpp)
add_store_test(allocation_strategy_test allocation_strategy_test.cpp)
add_store_test(replica_selection_test replica_selection_test.cpp)
add_store_test(gpu_transfer_policy_test gpu_transfer_policy_test.cpp)
add_store_test(multipath_placement_test multipath_placement_test.cpp)
foreach(
  gpu_path_case
  CpuDestinationUsesHostAddressablePath
  CxlGpuDestinationUsesCudaCopy
  RdmaGpuDestinationStagesByDefault
  GpuDirectRequiresExplicitOptIn
  TraceFlagIsFailClosed)
  string(TOLOWER "${gpu_path_case}" gpu_path_case_lower)
  add_test(
    NAME "gpu_transfer_${gpu_path_case_lower}"
    COMMAND
      gpu_transfer_policy_test
      "--gtest_filter=GpuTransferPolicyTest.${gpu_path_case}")
  set_tests_properties(
    "gpu_transfer_${gpu_path_case_lower}"
    PROPERTIES LABELS "gpu;multisource;t0;unit")
endforeach()
add_test(
  NAME gpu_multipath_preflight_test
  COMMAND gpu_transfer_policy_test
          --gtest_filter=GpuTransferPolicyTest.CxlGpuDestinationUsesCudaCopy:GpuTransferPolicyTest.RdmaGpuDestinationStagesByDefault)
add_test(
  NAME gpu_multipath_functional_test
  COMMAND allocation_strategy_test
          --gtest_filter=AllocationStrategyTest.CxlAwareRoutesCxlAndRdmaSegments)
add_test(
  NAME gpu_multipath_failure_test
  COMMAND allocation_strategy_test
          --gtest_filter=AllocationStrategyTest.CxlAwareDefaultPlacementExcludesCxl)
add_test(
  NAME gpu_multipath_status_test
  COMMAND gpu_transfer_policy_test
          --gtest_filter=GpuTransferPolicyTest.TraceFlagIsFailClosed)
set_tests_properties(
  gpu_multipath_preflight_test gpu_multipath_functional_test
  gpu_multipath_failure_test gpu_multipath_status_test
  PROPERTIES LABELS "gpu;multisource;t0;unit" TIMEOUT 30)
add_test(
  NAME replica_selection_env_opt_in_test
  COMMAND replica_selection_test
          --gtest_filter=ReplicaSelectionTest.EnvironmentOptInUsesBuiltinScorer)
set_tests_properties(replica_selection_env_opt_in_test
                     PROPERTIES ENVIRONMENT "MC_STORE_REPLICA_SCORING=1")
add_store_test(eviction_strategy_test eviction_strategy_test.cpp)
add_store_test(deadline_scheduler_test deadline_scheduler_test.cpp)
add_store_test(kv_event_publisher_test kv_event_publisher_test.cpp)
if(ENABLE_KV_EVENTS)
  find_library(
    KV_EVENT_TEST_ZMQ_LIBRARY
    NAMES zmq libzmq
    PATHS /usr/lib /usr/local/lib /usr/lib64)
  find_path(
    KV_EVENT_TEST_ZMQ_INCLUDE_DIR
    NAMES zmq.h
    PATHS /usr/include /usr/local/include)
  target_compile_definitions(kv_event_publisher_test
                             PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
  if(KV_EVENT_TEST_ZMQ_LIBRARY AND KV_EVENT_TEST_ZMQ_INCLUDE_DIR)
    target_include_directories(kv_event_publisher_test
                               PRIVATE ${KV_EVENT_TEST_ZMQ_INCLUDE_DIR})
    target_link_libraries(kv_event_publisher_test
                          PRIVATE ${KV_EVENT_TEST_ZMQ_LIBRARY})
  endif()
endif()
add_store_test(master_service_test master_service_test.cpp)
add_store_test(batch_evict_test batch_evict_test.cpp)
add_store_test(master_scenario_test master_scenario.cpp
               master_scenario_test.cpp)
add_store_test(master_service_scenario_test master_scenario.cpp
               master_service_scenario_test.cpp)
add_store_test(master_service_config_test master_service_config_test.cpp)
add_store_test(master_service_processing_key_double_erase_test
               master_service_processing_key_double_erase_test.cpp)
add_store_test(master_service_tenant_quota_test
               master_service_tenant_quota_test.cpp)
add_store_test(batch_remove_test batch_remove_test.cpp)
add_store_test(master_service_ssd_test master_service_ssd_test.cpp)
add_store_test(offload_on_evict_test offload_on_evict_test.cpp)
add_store_test(promotion_on_hit_test promotion_on_hit_test.cpp)
add_store_test(file_storage_promotion_test file_storage_promotion_test.cpp)
add_store_test(master_service_ssd_test_for_snapshot
               ha/snapshot/master_service_ssd_test_for_snapshot.cpp)
add_store_test(master_service_promotion_test_for_snapshot
               ha/snapshot/master_service_promotion_test_for_snapshot.cpp)
if(USE_NOF)
  add_store_test(nof_heartbeat_test nof_heartbeat_test.cpp)
endif()
add_store_test(client_integration_test client_integration_test.cpp)
add_test(
  NAME object_checksum_client_test
  COMMAND
    client_integration_test
    "--gtest_filter=ObjectChecksumTest.*:ClientIntegrationTest.ObjectChecksumRejectsCorruptedObject:ClientIntegrationTest.BatchPutPreservesObjectChecksumPairing"
)
set_tests_properties(object_checksum_client_test
                     PROPERTIES ENVIRONMENT "MOONCAKE_STORE_CHECKSUM=1")
add_store_test(rpc_timeout_test rpc_timeout_test.cpp)
if(USE_CXL)
  add_test(
    NAME cxl_allocation_endpoint_test
    COMMAND
      allocation_strategy_test
      --gtest_filter=AllocationStrategyTest.CxlUsesMountedTransferEndpoint:AllocationStrategyTest.RemovingLastAllocatorClearsMountedEndpoint
  )
  set_tests_properties(cxl_allocation_endpoint_test
                       PROPERTIES LABELS "cxl;todo1;t0;unit")

  add_store_test(cxl_client_integration_test cxl_client_integration_test.cpp)
  add_test(
    NAME cxl_client_single_process_test
    COMMAND
      cxl_client_integration_test
      --protocol=cxl
      --cxl_device_name=${CMAKE_CURRENT_BINARY_DIR}/cxl_client_single_process.pool
      --cxl_device_size=1073741824
      --transfer_engine_metadata_url=P2PHANDSHAKE
      --gtest_filter=ClientIntegrationTestCxl.BasicPutGetOperations:ClientIntegrationTestCxl.BatchPutGetOperations
  )
  set_tests_properties(
    cxl_client_single_process_test
    PROPERTIES
      LABELS "cxl;todo1;t0;integration"
      TIMEOUT 60
      RESOURCE_LOCK cxl_client_single_process_pool
      ENVIRONMENT
        "MC_CXL_PROVIDER=faketract;MC_CXL_BACKEND_KIND=file;MC_CXL_POOL_ID=callosum-single-pool;MOONCAKE_STORE_CHECKSUM=1;DEFAULT_KV_LEASE_TTL=1"
  )

  add_store_test(cxl_native_partition_test cxl_native_partition_test.cpp)
  add_test(
    NAME todo2_native_cxl_functional_test
    COMMAND cxl_native_partition_test
            --gtest_filter=CxlNativePartitionTest.FunctionalDisjointAllocationAndReclaim)
  add_test(
    NAME todo2_native_cxl_failure_test
    COMMAND cxl_native_partition_test
            --gtest_filter=CxlNativePartitionTest.FailureRejectsOverlapBoundsAndPoolMismatch:CxlNativePartitionTest.ConcurrentOverlapHasSingleWinner)
  add_test(
    NAME todo2_native_cxl_status_test
    COMMAND cxl_native_partition_test
            --gtest_filter=CxlNativePartitionTest.StatusExposesPortableOwnershipFields)
  set_tests_properties(
    todo2_native_cxl_functional_test todo2_native_cxl_failure_test
    todo2_native_cxl_status_test
    PROPERTIES LABELS "cxl;todo2;t0;unit" TIMEOUT 30)

  add_test(
    NAME todo2_native_cxl_single_process_store_test
    COMMAND
      cxl_client_integration_test
      --protocol=cxl
      --cxl_device_name=${CMAKE_CURRENT_BINARY_DIR}/todo2_native_cxl_single_process.pool
      --cxl_device_size=1073741824
      --transfer_engine_metadata_url=P2PHANDSHAKE
      --gtest_filter=ClientIntegrationTestCxl.BasicPutGetOperations:ClientIntegrationTestCxl.BatchPutGetOperations)
  set_tests_properties(
    todo2_native_cxl_single_process_store_test
    PROPERTIES
      LABELS "cxl;todo2;t0;integration"
      TIMEOUT 60
      RESOURCE_LOCK todo2_native_cxl_single_process_pool
      ENVIRONMENT
        "MC_CXL_PROVIDER=mooncake;MC_CXL_BACKEND_KIND=file;MC_CXL_POOL_ID=todo2-native-single-pool;MC_CXL_OWNED_OFFSET=0;MC_CXL_OWNED_SIZE=1073741824;MOONCAKE_STORE_CHECKSUM=1;DEFAULT_KV_LEASE_TTL=1")
endif()

# TODO1.5 protocol gates are deliberately portable.  They validate the
# two-process orchestration and fail-closed checksum/publication behavior; the
# real shared-CXL acceptance run is launched separately on two lab nodes.
set(TODO15_SHARED_CXL_TEST_CASES
    preflight_validation
    functional_bidirectional_single_and_batch
    failure_checksum_mismatch_blocks_publication
    status_summary_is_safe_and_complete)
foreach(todo15_case IN LISTS TODO15_SHARED_CXL_TEST_CASES)
  set(todo15_test_name "todo15_shared_cxl_${todo15_case}_test")
  add_test(
    NAME ${todo15_test_name}
    COMMAND
      ${PYTHON_EXECUTABLE} -m unittest -v
      test_todo15_shared_cxl.Todo15SharedCxlProtocolTest.test_${todo15_case})
  set_tests_properties(
    ${todo15_test_name}
    PROPERTIES
      LABELS "cxl;todo15;t0;unit"
      TIMEOUT 30
      WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/scripts")
endforeach()

# The WSS protocol tests remain dependency-light and use a bounded in-memory
# Store model. The 500 GiB plan test verifies exact accounting without
# allocating the payload; only the two-node launcher can close the T2 gate.
set(TODO2_CXL_WSS_TEST_CASES
    preflight_500g_plan_and_capacity
    functional_single_put_peer_get
    failure_corruption_blocks_verified
    status_is_bounded_and_safe)
foreach(todo2_wss_case IN LISTS TODO2_CXL_WSS_TEST_CASES)
  set(todo2_wss_test_name "todo2_cxl_wss_${todo2_wss_case}_test")
  add_test(
    NAME ${todo2_wss_test_name}
    COMMAND
      ${PYTHON_EXECUTABLE} -m unittest -v
      test_todo2_cxl_wss.Todo2CxlWssTest.test_${todo2_wss_case})
  set_tests_properties(
    ${todo2_wss_test_name}
    PROPERTIES
      LABELS "cxl;todo2;wss;t0;unit"
      TIMEOUT 30
      WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/scripts")
endforeach()

# Perf harness protocol/accounting gates are dependency-light. Hardware
# throughput remains a separate T1/T2 lab run.
add_test(
  NAME todoextra_rdma_cxl_perf_harness_test
  COMMAND ${PYTHON_EXECUTABLE} -m unittest -v test_perf_harness.py)
set_tests_properties(
  todoextra_rdma_cxl_perf_harness_test
  PROPERTIES
    LABELS "cxl;rdma;gpu;todoextra;perf;t0;unit"
    TIMEOUT 30
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/scripts")

add_store_test(master_metrics_test master_metrics_test.cpp)
add_store_test(master_admin_server_test master_admin_server_test.cpp)
add_store_test(posix_file_test posix_file_test.cpp)
add_store_test(thread_pool_test thread_pool_test.cpp)
add_store_test(transfer_task_test transfer_task_test.cpp)
find_package(CUDAToolkit QUIET)
if(CUDAToolkit_FOUND)
  target_compile_definitions(transfer_task_test PRIVATE MOONCAKE_TEST_CUDA_H2D)
  target_link_libraries(transfer_task_test PRIVATE CUDA::cudart)
endif()
if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
  target_compile_definitions(transfer_task_test PRIVATE USE_CUDA)
  if(USE_TENT)
    add_test(
      NAME transfer_scatter_tent_test
      COMMAND
        transfer_task_test
        --gtest_filter=TransferTaskTest.TransferScatterWritesGpuDestinationDirectly
    )
    set_tests_properties(transfer_scatter_tent_test PROPERTIES ENVIRONMENT
                                                               "MC_USE_TENT=1")
  endif()
endif()
add_store_test(tenant_quota_test tenant_quota_test.cpp)
add_store_test(tenant_id_test tenant_id_test.cpp)
add_store_test(segment_test segment_test.cpp)
add_store_test(offset_allocator_test offset_allocator_test.cpp)
add_store_test(utils_test utils_test.cpp)
add_store_test(client_buffer_test client_buffer_test.cpp)
add_store_test(client_local_hot_cache_test client_local_hot_cache_test.cpp)
add_store_test(client_tcp_local_memcpy_test client_tcp_local_memcpy_test.cpp)
add_store_test(pybind_client_test pybind_client_test.cpp)
if(CUDAToolkit_FOUND)
  target_compile_definitions(pybind_client_test PRIVATE MOONCAKE_TEST_CUDA_H2D)
  target_link_libraries(pybind_client_test PRIVATE CUDA::cudart)
endif()
add_store_test(ipv6_client_test ipv6_client_test.cpp)
add_store_test(host_port_fix_test host_port_fix_test.cpp)
add_store_test(client_metrics_test client_metrics_test.cpp)
add_store_test(ssd_metrics_test ssd_metrics_test.cpp)
add_store_test(serializer_test serializer_test.cpp)
add_store_test(
  embedded_snapshot_catalog_store_test
  ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store_test.cpp
)
add_store_test(zstd_util_test zstd_util_test.cpp)
add_store_test(
  local_file_snapshot_object_store_test
  ha/snapshot/object/backends/local/local_file_snapshot_object_store_test.cpp)
add_store_test(file_util_test file_util_test.cpp)
add_store_test(snapshot_child_process_test
               ha/snapshot/snapshot_child_process_test.cpp)
add_store_test(master_snapshot_codec_test
               ha/snapshot/master_snapshot_codec_test.cpp)
add_ha_test(batch_oplog_snapshot_types_test
            ha/snapshot/batch_oplog/metadata_test.cpp)
add_store_test(master_service_test_for_snapshot
               ha/snapshot/master_service_test_for_snapshot.cpp)
add_store_test(non_ha_reconnect_test non_ha_reconnect_test.cpp)
add_store_test(storage_backend_test storage_backend_test.cpp)
add_store_test(client_storage_backend_test client_storage_backend_test.cpp)
add_store_test(mutex_test mutex_test.cpp)
add_store_test(file_storage_test file_storage_test.cpp)
add_store_test(task_manager_test task_manager_test.cpp)
add_store_test(task_executor_test task_executor_test.cpp)
add_store_test(task_integration_test task_integration_test.cpp)
add_store_test(dummy_client_get_buffer_test dummy_client_get_buffer_test.cpp)
add_test(
  NAME dummy_client_object_checksum_test
  COMMAND
    dummy_client_get_buffer_test
    "--gtest_filter=DummyClientGetBufferTest.GetIntoRejectsCorruptedObjectWithChecksum:DummyClientGetBufferTest.BatchQueryPreservesObjectChecksum"
)
set_tests_properties(dummy_client_object_checksum_test
                     PROPERTIES ENVIRONMENT "MOONCAKE_STORE_CHECKSUM=1")
add_store_test(uds_transport_test uds_transport_test.cpp)
add_store_test(health_check_test health_check_test.cpp)
add_store_test(http_metadata_server_test http_metadata_server_test.cpp)
add_store_test(mmap_arena_test mmap_arena_test.cpp)
add_store_test(mmap_arena_fallback_test mmap_arena_fallback_test.cpp)
add_store_test(object_data_type_test object_data_type_test.cpp)
add_subdirectory(e2e)

add_executable(high_availability_test ha/leadership/high_availability_test.cpp)
target_include_directories(high_availability_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(STORE_USE_REDIS)
  target_sources(
    high_availability_test
    PRIVATE ha/leadership/backends/redis/high_availability_redis_test.cpp)
  target_include_directories(high_availability_test
                             PRIVATE ${MOONCAKE_STORE_HIREDIS_INCLUDE_DIR})
  target_link_libraries(high_availability_test
                        PRIVATE ${MOONCAKE_STORE_HIREDIS_LIBRARY})

  add_executable(
    redis_snapshot_catalog_store_test
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store_test.cpp)
  target_include_directories(
    redis_snapshot_catalog_store_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${MOONCAKE_STORE_HIREDIS_INCLUDE_DIR})
  target_link_libraries(
    redis_snapshot_catalog_store_test
    PUBLIC mooncake_store
           transfer_engine
           cachelib_memory_allocator
           ${ETCD_WRAPPER_LIB}
           glog
           gflags
           ibverbs
           gtest
           pthread
    PRIVATE ${MOONCAKE_STORE_HIREDIS_LIBRARY})
  add_test(NAME redis_snapshot_catalog_store_test
           COMMAND redis_snapshot_catalog_store_test)
endif()
if(STORE_USE_K8S_LEASE)
  target_sources(
    high_availability_test
    PRIVATE ha/leadership/backends/k8s/high_availability_k8s_test.cpp)
endif()
target_link_libraries(
  high_availability_test
  PUBLIC mooncake_store
         transfer_engine
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         glog
         gtest
         gtest_main
         pthread)
if(STORE_USE_K8S_LEASE)
  target_link_libraries(high_availability_test PRIVATE ${K8S_LEASE_WRAPPER_LIB})
endif()
if(STORE_USE_ETCD
   OR STORE_USE_REDIS
   OR STORE_USE_K8S_LEASE)
  add_test(NAME high_availability_test COMMAND high_availability_test)
endif()
add_ha_test(ha_backend_availability_test
            ha/leadership/ha_backend_availability_test.cpp)
add_ha_test(leader_label_reconciler_test
            ha/leadership/leader_label_reconciler_test.cpp)
add_ha_test(oplog_batch_storage_test ha/oplog/oplog_batch_storage_test.cpp)
add_ha_test(oplog_batch_auditor_test ha/oplog/oplog_batch_auditor_test.cpp)
target_sources(oplog_batch_auditor_test
               PRIVATE ../tools/oplog_batch_auditor.cpp)
target_include_directories(oplog_batch_auditor_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(
  stress_workload_test
  PUBLIC mooncake_store
         transfer_engine
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         mooncake_common
         glog
         gflags
         pthread)

# HA tests
add_ha_test(standby_state_machine_test
            ha/standby/standby_state_machine_test.cpp)
add_ha_test(ha_metric_manager_test ha/standby/ha_metric_manager_test.cpp)
add_ha_test(hot_standby_service_test ha/standby/hot_standby_service_test.cpp)
add_ha_test(hot_standby_snapshot_bootstrap_test
            ha/standby/hot_standby_snapshot_bootstrap_test.cpp)
add_ha_test(oplog_applier_test ha/oplog/oplog_applier_test.cpp)
add_ha_test(oplog_types_test ha/oplog/oplog_types_test.cpp)
add_ha_test(oplog_batch_codec_test ha/oplog/oplog_batch_codec_test.cpp)
add_ha_test(oplog_batch_standby_reader_test
            ha/oplog/oplog_batch_standby_reader_test.cpp)
add_ha_test(ordered_oplog_writer_test ha/oplog/ordered_oplog_writer_test.cpp)
if(MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(ha_metric_manager_test
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(ordered_oplog_writer_test
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
endif()
add_executable(
  oplog_test_failpoint_test ha/oplog/oplog_test_failpoint_test.cpp
                            ../src/ha/oplog/oplog_test_failpoint.cpp)
target_compile_definitions(oplog_test_failpoint_test
                           PRIVATE MOONCAKE_ENABLE_TEST_FAILPOINTS)
target_link_libraries(oplog_test_failpoint_test PRIVATE glog gtest gtest_main
                                                        pthread)
add_test(NAME oplog_test_failpoint_test COMMAND oplog_test_failpoint_test)
add_ha_test(master_service_ha_test ha/master_service_ha_test.cpp)
add_ha_test(catalog_backed_snapshot_provider_test
            ha/snapshot/catalog_backed_snapshot_provider_test.cpp)
if(STORE_USE_REDIS)
  foreach(target catalog_backed_snapshot_provider_test
                 hot_standby_snapshot_bootstrap_test)
    target_include_directories(${target}
                               PRIVATE ${MOONCAKE_STORE_HIREDIS_INCLUDE_DIR})
    target_link_libraries(${target} PRIVATE ${MOONCAKE_STORE_HIREDIS_LIBRARY})
  endforeach()
endif()
