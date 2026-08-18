file(GLOB CXL_SOURCES "*.cpp")

add_library(cxl_transport OBJECT ${CXL_SOURCES})
if(USE_CUDA)
  target_include_directories(cxl_transport PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
endif()
# target_link_libraries(rdma_transport PUBLIC transport)
