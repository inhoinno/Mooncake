// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#ifdef USE_CUDA
#include <cuda_runtime_api.h>
#endif

#include "transfer_engine.h"
#include "transport/transport.h"
#include "transport/cxl_transport/cxl_transport.h"
#include "common.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
// SetUp() publishes and then resolves this process's own CXL segment by name
// (openSegment -> getSegmentDescByID), which needs a real metadata store; P2P
// handshake cannot resolve a process's own bare-name segment. Match the rest of
// the suite (scripts/run_tests.sh) and default to the lightweight HTTP metadata
// server, which requires no etcd. Start it first:
//   mooncake_http_metadata_server --port 8080 &
// Override with --metadata_server=<etcd host:port> for an etcd deployment.
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Transfer Engine metadata server "
              "(http://host:port/metadata, or etcd host:port)");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "cxl", "Transfer protocol: rdma|tcp|cxl");

DEFINE_string(device_name, "tmp_dax_sim", "Device name for cxl");

DEFINE_int64(device_size, 1073741824, "Device Size for cxl");

static void* allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void* addr, size_t size) { numa_free(addr, size); }

class CXLTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    int tmp_fd = -1;
    uint8_t* addr = nullptr;
    uint8_t* base_addr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t offset_2 = 6 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;
    CxlTransport* cxl_xport;
    Transport* xport;
    void** args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    const size_t kDataLength = 4 * 1024;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("CXLTransportTest");
        FLAGS_logtostderr = 1;

        tmp_fd = open(FLAGS_device_name.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(tmp_fd, 0);
        ASSERT_EQ(ftruncate(tmp_fd, FLAGS_device_size), 0);

        // Set device name from gflags parameter
        setenv("MC_CXL_DEV_PATH", FLAGS_device_name.c_str(), 1);

        setenv("MC_CXL_DEV_SIZE", std::to_string(FLAGS_device_size).c_str(), 1);
        setenv("MC_CXL_PROVIDER", "faketract", 1);
        setenv("MC_CXL_BACKEND_KIND", "file", 1);
        setenv("MC_CXL_POOL_ID", "cxl-transport-test-pool", 1);

        // cxl setup
        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;

        args = (void**)malloc(2 * sizeof(void*));
        args[0] = nullptr;
        xport = engine->installTransport("cxl", args);
        ASSERT_NE(xport, nullptr);

        cxl_xport = dynamic_cast<CxlTransport*>(xport);
        base_addr = (uint8_t*)cxl_xport->getCxlBaseAddr();
        ASSERT_EQ(cxl_xport->getCxlDeviceSize(),
                  static_cast<size_t>(FLAGS_device_size));
        ASSERT_EQ(cxl_xport->getCxlPoolId(), "cxl-transport-test-pool");
        ASSERT_EQ(cxl_xport->getCxlPoolStatus().provider, "faketract");
        ASSERT_NE(cxl_xport->getCxlPoolStatus().ToJson().find(
                      "\"lifecycle\":\"ready\""),
                  std::string::npos);
        addr = (uint8_t*)allocateMemoryPool(kDataLength, 0, false);
        int rc = engine->registerLocalMemory(base_addr + offset_1, len);
        ASSERT_EQ(rc, 0);

        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        // bindToSocket(0);
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_NE(segment_desc, nullptr);
        ASSERT_EQ(segment_desc->cxl_pool_id, "cxl-transport-test-pool");
        ASSERT_EQ(segment_desc->cxl_map_offset, 0);
        ASSERT_EQ(segment_desc->cxl_capacity,
                  static_cast<uint64_t>(FLAGS_device_size));
    }

    void TearDown() override {
        if (tmp_fd >= 0) {
            close(tmp_fd);
            unlink(FLAGS_device_name.c_str());
        }
        unsetenv("MC_CXL_BACKEND_KIND");
        unsetenv("MC_CXL_PROVIDER");
        unsetenv("MC_CXL_POOL_ID");
        free(args);
        google::ShutdownGoogleLogging();
        freeMemoryPool(addr, kDataLength);
    }
};

TEST_F(CXLTransportTest, MultiWrite) {
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char*)(addr) + offset) = 'a' + lrand48() % 26;
        auto batch_id = xport->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t*)(addr);
        entry.target_id = segment_id;
        entry.target_offset = offset_1;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }

        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
}

TEST_F(CXLTransportTest, MultipleRead) {
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char*)(addr) + offset) = 'a' + lrand48() % 26;
        auto batch_id = xport->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t*)(addr);
        entry.target_id = segment_id;
        entry.target_offset = offset_2;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }

        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }

    // sleep(10);

    times = 10;
    while (times--) {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        void* src = allocateMemoryPool(kDataLength, 0, false);

        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t*)(src);
        entry.target_id = segment_id;
        entry.target_offset = offset_2;
        Status s;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
        ASSERT_EQ(s, Status::OK());

        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                completed = true;
            }
        }

        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
        ret = memcmp((uint8_t*)(src), (uint8_t*)(addr), kDataLength);
        ASSERT_EQ(ret, 0);

        freeMemoryPool(src, kDataLength);
    }
    engine->unregisterLocalMemory(addr);
}

#ifdef USE_CUDA
TEST_F(CXLTransportTest, FunctionalCxlToGpuAndGpuToCxl) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        cudaGetLastError();
        GTEST_SKIP() << "CUDA device is not available";
    }

    auto transfer_and_wait = [&](TransferRequest request) {
        auto batch_id = xport->allocateBatchID(1);
        EXPECT_TRUE(engine->submitTransfer(batch_id, {request}).ok());
        TransferStatus status;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        do {
            EXPECT_TRUE(xport->getTransferStatus(batch_id, 0, status).ok());
            if (std::chrono::steady_clock::now() >= deadline) {
                ADD_FAILURE() << "CXL CUDA transfer timed out";
                break;
            }
        } while (status.s == TransferStatusEnum::WAITING ||
                 status.s == TransferStatusEnum::PENDING);
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_TRUE(xport->freeBatchID(batch_id).ok());
    };

    for (size_t i = 0; i < kDataLength; ++i) addr[i] = i % 251;
    transfer_and_wait(TransferRequest{.opcode = TransferRequest::WRITE,
                                      .source = addr,
                                      .target_id = segment_id,
                                      .target_offset = offset_1,
                                      .length = kDataLength});

    void* gpu_buffer = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_buffer, kDataLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(gpu_buffer, 0, kDataLength), cudaSuccess);
    transfer_and_wait(TransferRequest{.opcode = TransferRequest::READ,
                                      .source = gpu_buffer,
                                      .target_id = segment_id,
                                      .target_offset = offset_1,
                                      .length = kDataLength});

    std::vector<uint8_t> observed(kDataLength);
    ASSERT_EQ(cudaMemcpy(observed.data(), gpu_buffer, kDataLength,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(std::memcmp(observed.data(), addr, kDataLength), 0);

    for (size_t i = 0; i < kDataLength; ++i) observed[i] = (i * 7) % 251;
    ASSERT_EQ(cudaMemcpy(gpu_buffer, observed.data(), kDataLength,
                         cudaMemcpyHostToDevice),
              cudaSuccess);
    transfer_and_wait(TransferRequest{.opcode = TransferRequest::WRITE,
                                      .source = gpu_buffer,
                                      .target_id = segment_id,
                                      .target_offset = offset_1,
                                      .length = kDataLength});
    EXPECT_EQ(std::memcmp(base_addr + offset_1, observed.data(), kDataLength),
              0);
    EXPECT_EQ(cudaFree(gpu_buffer), cudaSuccess);
}
#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    // InitGoogleTest must run first so it strips --gtest_* from argv; otherwise
    // gflags::ParseCommandLineFlags aborts with "unknown command line flag
    // 'gtest_filter'" when ctest selects a single case by filter.
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
