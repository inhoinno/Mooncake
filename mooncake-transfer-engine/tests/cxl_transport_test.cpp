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
#include <algorithm>
#include <cstring>
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>

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
DEFINE_bool(devdax_gpu_read_ack, false,
            "Enable the read-only real-devdax GPU case after obtaining a stable source lease");
DEFINE_string(devdax_gpu_read_owner, "", "Approved source extent/lease owner reference");
DEFINE_uint64(devdax_gpu_read_offset, 0, "Source offset relative to the configured CXL mapping");
DEFINE_uint64(devdax_gpu_read_bytes, 4096, "Read-only source bytes (1..64 MiB)");

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

        struct stat backing {};
        if (lstat(FLAGS_device_name.c_str(), &backing) == 0) {
            ASSERT_TRUE(S_ISREG(backing.st_mode))
                << "File fixture refuses devices/symlinks; use CXLDevDaxGpuTest for DAX";
        }
        tmp_fd = open(FLAGS_device_name.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW, 0666);
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

TEST_F(CXLTransportTest, RejectsDifferentPoolBeforeCopy) {
    // Both dispatch APIs must reject a remote pool offset instead of treating
    // it as an address in this process's unrelated local CXL mapping.
    const auto original_pool = segment_desc->cxl_pool_id;
    segment_desc->cxl_pool_id = "independent-remote-pool";
    for (bool through_engine : {false, true}) {
        std::memset(addr, 0xa5, kDataLength);
        std::memset(base_addr + offset_1, 0x5a, kDataLength);
        auto batch_id = xport->allocateBatchID(1);
        TransferRequest entry{.opcode = TransferRequest::WRITE,
                              .source = addr,
                              .target_id = segment_id,
                              .target_offset = offset_1,
                              .length = kDataLength};
        const auto submitted = through_engine
                                   ? engine->submitTransfer(batch_id, {entry})
                                   : xport->submitTransfer(batch_id, {entry});
        EXPECT_TRUE(submitted.ok());
        TransferStatus status;
        EXPECT_TRUE(xport->getTransferStatus(batch_id, 0, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
        for (size_t i = 0; i < kDataLength; ++i)
            EXPECT_EQ(base_addr[offset_1 + i], 0x5a);
        EXPECT_TRUE(xport->freeBatchID(batch_id).ok());
    }
    segment_desc->cxl_pool_id = original_pool;
}

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
// Separate fixture: the file fixture above creates, truncates and unlinks its
// backing. Never route a DAX path through that fixture.
TEST(CXLDevDaxGpuTest, FunctionalReadToGpu) {
    if (!FLAGS_devdax_gpu_read_ack) {
        GTEST_SKIP() << "Explicit devdax source read lease and acknowledgement required";
    }
    ASSERT_FALSE(FLAGS_devdax_gpu_read_owner.empty());
    const char* kind = std::getenv("MC_CXL_BACKEND_KIND");
    const char* path = std::getenv("MC_CXL_DEV_PATH");
    ASSERT_NE(kind, nullptr);
    ASSERT_STREQ(kind, "devdax");
    ASSERT_NE(path, nullptr);
    struct stat info {};
    ASSERT_EQ(stat(path, &info), 0);
    ASSERT_TRUE(S_ISCHR(info.st_mode));
    ASSERT_GT(FLAGS_devdax_gpu_read_bytes, 0);
    ASSERT_LE(FLAGS_devdax_gpu_read_bytes, 64 * 1024 * 1024);
    int devices = 0;
    ASSERT_EQ(cudaGetDeviceCount(&devices), cudaSuccess);
    ASSERT_GT(devices, 0);

    TransferEngine engine(false);
    const auto endpoint = parseHostNameWithPort(FLAGS_local_server_name);
    ASSERT_EQ(engine.init(FLAGS_metadata_server, FLAGS_local_server_name,
                          endpoint.first, endpoint.second), 0);
    void* arguments[2] = {nullptr, nullptr};
    auto* transport = engine.installTransport("cxl", arguments);
    ASSERT_NE(transport, nullptr);
    auto* cxl = dynamic_cast<CxlTransport*>(transport);
    ASSERT_NE(cxl, nullptr);
    const auto capacity = cxl->getCxlDeviceSize();
    const auto offset = FLAGS_devdax_gpu_read_offset;
    const auto length = FLAGS_devdax_gpu_read_bytes;
    ASSERT_LE(offset, capacity);
    ASSERT_LE(length, capacity - offset);
    auto* source = static_cast<uint8_t*>(cxl->getCxlBaseAddr()) + offset;
    std::vector<uint8_t> expected(source, source + length);
    ASSERT_TRUE(std::any_of(expected.begin(), expected.end(),
                            [](uint8_t byte) { return byte != 0; }))
        << "Use a stable nonzero source; the test never seeds or writes DAX";
    ASSERT_EQ(engine.registerLocalMemory(source, length), 0);
    const auto segment = engine.openSegment(FLAGS_local_server_name.c_str());
    ASSERT_NE(engine.getMetadata()->getSegmentDescByID(segment), nullptr);
    void* gpu = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu, length), cudaSuccess);
    ASSERT_EQ(cudaMemset(gpu, 0, length), cudaSuccess);
    const auto batch = transport->allocateBatchID(1);
    TransferRequest request{.opcode = TransferRequest::READ,
                            .source = gpu,
                            .target_id = segment,
                            .target_offset = offset,
                            .length = length};
    // A submit/status error may leave a CUDA copy active. Retain this stack,
    // engine and GPU allocation instead of freeing on an uncertain deadline.
    bool failed = !engine.submitTransfer(batch, {request}).ok();
    if (failed) ADD_FAILURE() << "Submission failed; retain source/GPU lifetime until completion";
    while (true) {
        TransferStatus status;
        const auto result = transport->getTransferStatus(batch, 0, status);
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) break;
        if (!failed && (!result.ok() ||
            (status.s != TransferStatusEnum::WAITING && status.s != TransferStatusEnum::PENDING))) {
            ADD_FAILURE() << "Completion uncertain; retain resources for operator diagnosis";
            failed = true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::vector<uint8_t> observed(length);
    EXPECT_EQ(cudaMemcpy(observed.data(), gpu, length, cudaMemcpyDeviceToHost), cudaSuccess);
    EXPECT_EQ(observed, expected);
    EXPECT_EQ(std::memcmp(source, expected.data(), length), 0) << "Source lease changed during load";
    EXPECT_TRUE(transport->freeBatchID(batch).ok());
    EXPECT_EQ(cudaFree(gpu), cudaSuccess);
    std::cout << "CXL_DEVDAX_GPU_FUNCTIONAL bytes=" << length
              << " backend=devdax api=cudaMemcpyAsync direct_dma_proven=false\n";
}

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
