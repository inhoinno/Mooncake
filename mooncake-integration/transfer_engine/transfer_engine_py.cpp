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

#include "transfer_engine_py.h"
#include "write_pipeline_engine.h"
#ifdef __linux__
#include <sys/auxv.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif
#include "../pool_relay/relay_batch.h"

#include <cassert>
#include <array>
#include <chrono>
#include <numeric>
#include <fstream>

#include <pybind11/stl.h>
#include "config.h"
#include "transport/rpc_communicator/rpc_interface.h"
#include "transport/rdma_transport/rdma_context.h"

#ifdef USE_TENT
#include "tent/common/types.h"
#endif

#ifdef USE_EFA
#include "transport/efa_transport/efa_transport.h"
#endif

#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#endif

#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif

#ifdef USE_INTRA_NVLINK
#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"
#endif

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

static void* (*allocateMemory)(size_t) = nullptr;
static void (*freeMemory)(void*) = nullptr;
static std::string g_protocol;

//  Handle allocateMemory function pointer based on protocol
void initMemoryAllocator(const char* protocol) {
    if (allocateMemory != nullptr) {
        LOG(WARNING) << "Memory allocator already initialized with: "
                     << g_protocol;
        return;
    }
    g_protocol = protocol;
    if (strcmp(protocol, "nvlink") == 0) {
#ifdef USE_MNNVL
        allocateMemory = [](size_t s) -> void* {
            return mooncake::NvlinkTransport::allocatePinnedLocalMemory(s);
        };
        freeMemory = [](void* p) {
            mooncake::NvlinkTransport::freePinnedLocalMemory(p);
        };
        LOG(INFO) << "Selected MNNVL (NVLink) memory allocator";
#else
        LOG(ERROR) << "Protocol 'nvlink' requires -DUSE_MNNVL=ON";
#endif
    } else if (strcmp(protocol, "hip") == 0) {
#ifdef USE_HIP
        allocateMemory = [](size_t s) -> void* {
            return mooncake::HipTransport::allocatePinnedLocalMemory(s);
        };
        freeMemory = [](void* p) {
            mooncake::HipTransport::freePinnedLocalMemory(p);
        };
        LOG(INFO) << "Selected HIP memory allocator";
#else
        LOG(ERROR) << "Protocol 'hip' requires -DUSE_HIP=ON";
#endif
    } else if (strcmp(protocol, "nvlink_intra") == 0) {
#ifdef USE_INTRA_NVLINK
        allocateMemory = [](size_t s) -> void* {
            return mooncake::IntraNodeNvlinkTransport::
                allocatePinnedLocalMemory(s);
        };
        freeMemory = [](void* p) {
            mooncake::IntraNodeNvlinkTransport::freePinnedLocalMemory(p);
        };
        LOG(INFO) << "Selected Intra-NVLink memory allocator";
#else
        LOG(ERROR) << "Protocol 'nvlink_intra' requires -DUSE_INTRA_NVLINK=ON";
#endif
    } else {
        allocateMemory = malloc;
        freeMemory = free;
        LOG(WARNING) << "Using default malloc/free for protocol: " << protocol;
    }
}

TransferEnginePy::TransferEnginePy() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    if (getenv("MC_TRANSFER_TIMEOUT")) {
        int timeout_sec = std::max(5, atoi(getenv("MC_TRANSFER_TIMEOUT")));
        transfer_timeout_nsec_ = timeout_sec * kNanosPerSecond;
    } else {
        transfer_timeout_nsec_ = 30 * kNanosPerSecond;
    }
}

TransferEnginePy::~TransferEnginePy() {
    for (auto& handle : handle_map_) engine_->closeSegment(handle.second);
    handle_map_.clear();
    engine_.reset();
    for (auto& buffer : buffer_list_) freeMemory(buffer);
    buffer_list_.clear();
    for (auto& buffer : large_buffer_list_) freeMemory(buffer);
    large_buffer_list_.clear();
}

std::vector<std::string> buildDeviceFilter(const std::string& device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }
    return tokens;
}

std::pair<std::string, std::string> parseConnectionString(
    const std::string& conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else if (conn_string == P2PHANDSHAKE) {
        proto = "";
        domain = P2PHANDSHAKE;
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

std::string buildConnString(const std::string& metadata_type,
                            const std::string& metadata_server) {
    if (metadata_server == P2PHANDSHAKE) {
        return P2PHANDSHAKE;
    }

    std::string conn_string = metadata_server;
    if (conn_string.find("://") == std::string::npos)
        conn_string = metadata_type + "://" + metadata_server;
    return conn_string;
}

int TransferEnginePy::initialize(const char* local_hostname,
                                 const char* metadata_server,
                                 const char* protocol,
                                 const char* device_name) {
    initMemoryAllocator(protocol);

    auto conn_string = parseConnectionString(metadata_server);
    return initializeExt(local_hostname, conn_string.second.c_str(), protocol,
                         device_name, conn_string.first.c_str());
}

int TransferEnginePy::initializeExt(const char* local_hostname,
                                    const char* metadata_server,
                                    const char* protocol,
                                    const char* device_name,
                                    const char* metadata_type) {
    if (strcmp(protocol, "xgmi") == 0) {
        LOG(ERROR) << "Protocol 'xgmi' is not exposed in the Python API. "
                   << "Use 'hip' instead.";
        return -1;
    }

    std::string proto = protocol ? std::string(protocol) : "";
    std::string conn_string = buildConnString(metadata_type, metadata_server);

    auto device_name_safe = device_name ? std::string(device_name) : "";
    auto device_filter = buildDeviceFilter(device_name_safe);

#ifdef USE_EFA
    // When using EFA protocol, we still need topology discovery but won't
    // auto-install RDMA
    bool use_efa = (proto == "efa");
    // Disable auto_discover to prevent RDMA transport installation, we'll
    // install EFA manually
    engine_ = std::make_unique<TransferEngine>(false, device_filter);
    // Manually discover topology for EFA to populate device list
    if (use_efa) {
        engine_->getLocalTopology()->discover(device_filter);
        LOG(INFO) << "Topology discovery complete for EFA. Found "
                  << engine_->getLocalTopology()->getHcaList().size()
                  << " devices.";
    }
#elif defined(USE_CXI)

    bool use_cxi = (proto == "cxi");
    engine_ = std::make_unique<TransferEngine>(false, device_filter);
    if (use_cxi) {
        engine_->getLocalTopology()->discover(device_filter);
        LOG(INFO) << "Topology discovery complete for CXI. Found "
                  << engine_->getLocalTopology()->getHcaList().size()
                  << " devices.";
    }
#else
    engine_ = std::make_unique<TransferEngine>(true, device_filter);
#endif

    if (getenv("MC_LEGACY_RPC_PORT_BINDING")) {
        auto hostname_port = parseHostNameWithPort(local_hostname);
        int ret =
            engine_->init(conn_string, local_hostname,
                          hostname_port.first.c_str(), hostname_port.second);
        if (ret) return -1;
    } else {
        // the last two params are unused
        int ret = engine_->init(conn_string, local_hostname, "", 0);
        if (ret) return -1;
    }

#ifdef USE_EFA
    // Install EFA transport when protocol is "efa"
    if (use_efa) {
        LOG(INFO)
            << "Installing EFA transport as requested by protocol parameter";
        auto transport = engine_->installTransport("efa", nullptr);
        if (!transport) {
            LOG(ERROR) << "Failed to install EFA transport";
            return -1;
        }
        LOG(INFO) << "EFA transport installed successfully";
    } else {
        // For non-EFA protocols (e.g. TCP), manually install TCP transport
        // since auto_discover is disabled to prevent RDMA installation
        // (RDMA QP creation fails on EFA devices).
        LOG(INFO)
            << "Installing TCP transport (auto_discover disabled in EFA build)";
        auto transport = engine_->installTransport("tcp", nullptr);
        if (!transport) {
            LOG(ERROR) << "Failed to install TCP transport";
            return -1;
        }
        LOG(INFO) << "TCP transport installed successfully";
    }
#elif defined(USE_CXI)
    if (use_cxi) {
        LOG(INFO)
            << "Installing CXI transport as requested by protocol parameter";
        auto transport = engine_->installTransport("cxi", nullptr);
        if (!transport) {
            LOG(ERROR) << "Failed to install CXI transport";
            return -1;
        }
        LOG(INFO) << "CXI transport installed successfully";
    } else {
        // For non-EFA protocols (e.g. TCP), manually install TCP transport
        // since auto_discover is disabled to prevent RDMA installation
        // (RDMA QP creation fails on EFA devices).
        LOG(INFO)
            << "Installing TCP transport (auto_discover disabled in CXI build)";
        auto transport = engine_->installTransport("tcp", nullptr);
        if (!transport) {
            LOG(ERROR) << "Failed to install TCP transport";
            return -1;
        }
        LOG(INFO) << "TCP transport installed successfully";
    }
#endif

    free_list_.resize(kSlabSizeKBTabLen);
    return 0;
}

int TransferEnginePy::getRpcPort() { return engine_->getRpcPort(); }

char* TransferEnginePy::allocateRawBuffer(size_t capacity) {
    auto buffer = allocateMemory(capacity);
    if (!buffer) return nullptr;
    int ret = engine_->registerLocalMemory(buffer, capacity, kWildcardLocation);
    if (ret) {
        freeMemory(buffer);
        return nullptr;
    }
    return (char*)buffer;
}

int TransferEnginePy::findClassId(size_t size) {
    if (size > 1024ull * kSlabSizeKB[kMaxClassId]) return -1;
    for (int i = kMaxClassId - 1; i >= 0; --i)
        if (size > 1024ull * kSlabSizeKB[i]) return i + 1;
    return 0;
}

int TransferEnginePy::doBuddyAllocate(int class_id) {
    if (class_id == kMaxClassId) {
        auto buffer = allocateRawBuffer(kDefaultBufferCapacity);
        if (!buffer) return -1;
        buffer_list_.push_back(buffer);
        for (size_t offset = 0; offset < kDefaultBufferCapacity;
             offset += 1024ull * kSlabSizeKB[kMaxClassId])
            free_list_[kMaxClassId].push(buffer + offset);
        return 0;
    }
    if (free_list_[class_id + 1].empty()) {
        int ret = doBuddyAllocate(class_id + 1);
        if (ret) return ret;
    }
    assert(!free_list_[class_id + 1].empty());
    char* buffer = free_list_[class_id + 1].top();
    free_list_[class_id + 1].pop();
    free_list_[class_id].push(buffer);
    free_list_[class_id].push(buffer + kSlabSizeKB[class_id] * 1024);
    return 0;
}

uintptr_t TransferEnginePy::allocateManagedBuffer(size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    int class_id = findClassId(length);
    if (class_id < 0) {
        char* buffer = allocateRawBuffer(length);
        if (buffer) large_buffer_list_.insert(buffer);
        return (uintptr_t)buffer;
    }
    if (free_list_[class_id].empty())
        if (doBuddyAllocate(class_id)) return 0;
    assert(!free_list_[class_id].empty());
    char* buffer = free_list_[class_id].top();
    free_list_[class_id].pop();
    return (uintptr_t)buffer;
}

int TransferEnginePy::freeManagedBuffer(uintptr_t buffer_addr, size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto buffer = (char*)buffer_addr;
    int class_id = findClassId(length);
    if (class_id < 0) {
        large_buffer_list_.erase(buffer);
        engine_->unregisterLocalMemory(buffer);
        freeMemory(buffer);
        return 0;
    }
    free_list_[class_id].push(buffer);
    return 0;
}

static int parseTransportHint(const std::string& name) {
#ifdef USE_TENT
    if (name.empty()) return mooncake::tent::UNSPEC;
    auto type = mooncake::tent::parseTransportType(name);
    if (type == mooncake::tent::UNSPEC && name != "unspec") {
        throw std::invalid_argument(
            "Unknown transport_hint '" + name +
            "' (valid: rdma, mnnvl, shm, nvlink, gds, io_uring, tcp, "
            "ascend, sunrise_link, unspec, or empty string for no hint)");
    }
    return type;
#else
    return 0;
#endif
}

int TransferEnginePy::transferSyncWrite(const char* target_hostname,
                                        uintptr_t buffer,
                                        uintptr_t peer_buffer_address,
                                        size_t length,
                                        const std::string& transport_hint) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::WRITE, nullptr, transport_hint);
}

int TransferEnginePy::transferSyncRead(const char* target_hostname,
                                       uintptr_t buffer,
                                       uintptr_t peer_buffer_address,
                                       size_t length,
                                       const std::string& transport_hint) {
    return transferSync(target_hostname, buffer, peer_buffer_address, length,
                        TransferOpcode::READ, nullptr, transport_hint);
}

int TransferEnginePy::batchTransferSyncWrite(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths,
    const std::string& transport_hint) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::WRITE, nullptr,
                             transport_hint);
}

int TransferEnginePy::batchTransferSyncRead(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths,
    const std::string& transport_hint) {
    return batchTransferSync(target_hostname, buffers, peer_buffer_addresses,
                             lengths, TransferOpcode::READ, nullptr,
                             transport_hint);
}

batch_id_t TransferEnginePy::batchTransferAsyncWrite(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, const std::string& transport_hint) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::WRITE, transport_hint);
}

batch_id_t TransferEnginePy::batchTransferAsyncRead(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, const std::string& transport_hint) {
    return batchTransferAsync(target_hostname, buffers, peer_buffer_addresses,
                              lengths, TransferOpcode::READ, transport_hint);
}

int TransferEnginePy::transferSync(const char* target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address, size_t length,
                                   TransferOpcode opcode,
                                   TransferNotify* notify,
                                   const std::string& transport_hint) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            LOG(INFO)
                << "transferSync, cache not found, openSegment with target "
                << target_hostname;
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
    }

    // TODO this is just a workaround
    // When transfer engine submits one task, it will be dispatch to a worker
    // associated with one local RNIC. If the local RNIC fails to connect to any
    // remote RNIC, it will eventually fail. This allows selecting multiple
    // local RNIC in one transferSync call. Will be fixed in the next revision.
    const int max_retry =
        engine_->numContexts() + 1;  // Iter all possible local contexts
    auto start_ts = getCurrentTimeInNano();
    for (int retry = 0; retry < max_retry; ++retry) {
        auto batch_id = engine_->allocateBatchID(1);
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = length;
        entry.source = (void*)buffer;
        entry.target_id = handle;
        entry.target_offset = peer_buffer_address;
        entry.advise_retry_cnt = retry;
        entry.transport_hint = parseTransportHint(transport_hint);

        Status s =
            notify
                ? engine_->submitTransferWithNotify(
                      batch_id, {entry},
                      TransferMetadata::NotifyDesc{notify->name, notify->msg})
                : engine_->submitTransfer(batch_id, {entry});
        if (!s.ok()) {
            Status segment_status = engine_->CheckSegmentStatus(handle);
            if (!segment_status.ok()) {
                LOG(WARNING)
                    << "submitTransfer failed with target " << target_hostname
                    << ", CheckSegmentStatus not ok, ready to closeSegment";
                std::lock_guard<std::mutex> guard(mutex_);
                engine_->closeSegment(handle);
                engine_->getMetadata()->removeSegmentDesc(target_hostname);
                handle_map_.erase(target_hostname);
            }
            return -1;
        }

        TransferStatus status;
        bool completed = false;
        while (!completed) {
            Status s = engine_->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(batch_id);
                return 0;
            } else if (status.s == TransferStatusEnum::FAILED) {
                engine_->freeBatchID(batch_id);
                completed = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
            auto current_ts = getCurrentTimeInNano();
            const int64_t timeout =
                transfer_timeout_nsec_ + length;  // 1GiB per second
            if (current_ts - start_ts > timeout) {
                LOG(INFO) << "Sync data transfer timeout after "
                          << current_ts - start_ts << "ns, local buffer "
                          << (void*)buffer << " remote buffer "
                          << (void*)peer_buffer_address << " length " << length;
                return -1;
            }
        }
    }
    return -1;
}

int TransferEnginePy::batchTransferSync(
    const char* target_hostname, std::vector<uintptr_t> buffers,
    std::vector<uintptr_t> peer_buffer_addresses, std::vector<size_t> lengths,
    TransferOpcode opcode, TransferNotify* notify,
    const std::string& transport_hint) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1) return -1;
            handle_map_[target_hostname] = handle;
        }
    }

    if (buffers.size() != peer_buffer_addresses.size() ||
        buffers.size() != lengths.size()) {
        LOG(ERROR)
            << "buffers, peer_buffer_addresses and lengths have different size";
        return -1;
    }

    const int max_retry = engine_->numContexts() + 1;
    auto start_ts = getCurrentTimeInNano();
    auto total_length = std::accumulate(lengths.begin(), lengths.end(), 0ull);
    auto batch_size = buffers.size();
    std::vector<TransferRequest> entries;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = lengths[i];
        entry.source = (void*)buffers[i];
        entry.target_id = handle;
        entry.target_offset = peer_buffer_addresses[i];
        entry.advise_retry_cnt = 0;
        entry.transport_hint = parseTransportHint(transport_hint);
        entries.push_back(entry);
    }

    for (int retry = 0; retry < max_retry; ++retry) {
        auto batch_id = engine_->allocateBatchID(batch_size);
        Status s =
            notify
                ? engine_->submitTransferWithNotify(
                      batch_id, entries,
                      TransferMetadata::NotifyDesc{notify->name, notify->msg})
                : engine_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            engine_->freeBatchID(batch_id);
            Status segment_status = engine_->CheckSegmentStatus(handle);
            if (!segment_status.ok()) {
                LOG(WARNING)
                    << "submitTransfer failed with target " << target_hostname
                    << ", CheckSegmentStatus not ok, ready to closeSegment";
                std::lock_guard<std::mutex> guard(mutex_);
                engine_->closeSegment(handle);
                engine_->getMetadata()->removeSegmentDesc(target_hostname);
                handle_map_.erase(target_hostname);
            }
            return -1;
        }

        TransferStatus status;
        bool completed = false;
        bool already_freed = false;
        while (!completed) {
            Status s = engine_->getBatchTransferStatus(batch_id, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(batch_id);
                return 0;
            } else if (status.s == TransferStatusEnum::FAILED) {
                engine_->freeBatchID(batch_id);
                already_freed = true;
                completed = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
                completed = true;
            }
            auto current_ts = getCurrentTimeInNano();
            const int64_t timeout =
                transfer_timeout_nsec_ + total_length;  // 1GiB per second
            if (current_ts - start_ts > timeout) {
                LOG(INFO) << "Sync batch data transfer timeout after "
                          << current_ts - start_ts << "ns";
                // TODO: as @doujiang24 mentioned, early free(while there are
                // still waiting tasks) the batch_id may fail and cause memory
                // leak(a known issue).
                if (!already_freed) {
                    engine_->freeBatchID(batch_id);
                }
                return -1;
            }
        }
    }
    return -1;
}

batch_id_t TransferEnginePy::batchTransferAsync(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, TransferOpcode opcode,
    const std::string& transport_hint) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            // batch_id_t is unsigned; 0 is the documented failure sentinel
            // (-1 would surface as 2^64 - 1 in Python).
            if (handle == (Transport::SegmentHandle)-1) return 0;
            handle_map_[target_hostname] = handle;
        }
    }

    if (buffers.size() != peer_buffer_addresses.size() ||
        buffers.size() != lengths.size()) {
        LOG(ERROR)
            << "buffers, peer_buffer_addresses and lengths have different size";
        return 0;
    }

    const int max_retry = engine_->numContexts() + 1;
    auto batch_size = buffers.size();
    std::vector<TransferRequest> entries;
    batch_id_t batch_id = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        if (opcode == TransferOpcode::WRITE) {
            entry.opcode = TransferRequest::WRITE;
        } else {
            entry.opcode = TransferRequest::READ;
        }
        entry.length = lengths[i];
        entry.source = (void*)buffers[i];
        entry.target_id = handle;
        entry.target_offset = peer_buffer_addresses[i];
        entry.advise_retry_cnt = 0;
        entry.transport_hint = parseTransportHint(transport_hint);
        entries.push_back(entry);
    }

    for (int retry = 0; retry < max_retry; ++retry) {
        batch_id = engine_->allocateBatchID(batch_size);
        auto batch_desc = reinterpret_cast<BatchDesc*>(batch_id);

        auto start_ts = getCurrentTimeInNano();
        batch_desc->start_timestamp = start_ts;

        Status s = engine_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            engine_->freeBatchID(batch_id);
            return 0;
        } else {
            break;
        }
    }

    return batch_id;
}

int TransferEnginePy::getBatchTransferStatus(
    const std::vector<batch_id_t>& batch_ids) {
    pybind11::gil_scoped_release release;
    TransferStatus status;
    std::unordered_map<batch_id_t, int64_t> timeout_table{};
    for (auto& batch_id : batch_ids) {
        int64_t total_length = 0;
        auto batch_desc = reinterpret_cast<BatchDesc*>(batch_id);
        const size_t task_count = batch_desc->task_list.size();

        for (size_t task_id = 0; task_id < task_count; task_id++) {
            auto& task = batch_desc->task_list[task_id];
            for (auto& slice : task.slice_list) {
                total_length += slice->length;
            }
        }

        timeout_table[batch_id] = total_length + transfer_timeout_nsec_;
    }

    bool failed_or_timeout = false;
    std::unordered_set<batch_id_t> remove_ids{};
    while (!timeout_table.empty() && !failed_or_timeout) {
        for (auto& entry : timeout_table) {
            auto batch_desc = reinterpret_cast<BatchDesc*>(entry.first);
            auto start_timestamp = batch_desc->start_timestamp;
            Status s = engine_->getBatchTransferStatus(entry.first, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(entry.first);
                LOG(INFO) << "Batch Transfer completed!";
                remove_ids.insert(entry.first);
            } else if (status.s == TransferStatusEnum::FAILED) {
                failed_or_timeout = true;
            } else if (status.s == TransferStatusEnum::TIMEOUT) {
                LOG(INFO) << "Sync data transfer timeout";
            }
            auto current_ts = getCurrentTimeInNano();
            if (current_ts - start_timestamp > entry.second) {
                LOG(INFO) << "Sync batch data transfer timeout after "
                          << current_ts - start_timestamp << "ns";
                failed_or_timeout = true;
            }
        }

        for (auto& remove_id : remove_ids) {
            timeout_table.erase(remove_id);
        }

        remove_ids.clear();
    }

    if (failed_or_timeout) {
        for (auto& entry : timeout_table) {
            engine_->freeBatchID(entry.first);
        }
    }

    return failed_or_timeout ? -1 : 0;
}

batch_id_t TransferEnginePy::transferSubmitWrite(
    const char* target_hostname, uintptr_t buffer,
    uintptr_t peer_buffer_address, size_t length,
    const std::string& transport_hint) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            // batch_id_t is unsigned; 0 is the documented failure sentinel
            // (-1 would surface as 2^64 - 1 in Python).
            if (handle == (Transport::SegmentHandle)-1) return 0;
            handle_map_[target_hostname] = handle;
        }
    }

    auto batch_id = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = length;
    entry.source = (void*)buffer;
    entry.target_id = handle;
    entry.target_offset = peer_buffer_address;
    entry.transport_hint = parseTransportHint(transport_hint);

    Status s = engine_->submitTransfer(batch_id, {entry});
    if (!s.ok()) {
        engine_->freeBatchID(batch_id);
        return 0;
    }

    return batch_id;
}

int TransferEnginePy::transferCheckStatus(batch_id_t batch_id) {
    pybind11::gil_scoped_release release;
    TransferStatus status;
    Status s = engine_->getTransferStatus(batch_id, 0, status);
    LOG_ASSERT(s.ok());
    if (status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(batch_id);
        return 1;
    } else if (status.s == TransferStatusEnum::FAILED) {
        engine_->freeBatchID(batch_id);
        return -1;
    } else if (status.s == TransferStatusEnum::TIMEOUT) {
        return -2;
    } else {
        return 0;
    }
}

int TransferEnginePy::batchRegisterMemory(
    std::vector<uintptr_t> buffer_addresses, std::vector<size_t> capacities,
    const std::string& location) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<BufferEntry> buffers;
    for (size_t i = 0; i < batch_size; i++) {
        buffers.push_back(
            BufferEntry{(void*)buffer_addresses[i], capacities[i]});
    }
    return engine_->registerLocalMemoryBatch(buffers, location);
}

int TransferEnginePy::batchUnregisterMemory(
    std::vector<uintptr_t> buffer_addresses) {
    pybind11::gil_scoped_release release;
    auto batch_size = buffer_addresses.size();
    std::vector<void*> buffers;
    for (size_t i = 0; i < batch_size; i++) {
        buffers.push_back(reinterpret_cast<char*>(buffer_addresses[i]));
    }
    return engine_->unregisterLocalMemoryBatch(buffers);
}

int TransferEnginePy::registerMemory(uintptr_t buffer_addr, size_t capacity,
                                     const std::string& location) {
    char* buffer = reinterpret_cast<char*>(buffer_addr);
    return engine_->registerLocalMemory(buffer, capacity, location);
}

int TransferEnginePy::unregisterMemory(uintptr_t buffer_addr) {
    char* buffer = reinterpret_cast<char*>(buffer_addr);
    return engine_->unregisterLocalMemory(buffer);
}

#ifdef USE_CUDA

/**
 * @brief Context structure for CUDA-stream-synchronized transfers.
 *
 * This structure holds all necessary data to execute a Mooncake transfer
 * from within a CUDA host callback.
 */
struct TransferOnCudaContext {
    std::shared_ptr<TransferEngine> engine;
    Transport::BatchID batch_id;
    std::vector<Transport::TransferRequest> requests;
    uint64_t total_bytes;
};

/**
 * @brief CUDA Host Callback function for triggered transfers.
 *
 * This function is called by the CUDA driver when all preceding operations
 * in the associated stream have completed. It submits the transfer requests
 * and waits synchronously for their completion.
 *
 * @param data Pointer to a TransferOnCudaContext object.
 */
void CUDART_CB transfer_on_cuda_callback(void* data) {
    auto* ctx = reinterpret_cast<TransferOnCudaContext*>(data);

    auto status = ctx->engine->submitTransfer(ctx->batch_id, ctx->requests);
    if (!status.ok()) {
        LOG(ERROR) << "[Mooncake Cuda] Submit failed: " << status.ToString()
                   << " | BatchID: " << ctx->batch_id;
        goto error_exit;
    }

    Transport::TransferStatus t_status;
    while (true) {
        auto ret = ctx->engine->getBatchTransferStatus(ctx->batch_id, t_status);
        if (!ret.ok()) {
            LOG(ERROR) << "[Mooncake Cuda] Failed to get status for BatchID: "
                       << ctx->batch_id;
            goto error_exit;
        }

        if (t_status.s == Transport::TransferStatusEnum::COMPLETED) {
            break;
        } else if (t_status.s == Transport::TransferStatusEnum::FAILED) {
            LOG(ERROR) << "[Mooncake Cuda] Transfer failed | BatchID: "
                       << ctx->batch_id << " | Bytes: " << ctx->total_bytes;
            goto error_exit;
        } else if (t_status.s == Transport::TransferStatusEnum::TIMEOUT) {
            LOG(ERROR) << "[Mooncake Cuda] Transfer timeout | BatchID: "
                       << ctx->batch_id;
            goto error_exit;
        }
    }

    ctx->engine->freeBatchID(ctx->batch_id);
    delete ctx;
    return;

error_exit:
    // Since this is a CUDA host callback running in a driver thread,
    // we cannot propagate exceptions or error codes back to the main
    // application. A failure here implies the data transfer required for
    // subsequent stream operations has failed, leaving the system in an
    // inconsistent state. We use _exit(1) to terminate the process
    // immediately and avoid undefined behavior.
    _exit(1);
}

/**
 * @brief Submits a batch of transfer requests synchronized with a CUDA stream.
 *
 * This method schedules a host callback on the provided CUDA stream. The
 * Mooncake transfer will only start after all previous kernels/memcpys on
 * the stream have finished.
 *
 * @param target_hostname Remote host to transfer to/from.
 * @param buffers Local buffer addresses.
 * @param peer_buffer_addresses Remote buffer addresses.
 * @param lengths Length of each transfer in bytes.
 * @param opcode READ or WRITE operation.
 * @param stream_ptr Handle to a CUDA stream (cudaStream_t as uintptr_t).
 */
void TransferEnginePy::batchTransferOnCuda(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, TransferOpcode opcode,
    uintptr_t stream_ptr, const std::string& transport_hint) {
    pybind11::gil_scoped_release release;
    Transport::SegmentHandle handle;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (handle_map_.count(target_hostname)) {
            handle = handle_map_[target_hostname];
        } else {
            handle = engine_->openSegment(target_hostname);
            if (handle == (Transport::SegmentHandle)-1)
                throw std::runtime_error("Failed to open segment");
            handle_map_[target_hostname] = handle;
        }
    }

    if (buffers.size() != peer_buffer_addresses.size() ||
        buffers.size() != lengths.size()) {
        LOG(ERROR)
            << "buffers, peer_buffer_addresses and lengths have different size";
        throw std::runtime_error(
            "buffers, peer_buffer_addresses and lengths have different size");
    }

    size_t batch_size = buffers.size();
    std::vector<TransferRequest> entries;
    uint64_t total_bytes = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        entry.opcode = (opcode == TransferOpcode::WRITE)
                           ? TransferRequest::WRITE
                           : TransferRequest::READ;
        entry.length = lengths[i];
        entry.source = (void*)buffers[i];
        entry.target_id = handle;
        entry.target_offset = peer_buffer_addresses[i];
        entry.transport_hint = parseTransportHint(transport_hint);
        entries.push_back(entry);
        total_bytes += lengths[i];
    }

    auto batch_id = engine_->allocateBatchID(batch_size);
    auto* ctx = new TransferOnCudaContext{engine_, batch_id, std::move(entries),
                                          total_bytes};

    cudaStream_t stream = reinterpret_cast<cudaStream_t>(stream_ptr);
    cudaError_t err =
        cudaLaunchHostFunc(stream, transfer_on_cuda_callback, ctx);
    if (err != cudaSuccess) {
        delete ctx;
        engine_->freeBatchID(batch_id);
        throw std::runtime_error(std::string("cudaLaunchHostFunc failed: ") +
                                 cudaGetErrorString(err));
    }
}

/**
 * @brief Async WRITE transfer triggered by a CUDA stream.
 */
void TransferEnginePy::transferWriteOnCuda(const char* target_hostname,
                                           uintptr_t buffer,
                                           uintptr_t peer_buffer_address,
                                           size_t length, uintptr_t stream_ptr,
                                           const std::string& transport_hint) {
    batchTransferOnCuda(target_hostname, {buffer}, {peer_buffer_address},
                        {length}, TransferOpcode::WRITE, stream_ptr,
                        transport_hint);
}

/**
 * @brief Async READ transfer triggered by a CUDA stream.
 */
void TransferEnginePy::transferReadOnCuda(const char* target_hostname,
                                          uintptr_t buffer,
                                          uintptr_t peer_buffer_address,
                                          size_t length, uintptr_t stream_ptr,
                                          const std::string& transport_hint) {
    batchTransferOnCuda(target_hostname, {buffer}, {peer_buffer_address},
                        {length}, TransferOpcode::READ, stream_ptr,
                        transport_hint);
}

/**
 * @brief Batch async WRITE transfer triggered by a CUDA stream.
 */
void TransferEnginePy::batchTransferWriteOnCuda(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, uintptr_t stream_ptr,
    const std::string& transport_hint) {
    batchTransferOnCuda(target_hostname, buffers, peer_buffer_addresses,
                        lengths, TransferOpcode::WRITE, stream_ptr,
                        transport_hint);
}

/**
 * @brief Batch async READ transfer triggered by a CUDA stream.
 */
void TransferEnginePy::batchTransferReadOnCuda(
    const char* target_hostname, const std::vector<uintptr_t>& buffers,
    const std::vector<uintptr_t>& peer_buffer_addresses,
    const std::vector<size_t>& lengths, uintptr_t stream_ptr,
    const std::string& transport_hint) {
    batchTransferOnCuda(target_hostname, buffers, peer_buffer_addresses,
                        lengths, TransferOpcode::READ, stream_ptr,
                        transport_hint);
}
#endif

int TransferEnginePy::warmupEfaSegment(const std::string& segment_name) {
#ifdef USE_EFA
    pybind11::gil_scoped_release release;
    auto* t = engine_->getTransport("efa");
    if (!t) return 0;  // EFA transport not installed; nothing to do.
    auto* efa = dynamic_cast<EfaTransport*>(t);
    if (!efa) return 0;
    return efa->warmupSegment(segment_name);
#else
    (void)segment_name;
    return 0;
#endif
}

uintptr_t TransferEnginePy::getFirstBufferAddress(
    const std::string& segment_name) {
    Transport::SegmentHandle segment_id =
        engine_->openSegment(segment_name.c_str());
    auto segment_desc = engine_->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc || segment_desc->buffers.empty()) {
        return 0;
    }
    return segment_desc->buffers[0].addr;
}

std::string TransferEnginePy::getLocalTopology(const char* device_name) {
    pybind11::gil_scoped_release release;
    auto device_name_safe = device_name ? std::string(device_name) : "";
    auto device_filter = buildDeviceFilter(device_name_safe);
    std::shared_ptr<TransferEngine> tmp_engine =
        std::make_shared<TransferEngine>(true, device_filter);

    std::string metadata_conn_string{"P2PHANDSHAKE"}, local_server_name{};
    tmp_engine->init(metadata_conn_string, local_server_name);

    return tmp_engine->getLocalTopology()->toString();
}

std::vector<TransferEnginePy::TransferNotify> TransferEnginePy::getNotifies() {
    std::vector<TransferMetadata::NotifyDesc> notifies;
    std::vector<TransferNotify> result;

    int ret = engine_->getNotifies(notifies);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get notifies: " << ret;
        return result;
    }

    for (const auto& notify : notifies) {
        result.emplace_back(
            TransferEnginePy::TransferNotify{notify.name, notify.notify_msg});
    }

    return result;
}

int TransferEnginePy::sendProbe(const std::string& peer_server_name) {
    if (!engine_) return -1;
    pybind11::gil_scoped_release release;
    return engine_->getMetadata()->sendProbe(peer_server_name);
}

namespace py = pybind11;

// Implementation of coro_rpc_interface binding function
void bind_coro_rpc_interface(py::module_& m) {
    // Note: RpcInterface, ReceivedData and ReceivedTensor are already
    // registered by bind_rpc_interface() so we don't register them again here
    // to avoid duplicate type registration errors. The factory functions are
    // also registered by bind_rpc_interface(), so we don't need to register
    // them again.

    // Add CoroRPCInterface as an alias to RpcInterface
    m.attr("CoroRPCInterface") = m.attr("RpcInterface");
}

// The same address/cardinality checks run in portable native relay tests.
namespace {
using WritePhases = std::array<int64_t, 3>;
using Clock = std::chrono::steady_clock;

int64_t elapsedNs(Clock::time_point begin, Clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
        .count();
}

RdmaTransport& relayRdma(const std::shared_ptr<TransferEngine>& engine) {
    auto* rdma = engine && !engine->isUsingTent()
                     ? engine->getTransport("rdma")
                     : nullptr;
    if (!rdma) throw std::runtime_error("relay requires legacy RDMA");
    return *static_cast<RdmaTransport*>(rdma);
}

void relayWriteBatch(TransferEnginePy& self,
                     const std::vector<std::string>& segments,
                     const std::vector<uintptr_t>& sources,
                     const std::vector<uintptr_t>& destinations,
                     const std::vector<size_t>& lengths,
                     WritePhases* phases = nullptr) {
    const auto begin = phases ? Clock::now() : Clock::time_point{};
    mooncake::pool_relay::ValidateWriteBatch(segments, sources, destinations,
                                            lengths);
    auto engine = self.getEngine();
    relayRdma(engine);
    for (const auto& segment : segments) {
        auto handle = engine->openSegment(segment);
        if (handle == static_cast<Transport::SegmentHandle>(-1))
            throw std::runtime_error("cannot open relay segment");
        auto desc = engine->getMetadata()->getSegmentDescByID(handle);
        engine->closeSegment(handle);
        if (!desc || desc->protocol != "rdma")
            throw std::runtime_error("relay refuses non-RDMA target");
    }
    const size_t zero = 0;
    std::vector<TransferEngine::ScatterTransferRange> ranges;
    ranges.reserve(segments.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        ranges.push_back({
            .opcode = TransferRequest::WRITE,
            .remote_segment = segments[i],
            .remote_base_offset = destinations[i],
            .remote_size = lengths[i],
            .local_buffer = reinterpret_cast<void*>(sources[i]),
            .local_capacity = lengths[i],
            .local_offsets = std::span<const size_t>(&zero, 1),
            .remote_offsets = std::span<const size_t>(&zero, 1),
            .lengths = std::span<const size_t>(&lengths[i], 1),
            .on_fragment_complete = {}});
    }
    // The operation destructor also waits. No diagnostic deadline frees MRs.
    const auto submit = phases ? Clock::now() : Clock::time_point{};
    auto operation = engine->submitScatter(ranges);
    const auto wait = phases ? Clock::now() : Clock::time_point{};
    auto status = operation.wait();
    if (phases)
        *phases = {elapsedNs(begin, submit), elapsedNs(submit, wait),
                   elapsedNs(wait, Clock::now())};
    if (!status.ok()) throw std::runtime_error(status.ToString());
}
}  // namespace

PYBIND11_MODULE(engine, m) {
#ifdef USE_EFA
    m.attr("SUPPORT_EFA") = true;
#else
    m.attr("SUPPORT_EFA") = false;
#endif

#ifdef USE_HIP
    m.attr("SUPPORT_HIP") = true;
#else
    m.attr("SUPPORT_HIP") = false;
#endif

#ifdef USE_MNNVL
    m.attr("SUPPORT_MNNVL") = true;
#else
    m.attr("SUPPORT_MNNVL") = false;
#endif

#ifdef USE_INTRA_NVLINK
    m.attr("SUPPORT_INTRA_NVLINK") = true;
#else
    m.attr("SUPPORT_INTRA_NVLINK") = false;
#endif

#ifdef USE_CUDA
    m.attr("SUPPORT_CUDA") = true;
#else
    m.attr("SUPPORT_CUDA") = false;
#endif

    py::enum_<TransferEnginePy::TransferOpcode> transfer_opcode(
        m, "TransferOpcode", py::arithmetic());
    transfer_opcode.value("Read", TransferEnginePy::TransferOpcode::READ)
        .value("Write", TransferEnginePy::TransferOpcode::WRITE)
        .export_values();

    py::class_<TransferEnginePy::TransferNotify>(m, "TransferNotify")
        .def(py::init<>())
        .def(py::init<const std::string&, const std::string&>(),
             py::arg("name"), py::arg("msg"))
        .def_readwrite("name", &TransferEnginePy::TransferNotify::name)
        .def_readwrite("msg", &TransferEnginePy::TransferNotify::msg);

    auto adaptor_cls =
        py::class_<TransferEnginePy>(m, "TransferEngine")
            .def(py::init<>())
            .def("initialize", &TransferEnginePy::initialize)
            .def("initialize_ext", &TransferEnginePy::initializeExt)
            .def("get_rpc_port", &TransferEnginePy::getRpcPort)
            .def("relay_abi_version", [](TransferEnginePy&) { return 2; })
            .def("relay_clock_probe", [](TransferEnginePy&) {
                namespace wp=mooncake::write_pipeline;
                constexpr size_t count=4096;
                uint64_t total=0,minimum=UINT64_MAX;
                for(size_t i=0;i<count;++i) {
                    const auto start=wp::monotonicNowNs();
                    const auto duration=wp::monotonicNowNs()-start;
                    total+=duration;minimum=std::min(minimum,duration);
                }
                py::dict out;
                out["clock"]="CLOCK_MONOTONIC";
                out["samples"]=count;
                out["consecutive_read_delta_mean_ns"]=double(total)/count;
                out["consecutive_read_delta_min_ns"]=minimum;
#ifdef __linux__
                out["vdso_mapping_present"]=getauxval(AT_SYSINFO_EHDR)!=0;
                timespec value{};
                const auto start=wp::monotonicNowNs();
                for(size_t i=0;i<count;++i) syscall(SYS_clock_gettime,CLOCK_MONOTONIC,&value);
                out["forced_syscall_loop_mean_ns"]=double(wp::monotonicNowNs()-start)/count;
#endif
                out["interpretation"]="Probe outside transfer; includes observation/loop overhead. vDSO mapping is not proof libc used it.";
                return out;
            })
            .def("relay_write_pipeline",
                 [](TransferEnginePy& self, const std::string& segment,
                    const std::vector<uintptr_t>& sources,
                    const std::vector<uintptr_t>& destinations,
                    const std::vector<size_t>& lengths,
                    const std::vector<uint64_t>& source_offsets,
                    const std::vector<uint64_t>& destination_offsets,
                    const std::string& source_backing, const std::string& destination_backing,
                    const std::string& mode, size_t depth, uint64_t byte_limit,
                    size_t descriptors, const std::string& instrumentation,
                    py::object owner, py::object ready, py::object drained) {
                     namespace wp = mooncake::write_pipeline;
                     if (sources.empty() || sources.size()>100000 ||
                         destinations.size()!=sources.size() || lengths.size()!=sources.size() ||
                         source_offsets.size()!=sources.size() || destination_offsets.size()!=sources.size() ||
                         owner.is_none() || (mode!="serialization" && mode!="batched") ||
                         (instrumentation!="off" && instrumentation!="counters" && instrumentation!="sampled"))
                         throw std::invalid_argument("invalid native WRITE pipeline schedule/options/owner");
                     auto engine = self.getEngine();
                     relayRdma(engine);
                     auto handle = engine->openSegment(segment);
                     auto target = engine->getMetadata()->getSegmentDescByID(handle);
                     engine->closeSegment(handle);
                     if (!target || target->protocol!="rdma")
                         throw std::runtime_error("pipeline refuses non-RDMA target");
                     // Both Python backing owner and all operation handles remain
                     // alive until the entire native call drains, including errors.
                     auto lifetime = std::make_shared<py::object>(std::move(owner));
                     std::vector<wp::Request> requests;
                     requests.reserve(sources.size());
                     for (size_t i=0; i<sources.size(); ++i)
                         requests.push_back({i,sources[i],destinations[i],lengths[i],segment,
                             {source_backing,source_offsets[i],lengths[i]},
                             {destination_backing,destination_offsets[i],lengths[i]},lifetime});
                     wp::Options options;
                     options.max_operations = mode=="serialization" ? 1 : depth;
                     options.max_bytes = byte_limit;
                     options.descriptors_per_operation = mode=="serialization" ? 1 : descriptors;
                     options.instrumentation = instrumentation=="off" ? wp::Instrumentation::Off :
                         instrumentation=="counters" ? wp::Instrumentation::Counters : wp::Instrumentation::Sampled;
                     wp::Result result;
                     {
                         py::gil_scoped_release release;
                         result = wp::run(*engine,requests,options,{},[&] {
                             if (!ready.is_none()) {
                                 py::gil_scoped_acquire acquire;
                                 ready();
                             }
                         });
                         if (!drained.is_none()) {
                             py::gil_scoped_acquire acquire;
                             drained();
                         }
                     }
                     py::dict out;
                     out["success"] = result.success;
                     out["bytes"] = result.bytes;
                     out["duration_ns"] = result.duration_ns;
                     out["start_ns"] = result.start_ns;
                     out["process_cpu_ns"] = result.process_cpu_ns;
                     out["operations"] = result.operations;
                     out["peak_operations"] = result.peak_operations;
                     out["peak_bytes"] = result.peak_bytes;
                     out["poll_passes"] = result.poll_passes;
                     out["idle_passes"] = result.idle_passes;
                     out["depth_wait_passes"] = result.depth_wait_passes;
                     out["byte_wait_passes"] = result.byte_wait_passes;
                     out["conflict_wait_passes"] = result.conflict_wait_passes;
                     out["admission_blocked_ns"] = result.admission_blocked_ns;
                     out["submission_ns"] = result.submission_ns;
                     out["completion_scan_ns"] = result.completion_scan_ns;
                     out["trace_overflow"] = result.trace_overflow;
                     out["phase_timing_available"] = instrumentation!="off";
                     py::list samples,trace,admission;
                     for (const auto& s : result.samples)
                         samples.append(py::make_tuple(s.request,s.operation,s.begin_ns,s.end_ns,s.success));
                     for (const auto& e : result.trace)
                         trace.append(py::make_tuple(e.ns,e.operation,e.request,e.kind));
                     for (const auto& s : result.samples)
                         admission.append(py::make_tuple(s.request,s.admission_wait_ns));
                     out["samples"] = samples;
                     out["trace"] = trace;
                     out["admission_samples"] = instrumentation=="off" ? py::object(py::none()) : py::object(admission);
                     return out;
                 }, py::arg("segment"),py::arg("sources"),py::arg("destinations"),py::arg("lengths"),
                    py::arg("source_offsets"),py::arg("destination_offsets"),py::arg("source_backing"),
                    py::arg("destination_backing"),py::arg("mode"),py::arg("depth"),py::arg("byte_limit"),
                    py::arg("descriptors"),py::arg("instrumentation"),py::arg("owner"),
                    py::arg("ready")=py::none(),py::arg("drained")=py::none())
            .def("relay_write_batch_drained",
                 [](TransferEnginePy& self,
                    const std::vector<std::string>& segments,
                    const std::vector<uintptr_t>& sources,
                    const std::vector<uintptr_t>& destinations,
                    const std::vector<size_t>& lengths) {
                     py::gil_scoped_release release;
                     relayWriteBatch(self, segments, sources, destinations, lengths);
                     return lengths;
                 }, py::arg("segments"), py::arg("sources"),
                 py::arg("destinations"), py::arg("lengths"))
            .def("relay_write_block_profiled",
                 [](TransferEnginePy& self, const std::string& segment,
                    uintptr_t src, uintptr_t dst, size_t length) {
                     py::gil_scoped_release release;
                     WritePhases phases{};
                     relayWriteBatch(self, {segment}, {src}, {dst}, {length},
                                     &phases);
                     return phases;
                 })
            .def("relay_write_blocks_diagnostic",
                 [](TransferEnginePy& self, const std::string& segment,
                    const std::vector<uintptr_t>& src,
                    const std::vector<uintptr_t>& dst,
                    const std::vector<size_t>& lengths) {
                     if (src.empty() || src.size() > 100000 ||
                         src.size() != dst.size() || src.size() != lengths.size())
                         throw std::invalid_argument("invalid diagnostic WRITE schedule");
                     py::gil_scoped_release release;
                     // Same per-block validation/submission/drain as the normal
                     // binding. Only the loop and Python crossing change.
                     const std::vector<std::string> segments{segment};
                     std::vector<uintptr_t> source(1), destination(1);
                     std::vector<size_t> length(1);
                     std::vector<std::array<int64_t, 5>> samples;
                     samples.reserve(src.size());
                     const auto start = Clock::now();
                     for (size_t i = 0; i < src.size(); ++i) {
                         source[0] = src[i];
                         destination[0] = dst[i];
                         length[0] = lengths[i];
                         WritePhases phases{};
                         const auto begin = Clock::now();
                         relayWriteBatch(self, segments, source, destination,
                                         length, &phases);
                         samples.push_back({elapsedNs(start, begin),
                             elapsedNs(start, Clock::now()), phases[0],
                             phases[1], phases[2]});
                     }
                     return samples;
                 })
            .def("relay_rdma_snapshot", [](TransferEnginePy& self, bool include_trace) {
                const auto& cfg = globalConfig();
                py::list rows;
                for (const auto& ctx : relayRdma(self.getEngine()).getContextList()) {
                    auto diag = ctx->snapshotWrites();
                    py::dict row;
                    row["device"] = ctx->deviceName();
                    row["port"] = ctx->portNum();
                    row["gid_index"] = ctx->gidIndex();
                    row["active_mtu_enum"] = static_cast<int>(ctx->activeMTU());
                    row["slice_bytes"] = cfg.slice_size;
                    row["qp_count"] = ctx->getTotalQPNumber();
                    row["configured_qps_per_endpoint"] = cfg.num_qp_per_ep;
                    row["configured_wr_depth"] = cfg.max_wr;
                    row["configured_cq_capacity"] = cfg.max_cqe;
                    row["cq_count"] = ctx->cqCount();
                    row["workers"] = cfg.workers_per_ctx;
                    row["tracked"] = cfg.track_rdma_posted_slices;
                    row["pending_write_wrs"] = diag.pending_wrs;
                    row["pending_write_bytes"] = diag.pending_bytes;
                    row["completed_write_bytes"] = diag.completed_bytes;
                    row["write_outstanding_ns"] = diag.outstanding_ns;
                    row["qp_completed_bytes"] = py::cast(diag.qp_completed_bytes);
                    row["qp_counters_truncated"] = diag.qp_counters_truncated;
                    row["diagnostic_version"] = 2;
                    row["slice_trace_enabled"] = cfg.sample_rdma_write_trace && cfg.track_rdma_posted_slices;
                    if (include_trace) {
                        auto [events,overflow]=ctx->snapshotWriteTrace();
                        py::list trace;
                        for(const auto& e:events)
                            trace.append(py::make_tuple(e.ns,e.request,e.slice_offset,e.bytes,e.qp,e.kind));
                        row["slice_trace"] = trace;
                        row["slice_trace_overflow"] = overflow;
                    }
                    row["all_wrs_signaled"] = true;
                    row["peak_pending_write_wrs"] = diag.peak_pending_wrs;
                    row["peak_pending_write_bytes"] = diag.peak_pending_bytes;
                    py::dict qps;
                    py::dict qp_mapping;
                    py::dict qp_outstanding;
                    for (const auto& [number, q] : diag.qp_statistics) {
                        py::dict counts;
                        counts["post_calls"] = q.post_calls;
                        counts["post_ns"] = q.post_ns;
                        counts["posted_wrs"] = q.posted_wrs;
                        counts["posted_bytes"] = q.posted_bytes;
                        counts["rejected_wrs"] = q.rejected_wrs;
                        counts["unknown_wrs"] = q.unknown_wrs;
                        counts["completed_wrs"] = q.completed_wrs;
                        counts["completed_bytes"] = q.completed_bytes;
                        counts["completion_errors"] = q.completion_errors;
                        qps[py::int_(number)] = counts;
                        py::dict mapping;
                        mapping["posting_tid"] = q.posting_tid;
                        mapping["completion_worker_index"] = q.completion_worker;
                        mapping["peer_nic"] = q.peer_nic;
                        qp_mapping[py::int_(number)] = mapping;
                        py::dict pending;
                        pending["pending_wrs"] = q.pending_wrs;
                        pending["pending_bytes"] = q.pending_bytes;
                        pending["lifetime_peak_wrs"] = q.peak_wrs;
                        pending["lifetime_peak_bytes"] = q.peak_bytes;
                        qp_outstanding[py::int_(number)] = pending;
                    }
                    row["qp_statistics"] = qps;
                    row["qp_mapping"] = qp_mapping;
                    row["qp_outstanding"] = qp_outstanding;
                    py::dict pollers;
                    for (const auto& [worker, p] : diag.pollers) {
                        py::dict counts;
                        counts["polls"] = p.polls;
                        counts["empty_polls"] = p.empty_polls;
                        counts["errors"] = p.errors;
                        counts["cqes"] = p.cqes;
                        counts["poll_ns"] = p.poll_ns;
                        pollers[py::int_(worker)] = counts;
                    }
                    row["poll_workers"] = pollers;
                    const auto mr = ctx->snapshotRegistration();
                    py::dict registrations;
                    registrations["calls"] = mr[0];
                    registrations["successful_calls"] = mr[1];
                    registrations["registered_bytes"] = mr[2];
                    registrations["register_ns"] = mr[3];
                    registrations["deregister_calls"] = mr[4];
                    registrations["deregister_successful_calls"] = mr[5];
                    registrations["deregister_ns"] = mr[6];
                    row["registration"] = registrations;
                    rows.append(row);
                }
                return rows;
            },py::arg("include_trace")=false)
            .def("relay_rdma_available",
                 [](TransferEnginePy& self) {
                     auto engine = self.getEngine();
                     return engine && !engine->isUsingTent() &&
                            engine->getTransport("rdma") != nullptr;
                 })
            .def(
                "relay_write_drained",
                [](TransferEnginePy& self, const std::string& segment,
                   uintptr_t source, uintptr_t destination, size_t length) {
                    // Dedicated relay gate: no deadline return while DMA is
                    // live. Do not use transferSync's timeout semantics for
                    // leased MRs.
                    py::gil_scoped_release release;
                    auto engine = self.getEngine();
                    if (!engine || engine->isUsingTent() ||
                        !engine->getTransport("rdma") || length == 0)
                        throw std::runtime_error(
                            "relay requires legacy RDMA engine and nonzero "
                            "length");
                    auto handle = engine->openSegment(segment);
                    if (handle == static_cast<Transport::SegmentHandle>(-1))
                        throw std::runtime_error(
                            "cannot open relay receiver segment");
                    auto desc =
                        engine->getMetadata()->getSegmentDescByID(handle);
                    engine->closeSegment(handle);
                    if (!desc || desc->protocol != "rdma")
                        throw std::runtime_error(
                            "relay refuses non-RDMA target");
                    const size_t zero = 0;
                    TransferEngine::ScatterTransferRange range{
                        .opcode = TransferRequest::WRITE,
                        .remote_segment = segment,
                        .remote_base_offset = destination,
                        .remote_size = length,
                        .local_buffer = reinterpret_cast<void*>(source),
                        .local_capacity = length,
                        .local_offsets = std::span<const size_t>(&zero, 1),
                        .remote_offsets = std::span<const size_t>(&zero, 1),
                        .lengths = std::span<const size_t>(&length, 1),
                        .on_fragment_complete = {}};
                    auto operation = engine->submitScatter({range});
                    auto status = operation.wait();
                    if (!status.ok())
                        throw std::runtime_error(status.ToString());
                    return length;
                },
                py::arg("segment"), py::arg("source"), py::arg("destination"),
                py::arg("length"))
            .def("allocate_managed_buffer",
                 &TransferEnginePy::allocateManagedBuffer)
            .def("free_managed_buffer", &TransferEnginePy::freeManagedBuffer)
            .def("transfer_sync_write", &TransferEnginePy::transferSyncWrite,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("transport_hint") = "")
            .def("transfer_sync_read", &TransferEnginePy::transferSyncRead,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("transport_hint") = "")
            .def("batch_transfer_sync_write",
                 &TransferEnginePy::batchTransferSyncWrite,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("transport_hint") = "")
            .def("batch_transfer_sync_read",
                 &TransferEnginePy::batchTransferSyncRead,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("transport_hint") = "")
            .def("batch_transfer_async_write",
                 &TransferEnginePy::batchTransferAsyncWrite,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("transport_hint") = "")
            .def("batch_transfer_async_read",
                 &TransferEnginePy::batchTransferAsyncRead,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("transport_hint") = "")
            .def("transfer_sync", &TransferEnginePy::transferSync,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("opcode"), py::arg("notify") = nullptr,
                 py::arg("transport_hint") = "")
            .def("batch_transfer_sync", &TransferEnginePy::batchTransferSync,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("opcode"), py::arg("notify") = nullptr,
                 py::arg("transport_hint") = "")
            .def("batch_transfer_async", &TransferEnginePy::batchTransferAsync,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("opcode"), py::arg("transport_hint") = "")
#ifdef USE_CUDA
            .def("transfer_write_on_cuda",
                 &TransferEnginePy::transferWriteOnCuda,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("stream_ptr") = 0, py::arg("transport_hint") = "")
            .def("transfer_read_on_cuda", &TransferEnginePy::transferReadOnCuda,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("stream_ptr") = 0, py::arg("transport_hint") = "")
            .def("batch_transfer_write_on_cuda",
                 &TransferEnginePy::batchTransferWriteOnCuda,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("stream_ptr") = 0, py::arg("transport_hint") = "")
            .def("batch_transfer_read_on_cuda",
                 &TransferEnginePy::batchTransferReadOnCuda,
                 py::arg("target_hostname"), py::arg("buffers"),
                 py::arg("peer_buffer_addresses"), py::arg("lengths"),
                 py::arg("stream_ptr") = 0, py::arg("transport_hint") = "")
#endif
            .def("get_batch_transfer_status",
                 &TransferEnginePy::getBatchTransferStatus)
            .def("transfer_submit_write",
                 &TransferEnginePy::transferSubmitWrite,
                 py::arg("target_hostname"), py::arg("buffer"),
                 py::arg("peer_buffer_address"), py::arg("length"),
                 py::arg("transport_hint") = "")
            .def("transfer_check_status",
                 &TransferEnginePy::transferCheckStatus)
            .def("write_bytes_to_buffer", &TransferEnginePy::writeBytesToBuffer)
            .def("read_bytes_from_buffer",
                 &TransferEnginePy::readBytesFromBuffer)
            .def("register_memory", &TransferEnginePy::registerMemory,
                 py::arg("buffer_addr"), py::arg("capacity"),
                 py::arg("location") = kWildcardLocation)
            .def("unregister_memory", &TransferEnginePy::unregisterMemory)
            .def("batch_register_memory",
                 &TransferEnginePy::batchRegisterMemory,
                 py::arg("buffer_addresses"), py::arg("capacities"),
                 py::arg("location") = kWildcardLocation)
            .def("batch_unregister_memory",
                 &TransferEnginePy::batchUnregisterMemory)
            .def("get_local_topology", &TransferEnginePy::getLocalTopology,
                 py::arg("device_name") = nullptr)
            .def("get_first_buffer_address",
                 &TransferEnginePy::getFirstBufferAddress)
            .def("warmup_efa_segment", &TransferEnginePy::warmupEfaSegment,
                 py::arg("segment_name"))
            .def("get_notifies", &TransferEnginePy::getNotifies)
            .def("send_probe", &TransferEnginePy::sendProbe,
                 py::arg("peer_server_name"),
                 "Send a JSON-RPC probe to peer to verify reachability. "
                 "Returns 0 on success, non-zero on failure. Used by "
                 "SGLang's failed-session blacklist recovery.")
            .def("get_engine", &TransferEnginePy::getEngine)
            .def("get_engine_ptr", &TransferEnginePy::getEnginePtr);

    adaptor_cls.attr("TransferOpcode") = transfer_opcode;

    py::class_<TransferEngine, std::shared_ptr<TransferEngine>>(
        m, "InnerTransferEngine");

    // Bind RpcInterface (this also registers ReceivedData, ReceivedTensor, and
    // factory functions)
    mooncake::bind_rpc_interface(m);

    // Add CoroRPCInterface as an alias to RpcInterface if needed
    bind_coro_rpc_interface(m);
}
