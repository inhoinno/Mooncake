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

#include "transport/cxl_transport/cxl_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include <cstring>

namespace mooncake {
namespace {

void LogCxlError(const char* event, const CxlPoolError& error) {
    LOG(ERROR) << "component=cxl_transport event=" << event
               << " error_code=" << ToString(error.code)
               << " operation=" << error.operation << " field=" << error.field
               << " system_error=" << error.system_error << " message=\""
               << error.message << "\"";
}

}  // namespace

CxlTransport::CxlTransport(std::shared_ptr<CxlPoolBackend> cxl_backend)
    : cxl_backend_(std::move(cxl_backend)) {}

CxlTransport::~CxlTransport() {
    if (metadata_ != nullptr && !local_server_name_.empty()) {
        metadata_->removeSegmentDesc(local_server_name_);
        metadata_->removeLocalSegment(local_server_name_);
    }
}

int CxlTransport::cxlMemcpy(void* dest, void* src, size_t size) {
    // Input validation
    if (!src || !dest) {
        LOG(ERROR) << "CxlTransport::cxlMemcpy invalid arguments: null pointer "
                      "provided.";
        return -1;  // null pointer
    }

    // Validate memory bounds using the helper function
    if (!validateMemoryBounds(dest, src, size)) {
        return -1;  // validation failed
    }

    // Perform the memory copy
    std::memcpy(dest, src, size);

    // Memory barriers and cache operations
    if (isAddressInCxlRange(dest) || isAddressInCxlRange(src)) {
        // Ensure memory ordering for CXL operations
        __sync_synchronize();
    }

    return 0;  // success
}

bool CxlTransport::validateMemoryBounds(void* dest, void* src, size_t size) {
    if (!cxl_backend_) return false;
    if (isAddressInCxlRange(dest) && !cxl_backend_->contains(dest, size)) {
        LOG(ERROR) << "component=cxl_transport event=copy_validation "
                      "field=destination error_code=out_of_bounds";
        return false;
    }
    if (isAddressInCxlRange(src) && !cxl_backend_->contains(src, size)) {
        LOG(ERROR) << "component=cxl_transport event=copy_validation "
                      "field=source error_code=out_of_bounds";
        return false;
    }
    return true;
}

bool CxlTransport::isAddressInCxlRange(void* addr) {
    if (!addr || !cxl_backend_ || !cxl_backend_->base()) return false;

    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_backend_->base());
    uintptr_t end = base + cxl_backend_->config().capacity;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);

    return (ptr >= base && ptr < end);
}

int CxlTransport::cxlDevInit() {
    if (cxl_backend_) {
        if (cxl_backend_->base() == nullptr ||
            cxl_backend_->config().capacity == 0) {
            LOG(ERROR) << "component=cxl_transport event=backend_injected "
                          "error_code=invalid_config";
            return ERR_MEMORY;
        }
        LOG(INFO) << cxl_backend_->status().ToJson();
        return 0;
    }

    CxlPoolError error;
    cxl_backend_ = OpenCxlPoolBackendFromEnvironment(&error);
    if (!cxl_backend_) {
        LogCxlError("backend_open_failed", error);
        return ERR_MEMORY;
    }
    LOG(INFO) << cxl_backend_->status().ToJson();
    return 0;
}

int CxlTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = cxlDevInit();
    if (ret) {
        LOG(ERROR) << "component=cxl_transport event=backend_init_failed";
        return -1;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot allocate local segment";
        cxl_backend_.reset();
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        metadata_->removeLocalSegment(local_server_name_);
        cxl_backend_.reset();
        return -1;
    }

    return 0;
}

int CxlTransport::allocateLocalSegmentID() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "cxl";
#else
    desc->protocol = "cxl";
#endif
    desc->cxl_base_addr = reinterpret_cast<uint64_t>(cxl_backend_->base());
    // cxl_name remains the mapping path for compatibility with existing
    // descriptors. cxl_pool_id is the stable topology identity.
    desc->cxl_name = cxl_backend_->config().path;
    desc->cxl_pool_id = cxl_backend_->config().logical_pool_id;
    desc->cxl_map_offset = cxl_backend_->config().mapping_offset;
    desc->cxl_capacity = cxl_backend_->config().capacity;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int CxlTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc cxl_buffer_desc;
    cxl_buffer_desc.name = local_server_name_;

    CxlPoolError error;
    const auto offset = cxl_backend_->offset_of(addr, length, &error);
    if (!offset.has_value()) {
        LogCxlError("register_memory_failed", error);
        errno = error.code == CxlPoolErrorCode::kOutOfBounds ? EFAULT : EINVAL;
        return -1;
    }

    cxl_buffer_desc.offset = *offset;
    cxl_buffer_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    cxl_buffer_desc.protocol = "cxl";
#endif
    return metadata_->addLocalMemoryBuffer(cxl_buffer_desc, update_metadata);
}

int CxlTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int CxlTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int CxlTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

Status CxlTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "CxlTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status CxlTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "CxlTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "CxlTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        CxlPoolError resolve_error;
        slice->cxl.dest_addr = cxl_backend_->resolve(
            request.target_offset, request.length, &resolve_error);
        if (slice->cxl.dest_addr == nullptr) {
            LogCxlError("transfer_extent_rejected", resolve_error);
            slice->markFailed();
            continue;
        }
        int err;
        if (slice->opcode == TransferRequest::READ)
            // READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void*)slice->cxl.dest_addr,
                            slice->length);
        else
            // WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void*)slice->cxl.dest_addr, slice->source_addr,
                            slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }

    return Status::OK();
}

Status CxlTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        task.total_bytes = request.length;

        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        CxlPoolError resolve_error;
        slice->cxl.dest_addr = cxl_backend_->resolve(
            request.target_offset, request.length, &resolve_error);
        if (slice->cxl.dest_addr == nullptr) {
            LogCxlError("transfer_task_extent_rejected", resolve_error);
            slice->markFailed();
            continue;
        }
        int err;
        if (slice->opcode == TransferRequest::READ)
            // READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void*)slice->cxl.dest_addr,
                            slice->length);
        else
            // WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void*)slice->cxl.dest_addr, slice->source_addr,
                            slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

}  // namespace mooncake
