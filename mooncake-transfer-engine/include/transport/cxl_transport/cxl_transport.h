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

#ifndef CXL_TRANSPORT_H_
#define CXL_TRANSPORT_H_

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cxl_pool_backend.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferMetadata;

class CxlTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    explicit CxlTransport(
        std::shared_ptr<CxlPoolBackend> cxl_backend = nullptr);

    ~CxlTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    void *getCxlBaseAddr() const {
        return cxl_backend_ ? cxl_backend_->base() : nullptr;
    }

    size_t getCxlDeviceSize() const {
        return cxl_backend_ ? cxl_backend_->config().capacity : 0;
    }

    std::string getCxlPoolId() const {
        return cxl_backend_ ? cxl_backend_->config().logical_pool_id
                            : std::string();
    }

    size_t getCxlOwnedOffset() const {
        return cxl_backend_ ? cxl_backend_->config().owned_offset : 0;
    }

    size_t getCxlOwnedSize() const {
        return cxl_backend_ ? cxl_backend_->config().owned_capacity : 0;
    }

    bool isCxlAllocationMasterManaged() const {
        return cxl_backend_ && cxl_backend_->master_managed_allocation();
    }

    CxlPoolStatusSnapshot getCxlPoolStatus() const {
        return cxl_backend_ ? cxl_backend_->status()
                            : CxlPoolStatusSnapshot{};
    }

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int allocateLocalSegmentID();

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    const char *getName() const override { return "cxl"; }

    int cxlDevInit();

    // Returns 0 when the copy completed synchronously, 1 when a CUDA event
    // will complete it asynchronously, and -1 on failure.
    int cxlMemcpy(Transport::Slice *slice, void *dest_addr,
                  void *source_addr, size_t size);

    void pollCudaCompletions(Transport::TransferTask &task);

    void destroyCudaStreams();

    bool isAddressInCxlRange(void *addr);

    bool validateMemoryBounds(void *dest, void *src, size_t size);

   private:
    std::shared_ptr<CxlPoolBackend> cxl_backend_;
    std::mutex cuda_stream_mutex_;
    // device id -> cudaStream_t, kept opaque for CPU-only builds.
    std::unordered_map<int, void *> cuda_streams_;
};
}  // namespace mooncake

#endif
