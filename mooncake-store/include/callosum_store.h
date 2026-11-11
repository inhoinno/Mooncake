#pragma once

#include <memory>
#include <string>
#include <vector>

#include "cxl_pool_backend.h"
#include "replica.h"
#include "pool_relay.h"

namespace mooncake {
class Client;
class TransferEngine;

// Opt-in, CXL-first adapter below the unchanged MooncakeStoreConnector API.
// Store owns keys, allocations, pins and replica publication. The backend maps
// bytes only. Native RDMA descriptors use receiver VAs, never pool offsets.
struct CallosumStoreConfig {
    std::string campaign;
    std::string pool_id;
    std::string path;
    std::string backend_kind;
    uint64_t capacity = 0;
    uint64_t mapping_offset = 0;
    uint64_t alignment = 0;
    uint64_t owned_offset = 0;
    uint64_t owned_size = 0;
    std::string token_file;
    std::string journal;
    uint64_t wait_ms = 30000;
    uint64_t mux_capacity = 2;
    uint64_t mux_queue_limit = 32;

    static std::unique_ptr<CallosumStoreConfig> Load();
    std::shared_ptr<CxlPoolBackend> Map() const;
};

class CallosumStoreBridge {
   public:
    struct CopyResult {
        bool success = false;
        // True means no payload was posted or every possible task drained.
        bool safe_to_revoke = false;
    };

    CallosumStoreBridge(Client& client, TransferEngine& engine,
                        CallosumStoreConfig config,
                        std::shared_ptr<CxlPoolBackend> pool);
    ~CallosumStoreBridge();
    CopyResult Copy(const std::string& key, const Replica::Descriptor& source,
                    const std::vector<Replica::Descriptor>& targets);
    bool Materialize(const std::string& key);
    bool Owns(const Replica::Descriptor& replica) const;
    std::vector<pool_relay::MuxEvent> MuxStatus() const;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake
