#pragma once
#include <cstdint>
#include <string>
namespace mooncake {
struct BufferDescriptor {
    uint64_t size_ = 0;
    uintptr_t buffer_address_ = 0;
    std::string protocol_, transport_endpoint_;
};
struct MemoryDescriptor { BufferDescriptor buffer_descriptor; };
class Replica {
 public:
    struct Descriptor {
        uint64_t id = 0;
        MemoryDescriptor memory;
        bool is_memory_replica() const { return true; }
        const MemoryDescriptor& get_memory_descriptor() const { return memory; }
    };
};
}
