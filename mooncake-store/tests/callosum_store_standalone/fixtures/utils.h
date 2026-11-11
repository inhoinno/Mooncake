#pragma once
#include <atomic>
#include <string>
namespace mooncake {
using UUID = uint64_t;
inline UUID generate_uuid() { static std::atomic<uint64_t> id{1}; return id++; }
inline std::string UuidToString(UUID id) { return std::to_string(id); }
}
