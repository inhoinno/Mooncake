// Copyright 2026 Mooncake Authors

#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake {

inline uint64_t StableKvPlacementHash(std::string_view key) noexcept {
    uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char value : key) {
        hash ^= value;
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline std::optional<std::string> SelectHashMultipathSegment(
    std::string_view key, std::string_view cxl_segment,
    std::string_view network_segment) {
    if (cxl_segment.empty() || network_segment.empty() ||
        cxl_segment == network_segment) {
        return std::nullopt;
    }
    return std::string((StableKvPlacementHash(key) & 1U) == 0U
                           ? cxl_segment
                           : network_segment);
}

inline std::optional<std::string> MultipathSegmentFromEnvironment(
    std::string_view key) {
    const char* mode = std::getenv("MC_STORE_MULTIPATH_PLACEMENT");
    if (mode == nullptr || std::string_view(mode) != "hash") {
        return std::nullopt;
    }
    const char* cxl = std::getenv("MC_STORE_MULTIPATH_CXL_SEGMENT");
    const char* network = std::getenv("MC_STORE_MULTIPATH_NETWORK_SEGMENT");
    return SelectHashMultipathSegment(key, cxl == nullptr ? "" : cxl,
                                      network == nullptr ? "" : network);
}

}  // namespace mooncake
