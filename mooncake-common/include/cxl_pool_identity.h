#pragma once

#include <cstdint>
#include <string_view>

namespace mooncake {
// A remote CXL offset can be resolved locally only in the same mapping domain.
// Virtual bases are deliberately absent. Unknown legacy geometry fails closed.
// This check does not prove physical sharing, visibility, or generation validity.
inline bool SameCxlMapping(std::string_view local_pool, uint64_t local_offset,
                           uint64_t local_capacity, std::string_view remote_pool,
                           uint64_t remote_offset, uint64_t remote_capacity) {
    return !local_pool.empty() && local_pool == remote_pool &&
           local_capacity != 0 && local_capacity == remote_capacity &&
           local_offset == remote_offset;
}
}  // namespace mooncake
