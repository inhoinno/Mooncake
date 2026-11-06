#pragma once

#include <cstddef>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace mooncake::pool_relay {
// Shared by the production engine binding and portable native contract tests.
// Values are classic TE process virtual addresses, never pool offsets.
inline void ValidateWriteBatch(const std::vector<std::string>& segments,
                               const std::vector<uintptr_t>& sources,
                               const std::vector<uintptr_t>& destinations,
                               const std::vector<size_t>& lengths) {
    const auto n = segments.size();
    if (!n || n > 256 || sources.size() != n || destinations.size() != n ||
        lengths.size() != n)
        throw std::invalid_argument("invalid relay batch cardinality (1..256)");
    for (size_t i = 0; i < n; ++i) {
        if (segments[i].empty() || !sources[i] || !destinations[i] || !lengths[i] ||
            lengths[i] > std::numeric_limits<uintptr_t>::max() - sources[i] ||
            lengths[i] > std::numeric_limits<uintptr_t>::max() - destinations[i])
            throw std::invalid_argument("invalid relay registered VA range");
        for (size_t j = 0; j < i; ++j) {
            if (segments[i] == segments[j] &&
                destinations[i] < destinations[j] + lengths[j] &&
                destinations[j] < destinations[i] + lengths[i])
                throw std::invalid_argument("overlapping relay WRITE targets");
        }
    }
}
}  // namespace mooncake::pool_relay
