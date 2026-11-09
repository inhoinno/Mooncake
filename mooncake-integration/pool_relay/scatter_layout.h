#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mooncake::pool_relay {

// A caller supplies the actual connector tensor layout, in logical byte order.
// address is a local source VA or receiver-advertised classic TE target VA.
struct TensorExtent {
    uint64_t logical, address, length;
};
struct ScatterFragment {
    uint64_t logical, source, destination, length;
};

inline void CheckLayout(const std::vector<TensorExtent>& layout,
                        uint64_t object_bytes) {
    uint64_t cursor = 0;
    if (!object_bytes || layout.empty())
        throw std::invalid_argument("empty tensor layout");
    for (const auto& e : layout) {
        if (e.logical != cursor || !e.address || !e.length ||
            e.length > object_bytes - cursor ||
            e.address > std::numeric_limits<uint64_t>::max() - e.length)
            throw std::invalid_argument(
                "tensor layout gap, overlap or overflow");
        cursor += e.length;
    }
    if (cursor != object_bytes)
        throw std::invalid_argument("incomplete tensor layout");
}

inline std::vector<ScatterFragment> ProjectScatter(
    const std::vector<TensorExtent>& source,
    const std::vector<TensorExtent>& destination, uint64_t object_bytes,
    uint64_t start, uint64_t length, uint64_t max_fragment) {
    CheckLayout(source, object_bytes);
    CheckLayout(destination, object_bytes);
    if (!length || start >= object_bytes || length > object_bytes - start ||
        !max_fragment)
        throw std::invalid_argument("invalid assigned logical range");
    // Destination tensors may alias despite distinct logical object offsets.
    for (size_t i = 0; i < destination.size(); ++i)
        for (size_t j = 0; j < i; ++j)
            if (destination[i].address <
                    destination[j].address + destination[j].length &&
                destination[j].address <
                    destination[i].address + destination[i].length)
                throw std::invalid_argument(
                    "aliased destination tensor extents");
    std::vector<ScatterFragment> result;
    size_t si = 0, di = 0;
    uint64_t pos = start, end = start + length;
    while (pos < end) {
        while (source[si].logical + source[si].length <= pos) ++si;
        while (destination[di].logical + destination[di].length <= pos) ++di;
        const auto& s = source[si];
        const auto& d = destination[di];
        auto n = std::min({end - pos, max_fragment, s.logical + s.length - pos,
                           d.logical + d.length - pos});
        result.push_back(
            {pos, s.address + pos - s.logical, d.address + pos - d.logical, n});
        pos += n;
    }
    return result;
}

// Receiver-owned per-object gather. Event identity is an assignment within one
// attempt. Sender completion, visibility and GPU ordering remain separate.
class ReadinessLedger {
   public:
    ReadinessLedger(std::string attempt, std::string generation, uint64_t bytes)
        : attempt_(std::move(attempt)),
          generation_(std::move(generation)),
          bytes_(bytes) {
        if (attempt_.empty() || generation_.empty() || !bytes_)
            throw std::invalid_argument("missing readiness identity");
    }
    void assign(std::string id, uint64_t start, uint64_t length, bool helper) {
        if (id.empty() || start >= bytes_ || !length || length > bytes_ - start)
            throw std::invalid_argument("invalid readiness range");
        for (const auto& e : entries_)
            if (e.id == id ||
                (start < e.start + e.length && e.start < start + length))
                throw std::invalid_argument(
                    "duplicate or overlapping assignment");
        entries_.push_back({std::move(id), start, length, helper, 0});
    }
    void event(const std::string& attempt, const std::string& generation,
               const std::string& id, const std::string& phase) {
        if (attempt != attempt_)
            throw std::invalid_argument("stale readiness attempt");
        if (generation != generation_)
            throw std::invalid_argument("stale object generation");
        for (auto& e : entries_) {
            if (e.id != id) continue;
            unsigned next = phase == "transport"                   ? 1
                            : phase == "validated"                 ? 2
                            : phase == "copy_complete" && e.helper ? 3
                            : phase == "gpu_ready" ? (e.helper ? 4 : 3)
                                                   : 0;
            if (!next) throw std::invalid_argument("unknown readiness phase");
            if (next <= e.phase)
                return;  // Typed success events are idempotent.
            if (next != e.phase + 1)
                throw std::invalid_argument("readiness dependency missing");
            e.phase = next;
            return;
        }
        throw std::invalid_argument("unknown readiness assignment");
    }
    bool ready(uint64_t required_end) const {
        if (!required_end || required_end > bytes_)
            throw std::invalid_argument("invalid dependent-operation boundary");
        auto ordered = entries_;
        std::sort(
            ordered.begin(), ordered.end(),
            [](const Entry& a, const Entry& b) { return a.start < b.start; });
        uint64_t cursor = 0;
        for (const auto& e : ordered) {
            if (e.start != cursor || e.phase != (e.helper ? 4U : 3U))
                return false;
            cursor += e.length;
            if (cursor >= required_end) return true;
        }
        return false;
    }

   private:
    struct Entry {
        std::string id;
        uint64_t start, length;
        bool helper;
        unsigned phase;
    };
    std::string attempt_;
    std::string generation_;
    uint64_t bytes_;
    std::vector<Entry> entries_;
};
}  // namespace mooncake::pool_relay
