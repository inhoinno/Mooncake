// Copyright 2026. SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>

namespace mooncake::write_pipeline {

// Canonical backing identity is supplied by the authority/caller. VA equality
// is not an alias test. The caller retains immutable source leases, authorized
// writable destinations and registrations throughout this synchronous run.
struct Range {
    std::string backing;
    uint64_t offset = 0, bytes = 0;
};
struct Request {
    uint64_t id = 0, source = 0, destination = 0, bytes = 0;
    std::string segment;
    Range source_range, destination_range;
    std::shared_ptr<void> lifetime;
};
enum class Instrumentation { Off, Counters, Sampled };
struct Options {
    size_t max_operations = 1;
    uint64_t max_bytes = 128ULL << 20;
    size_t descriptors_per_operation = 1;
    Instrumentation instrumentation = Instrumentation::Off;
    size_t trace_capacity = 4096;
};
struct Completion { bool success = true; };
class Operation {
 public:
    virtual ~Operation() = default;
    // False means pending/uncertain: do not reclaim any affected range.
    // Implementations use engine status, NEVER a second hardware CQ poller.
    virtual bool poll(Completion&) = 0;
};
struct Sample {
    uint64_t request = 0, operation = 0, begin_ns = 0, end_ns = 0;
    bool success = false;
    uint64_t admission_wait_ns = 0;
};
struct Event { uint64_t ns, operation, request; unsigned kind; };
struct Result {
    bool success = true;
    uint64_t bytes = 0, duration_ns = 0, operations = 0;
    uint64_t start_ns = 0;
    uint64_t process_cpu_ns = 0;
    uint64_t peak_operations = 0, peak_bytes = 0;
    uint64_t poll_passes = 0, idle_passes = 0;
    uint64_t depth_wait_passes = 0, byte_wait_passes = 0, conflict_wait_passes = 0;
    uint64_t admission_blocked_ns = 0, submission_ns = 0, completion_scan_ns = 0;
    bool trace_overflow = false;
    std::vector<Sample> samples;
    std::vector<Event> trace;
};

inline bool overlaps(const Range& a, const Range& b) {
    return a.backing == b.backing && a.offset < b.offset + b.bytes &&
           b.offset < a.offset + a.bytes;
}
inline bool conflicts(const Request& a, const Request& b) {
    return overlaps(a.destination_range, b.destination_range) ||
           overlaps(a.destination_range, b.source_range) ||
           overlaps(a.source_range, b.destination_range);
}

using Submit = std::function<std::unique_ptr<Operation>(const std::vector<const Request*>&)>;

inline uint64_t monotonicNowNs() {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC,&value))
        throw std::runtime_error("CLOCK_MONOTONIC unavailable");
    return uint64_t(value.tv_sec)*1000000000ULL+uint64_t(value.tv_nsec);
}

inline uint64_t processCpuNowNs() {
    timespec value{};
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&value))
        throw std::runtime_error("process CPU clock unavailable");
    return uint64_t(value.tv_sec)*1000000000ULL+uint64_t(value.tv_nsec);
}

// A failed operation stops admission, but the remaining handles still drain.
// Exceptions also destroy retained handles: engine adapter destructors drain.
// Pending/uncertain completion never releases credits, even after cancellation.
inline Result run(const std::vector<Request>& requests, Options options, const Submit& submit,
                  const std::function<bool()>& cancelled = {},
                  const std::function<void()>& ready = {}) {
    if (requests.empty() || requests.size() > 100000 || !options.max_operations ||
        options.max_operations > 4096 || !options.max_bytes ||
        !options.descriptors_per_operation || options.descriptors_per_operation > 1024 ||
        options.trace_capacity > 100000)
        throw std::invalid_argument("invalid bounded WRITE pipeline limits");
    uint64_t total = 0;
    std::unordered_set<uint64_t> ids;
    ids.reserve(requests.size());
    for (const auto& r : requests) {
        auto valid = [](const Range& x) {
            return !x.backing.empty() && x.bytes && x.offset <= UINT64_MAX - x.bytes;
        };
        if (r.id==UINT64_MAX || !r.bytes || r.bytes > options.max_bytes || !r.source || !r.destination ||
            r.source > UINT64_MAX-r.bytes || r.destination > UINT64_MAX-r.bytes ||
            r.segment.empty() || !valid(r.source_range) || !valid(r.destination_range) ||
            r.source_range.bytes != r.bytes || r.destination_range.bytes != r.bytes ||
            !r.lifetime || overlaps(r.source_range,r.destination_range) ||
            total > UINT64_MAX-r.bytes || !ids.insert(r.id).second)
            throw std::invalid_argument("invalid WRITE range/alias/lifetime or byte credit");
        total += r.bytes;
    }
    ids.clear();
    ids.rehash(0);  // Free validation scratch before ready, not beside a peer's DMA.
    struct Active {
        uint64_t id, bytes, start;
        uint64_t admission_wait_ns;
        std::vector<const Request*> requests;
        std::unique_ptr<Operation> operation;
    };
    Result out;
    out.samples.reserve(requests.size());
    if (options.instrumentation == Instrumentation::Sampled)
        out.trace.reserve(options.trace_capacity);
    std::vector<Active> active;
    active.reserve(options.max_operations);
    // All workload validation and persistent loop allocations precede release.
    if (ready) ready();
    const auto cpu_begin=processCpuNowNs();
    const auto origin = monotonicNowNs();
    out.start_ns = origin;
    auto now = [&]() -> uint64_t {
        return monotonicNowNs()-origin;
    };
    const bool detail = options.instrumentation != Instrumentation::Off;
    auto event = [&](uint64_t t, uint64_t op, uint64_t req, unsigned kind) {
        if (options.instrumentation != Instrumentation::Sampled) return;
        if (out.trace.size() < options.trace_capacity) out.trace.push_back({t,op,req,kind});
        else out.trace_overflow = true;
    };
    size_t next = 0;
    uint64_t pending_bytes = 0, blocked_since = 0;
    bool stopped = false, blocked = false;
    while (next < requests.size() || !active.empty()) {
        bool progress = false;
        ++out.poll_passes;
        const auto scan = detail ? now() : 0;
        // Scan ALL operations, not just the oldest. Removing an operation
        // releases only its own ranges/credits. Others remain strongly owned.
        for (size_t i=0; i<active.size();) {
            Completion terminal;
            if (!active[i].operation->poll(terminal)) { ++i; continue; }
            const auto end = now();  // Mandatory operation latency sample.
            auto& a = active[i];
            for (const auto* r : a.requests) {
                out.samples.push_back({r->id,a.id,a.start,end,terminal.success,a.admission_wait_ns});
                event(end,a.id,r->id,2);
            }
            out.success &= terminal.success;
            stopped |= !terminal.success;
            if (terminal.success) out.bytes += a.bytes;
            pending_bytes -= a.bytes;
            active.erase(active.begin()+i);
            progress = true;
        }
        if (detail) out.completion_scan_ns += now()-scan;
        if (cancelled && cancelled()) { stopped = true; out.success = false; }
        if (stopped) next = requests.size();
        while (next < requests.size()) {
            if (cancelled && cancelled()) { stopped=true;out.success=false;next=requests.size();break; }
            if (active.size() == options.max_operations) { ++out.depth_wait_passes; break; }
            std::vector<const Request*> batch;
            batch.reserve(options.descriptors_per_operation);
            uint64_t bytes = 0;
            while (next+batch.size() < requests.size() && batch.size() < options.descriptors_per_operation) {
                const auto& r = requests[next+batch.size()];
                if (r.bytes > options.max_bytes-pending_bytes-bytes) { ++out.byte_wait_passes; break; }
                bool collision = false;
                for (const auto& a : active)
                    for (const auto* prior : a.requests) collision |= conflicts(r,*prior);
                for (const auto* prior : batch) collision |= conflicts(r,*prior);
                if (collision) { ++out.conflict_wait_passes; break; }
                batch.push_back(&r);
                bytes += r.bytes;
            }
            if (batch.empty()) break;
            // Flush a partial batch immediately on exhaustion, conflict or
            // credit pressure. Never wait for a full descriptor batch.
            const auto start = now();
            const auto admission_wait=blocked && detail ? start-blocked_since : 0;
            out.admission_blocked_ns += admission_wait;
            blocked = false;
            auto handle = submit(batch);
            if (!handle) throw std::runtime_error("submission returned no retained handle");
            if (detail) out.submission_ns += now()-start;
            const auto id = out.operations++;
            for (const auto* r : batch) event(start,id,r->id,1);
            next += batch.size();
            active.push_back({id,bytes,start,admission_wait,std::move(batch),std::move(handle)});
            pending_bytes += bytes;
            out.peak_operations = std::max<uint64_t>(out.peak_operations,active.size());
            out.peak_bytes = std::max(out.peak_bytes,pending_bytes);
            progress = true;
        }
        if (next < requests.size() && !blocked) {
            blocked = true;
            if (detail) blocked_since = now();
        }
        if (!progress && !active.empty()) {
            ++out.idle_passes;
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
    out.duration_ns = now();
    out.process_cpu_ns = processCpuNowNs()-cpu_begin;
    out.success &= out.bytes == total;
    return out;
}
}  // namespace mooncake::write_pipeline
