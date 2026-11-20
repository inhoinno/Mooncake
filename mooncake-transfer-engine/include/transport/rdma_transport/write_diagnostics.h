#ifndef MOONCAKE_RDMA_WRITE_DIAGNOSTICS_H
#define MOONCAKE_RDMA_WRITE_DIAGNOSTICS_H

#include <cstddef>
#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace mooncake {

// Diagnostic accounting only. The WorkerPool mutex protects this value.
// All classic WRs on this path are signaled. These are software observations,
// not wire occupancy or a DMA lifetime/reclamation authority.
struct WriteDiagnostics {
    struct Trace { uint64_t ns,request,slice_offset,bytes,qp,kind; };
    struct Qp {
        uint64_t post_calls = 0, posted_wrs = 0, posted_bytes = 0, post_ns = 0;
        uint64_t rejected_wrs = 0, unknown_wrs = 0;
        uint64_t completed_wrs = 0, completed_bytes = 0, completion_errors = 0;
        uint64_t pending_wrs = 0, pending_bytes = 0, peak_wrs = 0, peak_bytes = 0;
        uint64_t posting_tid = 0;
        int completion_worker = -1;
        std::string peer_nic;
    };
    struct Poller {
        uint64_t polls = 0, empty_polls = 0, errors = 0, cqes = 0, poll_ns = 0;
    };
    static constexpr size_t kLimit = 4096;
    uint64_t pending_wrs = 0, pending_bytes = 0, completed_bytes = 0;
    uint64_t outstanding_ns = 0, peak_pending_wrs = 0, peak_pending_bytes = 0;
    std::unordered_map<uint32_t, uint64_t> qp_completed_bytes;
    std::unordered_map<uint32_t, Qp> qp_statistics;
    std::unordered_map<int, Poller> pollers;
    bool qp_counters_truncated = false;

    void reserve(uint32_t number,uint64_t bytes) {
        if(auto* q=qp(number)) {
            ++q->pending_wrs;q->pending_bytes+=bytes;
            q->peak_wrs=std::max(q->peak_wrs,q->pending_wrs);
            q->peak_bytes=std::max(q->peak_bytes,q->pending_bytes);
        }
    }
    void release(uint32_t number,uint64_t bytes) {
        if(auto* q=qp(number)) {
            if(q->pending_wrs && q->pending_bytes>=bytes) {--q->pending_wrs;q->pending_bytes-=bytes;}
        }
    }

    Qp* qp(uint32_t number) {
        auto it = qp_statistics.find(number);
        if (it != qp_statistics.end()) return &it->second;
        if (qp_statistics.size() == kLimit) {
            qp_counters_truncated = true;
            return nullptr;
        }
        return &qp_statistics.emplace(number, Qp{}).first->second;
    }
    void posted(uint32_t number, uint64_t wrs, uint64_t bytes,
                uint64_t rejected, uint64_t unknown, uint64_t tid = 0,
                const std::string& peer = {}, uint64_t post_ns = 0) {
        if (auto* q = qp(number)) {
            ++q->post_calls;
            q->posted_wrs += wrs;
            q->posted_bytes += bytes;
            q->rejected_wrs += rejected;
            q->unknown_wrs += unknown;
            q->posting_tid = tid;
            q->peer_nic = peer;
            q->post_ns += post_ns;
        }
    }
    void completed(uint32_t number, uint64_t bytes, bool success, int worker = -1) {
        if (auto* q = qp(number)) {
            q->completion_worker = worker;
            if (success) {
                ++q->completed_wrs;
                q->completed_bytes += bytes;
            } else {
                ++q->completion_errors;
            }
        }
        if (!success) return;
        completed_bytes += bytes;
        auto it = qp_completed_bytes.find(number);
        if (it != qp_completed_bytes.end()) it->second += bytes;
        else if (qp_completed_bytes.size() < kLimit) qp_completed_bytes.emplace(number, bytes);
        else qp_counters_truncated = true;
    }
    void polled(int worker, int entries, uint64_t elapsed = 0) {
        if (!pollers.count(worker) && pollers.size() == kLimit) {
            qp_counters_truncated = true;
            return;
        }
        auto& p = pollers[worker];
        ++p.polls;
        p.poll_ns += elapsed;
        if (entries < 0) ++p.errors;
        else if (entries == 0) ++p.empty_polls;
        else p.cqes += static_cast<uint64_t>(entries);
    }
};
}  // namespace mooncake
#endif
