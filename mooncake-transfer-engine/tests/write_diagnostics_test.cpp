// Hardware-free checks of the accounting used by the production WorkerPool.
#include "transport/rdma_transport/write_diagnostics.h"
#include <cstdlib>
#include <iostream>

#define CHECK(x) do { if (!(x)) { std::cerr << #x << " failed\n"; std::abort(); } } while (false)

int main() {
    mooncake::WriteDiagnostics d;
    d.reserve(11,128); d.reserve(11,64);
    CHECK(d.qp_statistics[11].pending_wrs == 2 && d.qp_statistics[11].peak_bytes == 192);
    d.release(11,128); d.release(11,64);
    CHECK(d.qp_statistics[11].pending_wrs == 0 && d.qp_statistics[11].pending_bytes == 0);
    // Fast CQ processing can precede the posting thread's return from verbs.
    d.completed(11, 128, true);
    d.posted(11, 1, 128, 1, 0);
    d.posted(12, 0, 0, 0, 3);
    d.completed(12, 64, false);
    CHECK(d.completed_bytes == 128);
    CHECK(d.qp_completed_bytes.size() == 1);
    CHECK(d.qp_statistics[11].posted_bytes == d.qp_statistics[11].completed_bytes);
    CHECK(d.qp_statistics[11].posted_wrs == 1 && d.qp_statistics[11].rejected_wrs == 1);
    CHECK(d.qp_statistics[12].posted_wrs == 0 && d.qp_statistics[12].unknown_wrs == 3);
    CHECK(d.qp_statistics[12].completion_errors == 1);
    d.polled(0, 0,7); d.polled(0, 4,11); d.polled(1, -1);
    CHECK(d.pollers[0].polls == 2 && d.pollers[0].empty_polls == 1 && d.pollers[0].cqes == 4);
    CHECK(d.pollers[0].poll_ns == 18);
    CHECK(d.pollers[1].errors == 1);
    const auto saved = d;
    d.posted(11, 1, 8, 0, 0);
    CHECK(saved.qp_statistics.at(11).posted_bytes == 128);
    for (uint32_t q = 100; q < 100 + d.kLimit; ++q) d.completed(q, 1, true);
    CHECK(d.qp_statistics.size() == d.kLimit && d.qp_completed_bytes.size() == d.kLimit);
    CHECK(d.qp_counters_truncated);
    std::cout << "PASS: posting, CQ errors, unknown acceptance, bounded diagnostic accounting\n";
}
