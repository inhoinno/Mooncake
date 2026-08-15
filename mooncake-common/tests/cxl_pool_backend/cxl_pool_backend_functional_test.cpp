// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

#include <cstring>

using namespace mooncake;
using namespace mooncake::cxl_test;

int main() {
    TestContext test("T1.2_functional");
    constexpr std::uint64_t kCapacity = 2U << 20;
    constexpr std::uint64_t kPayloadBytes = 128U << 10;
    TemporaryPoolFile file(kCapacity);
    test.Expect(file.valid(), "file-backed CXL model is created");
    if (!file.valid()) return test.Finish();

    CxlPoolError error;
    auto backend = faketract::OpenMmapCxlPoolBackend(
        MakeFileConfig(file.path(), kCapacity, "rack0-pool0"), &error);
    test.Expect(backend != nullptr, "backend opens");
    if (!backend) return test.Finish();

    test.Expect(!backend->metadata_lookup("block-hash-001", &error) && !error,
                "metadata_lookup reports an initial MISS");
    auto allocation = backend->alloc("block-hash-001", kPayloadBytes, &error);
    test.Expect(allocation != nullptr && allocation->data() != nullptr,
                "alloc returns a writable reservation");
    test.Expect(!backend->metadata_lookup("block-hash-001", &error),
                "reservation is invisible before commit");
    if (!allocation) return test.Finish();

    FillDeterministic(allocation->data(), kPayloadBytes);
    const std::uint64_t expected_checksum =
        Checksum(allocation->data(), kPayloadBytes);
    const auto committed = allocation->commit(&error);
    test.Expect(committed.has_value() && !error,
                "commit publishes completed reservation");
    test.Expect(allocation->state() == CxlAllocationState::kCommitted,
                "allocation records committed terminal state");

    const auto found = backend->metadata_lookup("block-hash-001", &error);
    test.Expect(found.has_value() && found->logical_pool_id == "rack0-pool0" &&
                    found->length == kPayloadBytes && found->generation != 0,
                "metadata_lookup returns portable location projection");
    void* resolved =
        found ? backend->resolve(found->offset, found->length, &error)
              : nullptr;
    test.Expect(resolved != nullptr &&
                    Checksum(resolved, kPayloadBytes) == expected_checksum,
                "committed payload checksum matches through offset mapping");
    test.Expect(backend->alloc("block-hash-001", 4096, &error) == nullptr &&
                    error.code == CxlPoolErrorCode::kObjectAlreadyExists,
                "duplicate object allocation is explicit");

    std::cout << "object=block-hash-001 bytes=" << kPayloadBytes
              << " checksum=" << expected_checksum
              << " location_offset=" << (found ? found->offset : 0) << '\n';
    allocation.reset();
    backend.reset();
    test.Expect(true, "allocation and mapping close without leaked ownership");
    return test.Finish();
}
