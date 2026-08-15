// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

using namespace mooncake;
using namespace mooncake::cxl_test;

int main() {
    TestContext test("T1.3_failure_debug");
    constexpr std::uint64_t kCapacity = 16U << 10;
    TemporaryPoolFile file(kCapacity);
    test.Expect(file.valid(), "small failure-test pool is created");
    if (!file.valid()) return test.Finish();

    CxlPoolError error;
    auto backend = faketract::OpenMmapCxlPoolBackend(
        MakeFileConfig(file.path(), kCapacity, "debug-pool"), &error);
    test.Expect(backend != nullptr, "small backend opens");
    if (!backend) return test.Finish();

    test.Expect(backend->resolve(kCapacity - 8, 16, &error) == nullptr &&
                    ErrorIs(error, CxlPoolErrorCode::kOutOfBounds, "extent"),
                "extent beyond capacity is rejected");
    test.Expect(backend->resolve(std::numeric_limits<std::uint64_t>::max(), 2,
                                 &error) == nullptr &&
                    error.code == CxlPoolErrorCode::kOutOfBounds,
                "overflow-shaped offset is rejected without pointer math");

    auto aborted = backend->alloc("aborted-block", 4096, &error);
    test.Expect(aborted != nullptr, "abort test reservation succeeds");
    if (aborted) {
        FillDeterministic(aborted->data(), 4096);
        aborted->abort();
        aborted->abort();
        test.Expect(aborted->state() == CxlAllocationState::kAborted,
                    "abort is terminal and idempotent");
    }
    test.Expect(!backend->metadata_lookup("aborted-block", &error),
                "aborted payload never becomes visible");

    {
        auto destructor_abort =
            backend->alloc("destructor-abort", 4096, &error);
        test.Expect(destructor_abort != nullptr,
                    "destructor-abort reservation succeeds");
    }
    test.Expect(!backend->metadata_lookup("destructor-abort", &error),
                "reservation destructor aborts unpublished state");

    constexpr std::size_t kRacers = 8;
    std::atomic<std::size_t> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::unique_ptr<CxlAllocation>> race_allocations(kRacers);
    std::vector<std::thread> racers;
    racers.reserve(kRacers);
    for (std::size_t index = 0; index < kRacers; ++index) {
        racers.emplace_back([&, index] {
            ++ready;
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            CxlPoolError thread_error;
            race_allocations[index] =
                backend->alloc("duplicate-race", 1024, &thread_error);
        });
    }
    while (ready.load(std::memory_order_acquire) != kRacers) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);
    for (auto& racer : racers) racer.join();
    std::size_t race_winners = 0;
    for (auto& allocation : race_allocations) {
        if (allocation) {
            ++race_winners;
            allocation->abort();
        }
    }
    test.Expect(race_winners == 1 &&
                    !backend->metadata_lookup("duplicate-race", &error),
                "concurrent duplicate alloc has one invisible winner");

    auto full = backend->alloc("full-pool", kCapacity, &error);
    test.Expect(full != nullptr,
                "aborted extents are reclaimed for a full-capacity allocation");
    test.Expect(
        backend->alloc("exhausted", 64, &error) == nullptr &&
            ErrorIs(error, CxlPoolErrorCode::kAllocationExhausted, "capacity"),
        "exhaustion reports an actionable error");
    if (full) full->abort();

    auto terminal = backend->alloc("terminal", 64, &error);
    test.Expect(terminal != nullptr, "terminal-state reservation succeeds");
    if (terminal) {
        terminal->abort();
        test.Expect(!terminal->commit(&error) &&
                        error.code == CxlPoolErrorCode::kReservationConflict &&
                        !backend->metadata_lookup("terminal"),
                    "commit after abort fails without publication");
    }

    TemporaryPoolFile short_file(4096);
    auto oversized = MakeFileConfig(short_file.path(), 8192, "short-file");
    test.Expect(
        faketract::OpenMmapCxlPoolBackend(oversized, &error) == nullptr &&
            ErrorIs(error, CxlPoolErrorCode::kInvalidConfig, "capacity"),
        "short backing file fails before mmap/SIGBUS");

    auto map_failure = MakeFileConfig("/dev/null", 4096, "map-failure");
    map_failure.backend_kind = CxlPoolBackendKind::kDevDax;
    map_failure.capacity = map_failure.mapping_alignment;
    test.Expect(
        faketract::OpenMmapCxlPoolBackend(map_failure, &error) == nullptr &&
            ErrorIs(error, CxlPoolErrorCode::kMapFailed, "extent") &&
            error.system_error != 0,
        "mmap failure is actionable and closes the opened device");

    const auto status = backend->status();
    test.Expect(status.active_reservations == 0 && status.reserved_bytes == 0,
                "all injected failures leave no live reservation");
    return test.Finish();
}
