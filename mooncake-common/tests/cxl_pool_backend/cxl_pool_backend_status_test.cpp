// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

using namespace mooncake;
using namespace mooncake::cxl_test;

namespace {

bool Has(const std::string& status, std::string_view field) {
    return status.find(field) != std::string::npos;
}

}  // namespace

int main() {
    TestContext test("T1.4_status_observability");
    constexpr std::uint64_t kCapacity = 1U << 20;
    TemporaryPoolFile file(kCapacity);
    test.Expect(file.valid(), "status-test pool is created");
    if (!file.valid()) return test.Finish();

    CxlPoolError error;
    auto backend = faketract::OpenMmapCxlPoolBackend(
        MakeFileConfig(file.path(), kCapacity, "rack7-cxl9"), &error);
    test.Expect(backend != nullptr, "status-test backend opens");
    if (!backend) return test.Finish();

    auto aborted = backend->alloc("status-abort", 1024, &error);
    test.Expect(aborted != nullptr, "observable abort reservation is created");
    if (aborted) aborted->abort();

    auto committed = backend->alloc("status-commit", 2048, &error);
    test.Expect(committed != nullptr,
                "observable commit reservation is created");
    if (committed) {
        FillDeterministic(committed->data(), 2048);
        test.Expect(committed->commit(&error).has_value(),
                    "observable commit succeeds");
    }
    backend->metadata_lookup("status-miss", &error);
    backend->metadata_lookup("status-commit", &error);

    const auto snapshot = backend->status();
    const std::string json = snapshot.ToJson();
    test.Expect(
        snapshot.provider == "faketract" && snapshot.lifecycle == "ready" &&
            snapshot.commit_count == 1 && snapshot.abort_count == 1 &&
            snapshot.active_reservations == 0 &&
            snapshot.lookup_hit_count == 1 && snapshot.lookup_miss_count == 1,
        "status counters expose balanced terminal lifecycle");
    test.Expect(Has(json, "\"component\":\"cxl_pool_backend\"") &&
                    Has(json, "\"provider\":\"faketract\"") &&
                    Has(json, "\"logical_pool_id\":\"rack7-cxl9\"") &&
                    Has(json, "\"backend_kind\":\"file\"") &&
                    Has(json, "\"path\":\"" + file.path() + "\"") &&
                    Has(json, "\"capacity\":1048576") &&
                    Has(json, "\"commit_count\":1") &&
                    Has(json, "\"abort_count\":1") &&
                    Has(json, "\"last_error_code\":\"ok\""),
                "JSON includes reproducible pool, capacity, and result fields");
    test.Expect(!Has(json, "0x"), "ordinary status omits raw pointer values");
    std::cout << json << '\n';
    return test.Finish();
}
