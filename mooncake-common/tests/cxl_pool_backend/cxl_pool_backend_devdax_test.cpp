// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

#include <cstdlib>
#include <string_view>

using namespace mooncake;
using namespace mooncake::cxl_test;

int main() {
    const char* path = std::getenv("MC_CXL_DEV_PATH");
    const char* destructive = std::getenv("MC_CXL_TEST_DESTRUCTIVE");
    if (path == nullptr || std::string_view(path).rfind("/dev/dax", 0) != 0) {
        std::cout << "test=T1.devdax tier=T2 status=SKIP "
                     "reason=MC_CXL_DEV_PATH_is_not_/dev/dax\n";
        return 0;
    }
    if (destructive == nullptr || std::string_view(destructive) != "1") {
        std::cout << "test=T1.devdax tier=T2 status=SKIP "
                     "reason=set_MC_CXL_TEST_DESTRUCTIVE=1_for_a_dedicated_"
                     "aligned_test_extent\n";
        return 0;
    }

    TestContext test("T1.devdax_mapping", "T2", "devdax");
    CxlPoolError error;
    CxlPoolConfig config;
    test.Expect(LoadCxlPoolConfigFromEnvironment(&config, &error),
                "devdax environment passes preflight");
    if (error) {
        std::cerr << "error_code=" << ToString(error.code)
                  << " operation=" << error.operation
                  << " field=" << error.field
                  << " system_error=" << error.system_error << " message=\""
                  << error.message << "\"\n";
        return test.Finish();
    }
    test.Expect(config.backend_kind == CxlPoolBackendKind::kDevDax,
                "backend kind is devdax");

    auto backend = faketract::OpenMmapCxlPoolBackend(config, &error);
    test.Expect(backend != nullptr, "dedicated devdax extent opens and maps");
    if (!backend) {
        std::cerr << "error_code=" << ToString(error.code)
                  << " operation=" << error.operation
                  << " field=" << error.field
                  << " system_error=" << error.system_error << " message=\""
                  << error.message << "\"\n";
        return test.Finish();
    }

    constexpr std::uint64_t kPayloadBytes = 4096;
    auto allocation =
        backend->alloc("devdax-probe-object", kPayloadBytes, &error);
    test.Expect(allocation != nullptr,
                "devdax endpoint creates an invisible reservation");
    if (allocation) {
        FillDeterministic(allocation->data(), kPayloadBytes);
        const std::uint64_t expected =
            Checksum(allocation->data(), kPayloadBytes);
        const auto location = allocation->commit(&error);
        const void* resolved =
            location
                ? backend->resolve(location->offset, location->length, &error)
                : nullptr;
        test.Expect(location.has_value() && resolved != nullptr &&
                        Checksum(resolved, kPayloadBytes) == expected,
                    "devdax write, commit, lookup extent, and checksum pass");
        std::cout << "logical_pool_id=" << config.logical_pool_id
                  << " mapping_offset=" << config.mapping_offset
                  << " mapping_bytes=" << config.capacity
                  << " checksum=" << expected << '\n';
    }
    std::cout << backend->status().ToJson() << '\n';
    return test.Finish();
}
