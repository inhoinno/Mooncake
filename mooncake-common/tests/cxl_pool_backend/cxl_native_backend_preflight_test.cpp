// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

#include <cstdlib>
#include <string>

using namespace mooncake;
using namespace mooncake::cxl_test;

namespace {

void SetEnvironment(const char* name, const std::string& value) {
    ::setenv(name, value.c_str(), 1);
}

void ClearEnvironment() {
    for (const char* name :
         {"MC_CXL_PROVIDER", "MC_CXL_DEV_PATH", "MC_CXL_DEV_SIZE",
          "MC_CXL_POOL_ID", "MC_CXL_BACKEND_KIND", "MC_CXL_OWNED_OFFSET",
          "MC_CXL_OWNED_SIZE"}) {
        ::unsetenv(name);
    }
}

}  // namespace

int main() {
    TestContext test("T2.1_native_cxl_preflight");
    constexpr std::uint64_t kCapacity = 8U << 20;
    constexpr std::uint64_t kOwnedOffset = 4U << 20;
    constexpr std::uint64_t kOwnedCapacity = 4U << 20;
    TemporaryPoolFile file(kCapacity);
    test.Expect(file.valid(), "file-backed shared pool is created");
    if (!file.valid()) return test.Finish();

    ClearEnvironment();
    SetEnvironment("MC_CXL_PROVIDER", "mooncake");
    SetEnvironment("MC_CXL_DEV_PATH", file.path());
    SetEnvironment("MC_CXL_DEV_SIZE", std::to_string(kCapacity));
    SetEnvironment("MC_CXL_POOL_ID", "todo2-native-pool");
    SetEnvironment("MC_CXL_BACKEND_KIND", "file");

    CxlPoolError error;
    auto missing_ownership = OpenCxlPoolBackendFromEnvironment(&error);
    test.Expect(!missing_ownership &&
                    error.code == CxlPoolErrorCode::kInvalidConfig &&
                    error.field == "MC_CXL_OWNED_SIZE",
                "native provider fails closed without an owned subrange");

    SetEnvironment("MC_CXL_OWNED_OFFSET", std::to_string(kOwnedOffset));
    SetEnvironment("MC_CXL_OWNED_SIZE", std::to_string(kOwnedCapacity));
    auto backend = OpenCxlPoolBackendFromEnvironment(&error);
    test.Expect(backend != nullptr && !error,
                "native Mooncake mapping provider opens");
    if (!backend) {
        ClearEnvironment();
        return test.Finish();
    }

    test.Expect(backend->master_managed_allocation(),
                "backend declares single-Master allocation ownership");
    test.Expect(backend->config().capacity == kCapacity &&
                    backend->config().owned_offset == kOwnedOffset &&
                    backend->config().owned_capacity == kOwnedCapacity,
                "full mapping and allocation-owned extent stay distinct");
    test.Expect(backend->resolve(0, kCapacity, &error) == backend->base(),
                "CxlTransport can resolve the complete shared mapping");

    const auto lookup = backend->metadata_lookup("object", &error);
    test.Expect(!lookup && error.code == CxlPoolErrorCode::kUnsupportedBackend,
                "mapping provider cannot create a second metadata owner");
    test.Expect(backend->alloc("object", 4096, &error) == nullptr &&
                    error.code == CxlPoolErrorCode::kUnsupportedBackend,
                "mapping provider cannot create a second allocator");

    const auto status = backend->status();
    const std::string json = status.ToJson();
    test.Expect(
        status.provider == "mooncake" &&
            status.logical_pool_id == "todo2-native-pool" &&
            status.owned_offset == kOwnedOffset &&
            status.owned_capacity == kOwnedCapacity &&
            json.find("\"owned_offset\":4194304") != std::string::npos &&
            json.find("\"owned_capacity\":4194304") != std::string::npos,
        "safe status reports provider and portable ownership extent");

    backend.reset();
    ClearEnvironment();
    return test.Finish();
}
