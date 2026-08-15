// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "test_support.h"

#include <cstdlib>

using namespace mooncake;
using namespace mooncake::cxl_test;

namespace {

// Compile-time probe for the confidential boundary: an out-of-tree adapter
// needs only the installed public contract, not Mooncake Store internals.
class ContractOnlyBackend final : public CxlPoolBackend {
   public:
    ContractOnlyBackend() {
        config_.logical_pool_id = "contract-only-pool";
        config_.backend_kind = CxlPoolBackendKind::kPrivate;
        config_.capacity = 4096;
    }

    const CxlPoolConfig& config() const noexcept override { return config_; }
    void* base() const noexcept override { return nullptr; }
    void* resolve(std::uint64_t, std::uint64_t, CxlPoolError*) const override {
        return nullptr;
    }
    bool contains(const void*, std::uint64_t) const noexcept override {
        return false;
    }
    std::optional<std::uint64_t> offset_of(const void*, std::uint64_t,
                                           CxlPoolError*) const override {
        return std::nullopt;
    }
    CxlLookupResult metadata_lookup(std::string_view,
                                    CxlPoolError*) const override {
        return std::nullopt;
    }
    std::unique_ptr<CxlAllocation> alloc(std::string_view, std::uint64_t,
                                         CxlPoolError*) override {
        return nullptr;
    }
    CxlPoolStatusSnapshot status() const override {
        CxlPoolStatusSnapshot snapshot;
        snapshot.provider = "contract-only";
        snapshot.logical_pool_id = config_.logical_pool_id;
        snapshot.backend_kind = config_.backend_kind;
        snapshot.capacity = config_.capacity;
        snapshot.lifecycle = "ready";
        return snapshot;
    }

   private:
    CxlPoolConfig config_;
};

std::shared_ptr<CxlPoolBackend> OpenContractOnlyBackend(CxlPoolError*) {
    return std::make_shared<ContractOnlyBackend>();
}

std::shared_ptr<CxlPoolBackend> OpenDifferentContractBackend(CxlPoolError*) {
    return std::make_shared<ContractOnlyBackend>();
}

std::shared_ptr<CxlPoolBackend> OpenInconsistentContractBackend(
    CxlPoolError* error) {
    error->code = CxlPoolErrorCode::kOpenFailed;
    error->operation = "open_private_backend";
    error->field = "private_config";
    error->message = "injected provider open failure";
    return std::make_shared<ContractOnlyBackend>();
}

}  // namespace

int main() {
    TestContext test("T1.1_preflight");
    TemporaryPoolFile file(1U << 20);
    test.Expect(file.valid(), "temporary file-backed pool is available");
    if (!file.valid()) return test.Finish();

    CxlPoolError error;
    auto valid = MakeFileConfig(file.path(), 1U << 20, "rack0-cxl0");
    test.Expect(ValidateCxlPoolConfig(valid, &error),
                "valid file configuration passes preflight");
    auto backend = faketract::OpenMmapCxlPoolBackend(valid, &error);
    test.Expect(backend != nullptr && !error,
                "valid file mapping opens before traffic");
    auto compatibility_backend =
        mooncake::OpenMmapCxlPoolBackend(valid, &error);
    test.Expect(
        compatibility_backend != nullptr &&
            compatibility_backend->status().provider == "faketract",
        "legacy factory delegates to the namespaced FakeTraCT provider");
    ContractOnlyBackend contract_probe;
    test.Expect(contract_probe.base() == nullptr,
                "out-of-tree adapter compiles against the public contract");

    // Provider selection is the runtime seam used by CxlTransport. FakeTraCT
    // remains the phase-one default, while a requested private provider must
    // be explicitly registered and can never silently fall back to FakeTraCT.
    ::setenv("MC_CXL_DEV_PATH", file.path().c_str(), 1);
    ::setenv("MC_CXL_DEV_SIZE", "1048576", 1);
    ::unsetenv("MC_CXL_PROVIDER");
    auto default_provider = OpenCxlPoolBackendFromEnvironment(&error);
    test.Expect(default_provider != nullptr && !error &&
                    default_provider->status().provider == "faketract",
                "provider open defaults to FakeTraCT for TODO 1 compatibility");

    ::setenv("MC_CXL_PROVIDER", "tract-test", 1);
    test.Expect(OpenCxlPoolBackendFromEnvironment(&error) == nullptr &&
                    ErrorIs(error, CxlPoolErrorCode::kProviderUnavailable,
                            "MC_CXL_PROVIDER"),
                "unregistered private provider fails closed without fallback");
    ::unsetenv("MC_CXL_DEV_PATH");
    ::unsetenv("MC_CXL_DEV_SIZE");
    test.Expect(RegisterCxlPoolBackendProvider(
                    "tract-test", &OpenContractOnlyBackend, &error) &&
                    !error,
                "out-of-tree provider factory registers through public API");
    auto private_provider = OpenCxlPoolBackendFromEnvironment(&error);
    test.Expect(
        private_provider != nullptr && !error &&
            private_provider->status().provider == "contract-only",
        "selected private provider opens without FakeTraCT configuration");
    test.Expect(
        !RegisterCxlPoolBackendProvider(
            "tract-test", &OpenDifferentContractBackend, &error) &&
            ErrorIs(error, CxlPoolErrorCode::kProviderRegistrationConflict,
                    "provider_name"),
        "conflicting provider registration is rejected deterministically");
    test.Expect(
        RegisterCxlPoolBackendProvider(
            "tract-inconsistent", &OpenInconsistentContractBackend, &error),
        "provider failure test factory registers");
    ::setenv("MC_CXL_PROVIDER", "tract-inconsistent", 1);
    test.Expect(
        OpenCxlPoolBackendFromEnvironment(&error) == nullptr &&
            ErrorIs(error, CxlPoolErrorCode::kOpenFailed, "private_config"),
        "provider error cannot return a partially usable backend");
    ::unsetenv("MC_CXL_PROVIDER");

    auto bad_alignment = valid;
    bad_alignment.mapping_alignment = 3000;
    test.Expect(!ValidateCxlPoolConfig(bad_alignment, &error) &&
                    ErrorIs(error, CxlPoolErrorCode::kInvalidConfig,
                            "mapping_alignment"),
                "non-power-of-two mapping alignment is actionable");

    auto subpage_alignment = valid;
    subpage_alignment.mapping_alignment = 64;
    test.Expect(!ValidateCxlPoolConfig(subpage_alignment, &error) &&
                    ErrorIs(error, CxlPoolErrorCode::kInvalidConfig,
                            "mapping_alignment"),
                "sub-page mapping alignment is rejected before mmap");

    auto bad_offset = valid;
    bad_offset.mapping_offset = 1;
    test.Expect(
        !ValidateCxlPoolConfig(bad_offset, &error) &&
            ErrorIs(error, CxlPoolErrorCode::kInvalidConfig, "mapping_offset"),
        "misaligned mapping offset is rejected");

    auto absent = valid;
    absent.path = file.path() + ".absent";
    test.Expect(faketract::OpenMmapCxlPoolBackend(absent, &error) == nullptr &&
                    ErrorIs(error, CxlPoolErrorCode::kOpenFailed, "path") &&
                    error.system_error != 0,
                "absent device path reports open failure and errno");

    ::unsetenv("MC_CXL_DEV_PATH");
    CxlPoolConfig environment_config;
    test.Expect(
        !LoadCxlPoolConfigFromEnvironment(&environment_config, &error) &&
            ErrorIs(error, CxlPoolErrorCode::kInvalidConfig, "MC_CXL_DEV_PATH"),
        "missing legacy path environment is diagnosed");

    ::setenv("MC_CXL_DEV_PATH", file.path().c_str(), 1);
    ::setenv("MC_CXL_DEV_SIZE", "not-a-size", 1);
    test.Expect(
        !LoadCxlPoolConfigFromEnvironment(&environment_config, &error) &&
            ErrorIs(error, CxlPoolErrorCode::kInvalidConfig, "MC_CXL_DEV_SIZE"),
        "malformed legacy size is diagnosed");
    ::setenv("MC_CXL_DEV_SIZE", "-1", 1);
    test.Expect(
        !LoadCxlPoolConfigFromEnvironment(&environment_config, &error) &&
            ErrorIs(error, CxlPoolErrorCode::kInvalidConfig, "MC_CXL_DEV_SIZE"),
        "negative legacy size is rejected as unsigned input");
    ::unsetenv("MC_CXL_DEV_PATH");
    ::unsetenv("MC_CXL_DEV_SIZE");

    std::cout << "test=T1.1_preflight tier=T2 status=SKIP "
                 "reason=no_/dev/dax_device_in_portable_test\n";
    return test.Finish();
}
