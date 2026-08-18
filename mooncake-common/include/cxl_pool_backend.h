// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_CXL_POOL_BACKEND_H_
#define MOONCAKE_CXL_POOL_BACKEND_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake {

// CXL is a memory tier. This type intentionally does not derive from the
// Mooncake Store disk/offload StorageBackendInterface.
enum class CxlPoolBackendKind {
    kFile,
    kDevDax,
    // Reserved for an out-of-tree adapter around the confidential CXL pool
    // allocator/index. The open implementation never attempts to construct it.
    kPrivate,
};

enum class CxlPoolErrorCode {
    kOk = 0,
    kInvalidConfig,
    kUnsupportedBackend,
    kOpenFailed,
    kSizeDiscoveryFailed,
    kMapFailed,
    kOutOfBounds,
    kAllocationExhausted,
    kObjectAlreadyExists,
    kReservationConflict,
    kBackendClosed,
    kProviderUnavailable,
    kProviderRegistrationConflict,
};

struct CxlPoolError {
    CxlPoolErrorCode code{CxlPoolErrorCode::kOk};
    std::string operation;
    std::string field;
    std::string message;
    int system_error{0};

    explicit operator bool() const noexcept {
        return code != CxlPoolErrorCode::kOk;
    }
};

const char* ToString(CxlPoolBackendKind kind) noexcept;
const char* ToString(CxlPoolErrorCode code) noexcept;

struct CxlPoolConfig {
    // Stable identity shared by users of one logical CXL pool. It is not a
    // pathname and must not be interpreted as a private allocator identifier.
    std::string logical_pool_id;
    CxlPoolBackendKind backend_kind{CxlPoolBackendKind::kFile};
    std::string path;
    std::uint64_t capacity{0};
    std::uint64_t mapping_offset{0};
    std::uint64_t mapping_alignment{0};
    std::uint64_t allocation_alignment{64};
    // TODO #2 separates the extent mapped for reads from the extent this
    // client contributes to the single Mooncake Master's allocator. Every
    // client maps the complete shared pool, but advertises one disjoint
    // allocation-owned subrange. A zero owned_capacity means the legacy
    // full-pool ownership model.
    std::uint64_t owned_offset{0};
    std::uint64_t owned_capacity{0};
};

// Environment compatibility:
//   existing: MC_CXL_DEV_PATH, MC_CXL_DEV_SIZE
//   new:      MC_CXL_POOL_ID, MC_CXL_BACKEND_KIND,
//             MC_CXL_MAP_OFFSET, MC_CXL_MAP_ALIGNMENT,
//             MC_CXL_ALLOC_ALIGNMENT, MC_CXL_OWNED_OFFSET,
//             MC_CXL_OWNED_SIZE
// If MC_CXL_BACKEND_KIND is absent, /dev/dax* paths select devdax and all
// other paths select the file-backed model.
bool LoadCxlPoolConfigFromEnvironment(CxlPoolConfig* config,
                                      CxlPoolError* error = nullptr);
bool ValidateCxlPoolConfig(const CxlPoolConfig& config,
                           CxlPoolError* error = nullptr);

struct CxlLocation {
    std::string logical_pool_id;
    std::uint64_t offset{0};
    std::uint64_t length{0};
    // Equality-only change token. Callers must not assume monotonicity even
    // though the open model happens to generate increasing values.
    std::uint64_t generation{0};
};

using CxlLookupResult =
    std::optional<CxlLocation>;  // nullopt with no error = MISS

enum class CxlAllocationState {
    kReserved,
    kCommitted,
    kAborted,
};

class CxlAllocation {
   public:
    virtual ~CxlAllocation() = default;

    virtual const CxlLocation& location() const noexcept = 0;
    virtual void* data() const noexcept = 0;
    virtual CxlAllocationState state() const noexcept = 0;

    // Commit is the only publication point. A failed commit automatically
    // aborts the reservation, so no partially written object becomes visible.
    virtual CxlLookupResult commit(CxlPoolError* error = nullptr) = 0;

    // Abort is idempotent. Destruction of a reserved allocation calls abort.
    virtual void abort() noexcept = 0;
};

struct CxlPoolStatusSnapshot {
    // Backend provider name, for example "faketract", "mooncake", or a
    // future private TraCT adapter. This is diagnostic identity, not
    // placement policy.
    std::string provider{"unspecified"};
    std::string logical_pool_id;
    CxlPoolBackendKind backend_kind{CxlPoolBackendKind::kFile};
    std::string path;
    std::uint64_t capacity{0};
    std::uint64_t mapping_offset{0};
    std::uint64_t mapping_alignment{0};
    std::uint64_t allocation_alignment{0};
    std::uint64_t owned_offset{0};
    std::uint64_t owned_capacity{0};
    std::uint64_t reserved_bytes{0};
    std::uint64_t committed_bytes{0};
    std::uint64_t active_reservations{0};
    std::uint64_t committed_objects{0};
    std::uint64_t commit_count{0};
    std::uint64_t abort_count{0};
    std::uint64_t lookup_hit_count{0};
    std::uint64_t lookup_miss_count{0};
    std::string lifecycle{"closed"};
    std::string last_operation{"none"};
    std::string last_result{"none"};
    CxlPoolErrorCode last_error_code{CxlPoolErrorCode::kOk};

    // Safe structured status for black-box debugging. It deliberately omits
    // payload bytes, private metadata, raw pointers, and transport keys.
    std::string ToJson() const;
};

// Public CXLConnector endpoint. The two connector-facing operations are
// metadata_lookup() and alloc(); mapping helpers let Mooncake Transfer Engine
// translate validated offsets without knowing confidential pool internals.
class CxlPoolBackend {
   public:
    virtual ~CxlPoolBackend() = default;

    virtual const CxlPoolConfig& config() const noexcept = 0;
    virtual void* base() const noexcept = 0;

    virtual void* resolve(std::uint64_t offset, std::uint64_t length,
                          CxlPoolError* error = nullptr) const = 0;
    virtual bool contains(const void* address,
                          std::uint64_t length) const noexcept = 0;
    virtual std::optional<std::uint64_t> offset_of(
        const void* address, std::uint64_t length,
        CxlPoolError* error = nullptr) const = 0;

    virtual CxlLookupResult metadata_lookup(
        std::string_view opaque_object_id,
        CxlPoolError* error = nullptr) const = 0;
    virtual std::unique_ptr<CxlAllocation> alloc(
        std::string_view opaque_object_id, std::uint64_t length,
        CxlPoolError* error = nullptr) = 0;

    virtual CxlPoolStatusSnapshot status() const = 0;

    // True only when Mooncake Master, rather than this backend, owns object
    // metadata and extent allocation. The built-in "mooncake" provider uses
    // this mode; FakeTraCT and confidential adapters keep their own contract.
    virtual bool master_managed_allocation() const noexcept { return false; }
};

// A provider factory is the only production hook required by CxlTransport.
// The confidential adapter registers a factory (normally named "tract")
// during process startup; the factory may parse its own private configuration
// and return a backend backed by the real TraCT allocator and prefix index.
// Mooncake never needs to include or link against those private definitions.
using CxlPoolBackendProviderFactory =
    std::shared_ptr<CxlPoolBackend> (*)(CxlPoolError* error);

// Registers an out-of-tree backend provider. Registration is process-local,
// thread-safe, and idempotent only when the same name/factory pair is supplied.
// "faketract" and "mooncake" are reserved built-in providers.
bool RegisterCxlPoolBackendProvider(std::string_view provider_name,
                                    CxlPoolBackendProviderFactory factory,
                                    CxlPoolError* error = nullptr);

// Opens the provider selected by MC_CXL_PROVIDER. The phase-one default is
// "faketract" for compatibility with file/devdax testing. Selecting an
// unavailable provider (for example "tract" before its adapter registers)
// fails closed and never falls back to a second allocator over the same pool.
std::shared_ptr<CxlPoolBackend> OpenCxlPoolBackendFromEnvironment(
    CxlPoolError* error = nullptr);

// Compatibility entry points for the dependency-free model. New development
// code should include faketract/cxl_pool_backend.h and call the explicitly
// namespaced provider. A confidential adapter instead implements
// CxlPoolBackend and can be injected into CxlTransport without exposing its
// private index, allocator, lock, or publication structures.
std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackend(
    CxlPoolConfig config, CxlPoolError* error = nullptr);

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackendFromEnvironment(
    CxlPoolError* error = nullptr);

}  // namespace mooncake

#endif  // MOONCAKE_CXL_POOL_BACKEND_H_
