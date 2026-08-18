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

#include "faketract/cxl_pool_backend.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <mutex>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

constexpr std::uint64_t kDefaultDevDaxAlignment = 2ULL * 1024 * 1024;
constexpr std::uint64_t kDefaultAllocationAlignment = 64;
constexpr std::string_view kFakeTraCTProvider = "faketract";
constexpr std::string_view kMooncakeProvider = "mooncake";

using CxlPoolBackendProviderRegistry =
    std::unordered_map<std::string, CxlPoolBackendProviderFactory>;

CxlPoolBackendProviderRegistry& ProviderRegistry() {
    static CxlPoolBackendProviderRegistry registry;
    return registry;
}

std::mutex& ProviderRegistryMutex() {
    static std::mutex mutex;
    return mutex;
}

void ClearError(CxlPoolError* error) {
    if (error != nullptr) {
        *error = {};
    }
}

void SetError(CxlPoolError* error, CxlPoolErrorCode code, std::string operation,
              std::string field, std::string message, int system_error = 0) {
    if (error == nullptr) {
        return;
    }
    error->code = code;
    error->operation = std::move(operation);
    error->field = std::move(field);
    error->message = std::move(message);
    error->system_error = system_error;
}

bool IsPowerOfTwo(std::uint64_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

bool AddOverflows(std::uint64_t left, std::uint64_t right) {
    return right > std::numeric_limits<std::uint64_t>::max() - left;
}

std::optional<std::uint64_t> ParseUnsignedEnvironment(const char* name,
                                                      bool required,
                                                      CxlPoolError* error) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        if (required) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config",
                     name, std::string(name) + " is required");
        }
        return std::nullopt;
    }
    if (*raw == '-') {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config", name,
                 std::string(name) + " must not be negative");
        return std::nullopt;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(raw, &end, 0);
    if (errno == ERANGE || end == raw || *end != '\0') {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config", name,
                 std::string(name) + " must be an unsigned integer");
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(value);
}

std::uint64_t SystemPageSize() {
    const long page_size = ::sysconf(_SC_PAGESIZE);
    return page_size > 0 ? static_cast<std::uint64_t>(page_size) : 4096;
}

std::optional<std::uint64_t> DiscoverDevDaxSize(const std::string& path,
                                                CxlPoolError* error) {
#ifdef __linux__
    std::regex dax_pattern(R"(dax[0-9]+\.[0-9]+)");
    std::smatch match;
    if (!std::regex_search(path, match, dax_pattern)) {
        SetError(error, CxlPoolErrorCode::kSizeDiscoveryFailed, "discover_size",
                 "path", "cannot derive dax device name from path");
        return std::nullopt;
    }

    const std::string size_path =
        "/sys/bus/dax/devices/" + match.str() + "/size";
    std::ifstream input(size_path);
    std::string raw;
    if (!input.is_open() || !std::getline(input, raw)) {
        SetError(error, CxlPoolErrorCode::kSizeDiscoveryFailed, "discover_size",
                 "capacity", "cannot read devdax capacity from " + size_path,
                 errno);
        return std::nullopt;
    }

    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(raw.c_str(), &end, 0);
    if (errno == ERANGE || end == raw.c_str() ||
        (*end != '\0' && *end != '\n') || value == 0) {
        SetError(error, CxlPoolErrorCode::kSizeDiscoveryFailed, "discover_size",
                 "capacity", "devdax capacity is malformed in " + size_path);
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(value);
#else
    (void)path;
    SetError(error, CxlPoolErrorCode::kSizeDiscoveryFailed, "discover_size",
             "capacity",
             "automatic devdax size discovery is available only on Linux");
    return std::nullopt;
#endif
}

std::string EscapeJson(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped += ch;
                break;
        }
    }
    return escaped;
}

}  // namespace

namespace faketract {

struct Extent {
    std::uint64_t offset{0};
    std::uint64_t length{0};
};

class MmapCxlPoolBackend;

// One FakeTraCT reservation. It wraps a writable mapped extent and guarantees
// that the fake metadata index sees it only after commit(). Production TraCT
// should provide an equivalent handle backed by its own transaction/lifetime
// rules instead of reusing this allocator state.
class MmapCxlAllocation final : public CxlAllocation {
   public:
    MmapCxlAllocation(std::shared_ptr<MmapCxlPoolBackend> owner,
                      std::uint64_t reservation_id, std::string object_id,
                      CxlLocation location, void* data)
        : owner_(std::move(owner)),
          reservation_id_(reservation_id),
          object_id_(std::move(object_id)),
          location_(std::move(location)),
          data_(data) {}

    ~MmapCxlAllocation() override { abort(); }

    const CxlLocation& location() const noexcept override { return location_; }
    void* data() const noexcept override { return data_; }
    CxlAllocationState state() const noexcept override {
        return state_.load(std::memory_order_acquire);
    }

    CxlLookupResult commit(CxlPoolError* error) override;
    void abort() noexcept override;

   private:
    std::shared_ptr<MmapCxlPoolBackend> owner_;
    const std::uint64_t reservation_id_;
    const std::string object_id_;
    CxlLocation location_;
    void* data_{nullptr};
    std::atomic<CxlAllocationState> state_{CxlAllocationState::kReserved};
    std::mutex terminal_mutex_;
};

// Open behavioral model of the confidential TraCT boundary. It owns the
// file/devdax mapping plus a deliberately small in-process extent allocator
// and committed-object index. It is suitable for Mooncake integration tests;
// it is not the allocator for a live legacy TraCT pool.
class MmapCxlPoolBackend final
    : public CxlPoolBackend,
      public std::enable_shared_from_this<MmapCxlPoolBackend> {
   public:
    MmapCxlPoolBackend(CxlPoolConfig config, int fd, void* base)
        : config_(std::move(config)), fd_(fd), base_(base) {
        status_.provider = "faketract";
        status_.logical_pool_id = config_.logical_pool_id;
        status_.backend_kind = config_.backend_kind;
        status_.path = config_.path;
        status_.capacity = config_.capacity;
        status_.mapping_offset = config_.mapping_offset;
        status_.mapping_alignment = config_.mapping_alignment;
        status_.allocation_alignment = config_.allocation_alignment;
        status_.owned_offset = config_.owned_offset;
        status_.owned_capacity = config_.owned_capacity;
        status_.lifecycle = "ready";
        status_.last_operation = "open";
        status_.last_result = "ok";
    }

    ~MmapCxlPoolBackend() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            status_.lifecycle = "closed";
            status_.last_operation = "close";
            status_.last_result = "ok";
        }
        if (base_ != nullptr && base_ != MAP_FAILED && config_.capacity != 0) {
            ::munmap(base_, static_cast<std::size_t>(config_.capacity));
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    const CxlPoolConfig& config() const noexcept override { return config_; }
    void* base() const noexcept override { return base_; }

    void* resolve(std::uint64_t offset, std::uint64_t length,
                  CxlPoolError* error) const override {
        ClearError(error);
        if (base_ == nullptr || base_ == MAP_FAILED) {
            SetError(error, CxlPoolErrorCode::kBackendClosed, "resolve",
                     "backend", "CXL pool mapping is not open");
            return nullptr;
        }
        if (offset > config_.capacity || length > config_.capacity - offset) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "resolve", "extent",
                     "offset and length exceed CXL pool capacity");
            return nullptr;
        }
        return static_cast<char*>(base_) + offset;
    }

    bool contains(const void* address,
                  std::uint64_t length) const noexcept override {
        if (address == nullptr || base_ == nullptr || base_ == MAP_FAILED) {
            return false;
        }
        const auto base_value = reinterpret_cast<std::uintptr_t>(base_);
        const auto address_value = reinterpret_cast<std::uintptr_t>(address);
        if (address_value < base_value) {
            return false;
        }
        const std::uint64_t offset = address_value - base_value;
        return offset <= config_.capacity &&
               length <= config_.capacity - offset;
    }

    std::optional<std::uint64_t> offset_of(const void* address,
                                           std::uint64_t length,
                                           CxlPoolError* error) const override {
        ClearError(error);
        if (address == nullptr || base_ == nullptr || base_ == MAP_FAILED) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "offset_of",
                     "address", "address or CXL mapping is null");
            return std::nullopt;
        }

        const auto base_value = reinterpret_cast<std::uintptr_t>(base_);
        const auto address_value = reinterpret_cast<std::uintptr_t>(address);
        if (address_value < base_value) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "offset_of",
                     "address", "address precedes CXL mapping");
            return std::nullopt;
        }
        const std::uint64_t offset = address_value - base_value;
        if (offset > config_.capacity || length > config_.capacity - offset) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "offset_of",
                     "extent", "address range exceeds CXL pool capacity");
            return std::nullopt;
        }
        return offset;
    }

    CxlLookupResult metadata_lookup(std::string_view opaque_object_id,
                                    CxlPoolError* error) const override {
        ClearError(error);
        std::lock_guard<std::mutex> lock(mutex_);
        status_.last_operation = "metadata_lookup";
        if (opaque_object_id.empty()) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "metadata_lookup",
                     "opaque_object_id",
                     "opaque object identity must not be empty");
            RecordErrorLocked(CxlPoolErrorCode::kInvalidConfig);
            return std::nullopt;
        }
        const auto iter = committed_.find(std::string(opaque_object_id));
        if (iter == committed_.end()) {
            ++status_.lookup_miss_count;
            status_.last_result = "miss";
            status_.last_error_code = CxlPoolErrorCode::kOk;
            return std::nullopt;
        }
        ++status_.lookup_hit_count;
        status_.last_result = "hit";
        status_.last_error_code = CxlPoolErrorCode::kOk;
        return iter->second;
    }

    std::unique_ptr<CxlAllocation> alloc(std::string_view opaque_object_id,
                                         std::uint64_t length,
                                         CxlPoolError* error) override {
        ClearError(error);
        std::lock_guard<std::mutex> lock(mutex_);
        status_.last_operation = "alloc";
        if (opaque_object_id.empty() || length == 0) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "alloc",
                     opaque_object_id.empty() ? "opaque_object_id" : "length",
                     "object identity and allocation length must be non-zero");
            RecordErrorLocked(CxlPoolErrorCode::kInvalidConfig);
            return nullptr;
        }

        const std::string object_id(opaque_object_id);
        if (committed_.contains(object_id) ||
            pending_by_object_.contains(object_id)) {
            SetError(error, CxlPoolErrorCode::kObjectAlreadyExists, "alloc",
                     "opaque_object_id",
                     "object is already committed or reserved");
            RecordErrorLocked(CxlPoolErrorCode::kObjectAlreadyExists);
            return nullptr;
        }

        const auto aligned_length = AlignLength(length, error);
        if (!aligned_length.has_value()) {
            RecordErrorLocked(error == nullptr
                                  ? CxlPoolErrorCode::kInvalidConfig
                                  : error->code);
            return nullptr;
        }

        const auto extent = ReserveExtentLocked(*aligned_length);
        if (!extent.has_value()) {
            SetError(error, CxlPoolErrorCode::kAllocationExhausted, "alloc",
                     "capacity", "CXL pool has no extent large enough");
            RecordErrorLocked(CxlPoolErrorCode::kAllocationExhausted);
            return nullptr;
        }

        const std::uint64_t reservation_id = next_reservation_id_++;
        pending_.emplace(reservation_id,
                         Reservation{object_id, *extent, length});
        pending_by_object_.emplace(object_id, reservation_id);
        status_.reserved_bytes += extent->length;
        ++status_.active_reservations;
        status_.last_result = "reserved";
        status_.last_error_code = CxlPoolErrorCode::kOk;

        CxlLocation location{config_.logical_pool_id, extent->offset, length,
                             0};
        void* data = static_cast<char*>(base_) + extent->offset;
        return std::make_unique<MmapCxlAllocation>(shared_from_this(),
                                                   reservation_id, object_id,
                                                   std::move(location), data);
    }

    CxlPoolStatusSnapshot status() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return status_;
    }

    CxlLookupResult CommitReservation(std::uint64_t reservation_id,
                                      const std::string& object_id,
                                      CxlPoolError* error) {
        ClearError(error);
        std::lock_guard<std::mutex> lock(mutex_);
        status_.last_operation = "commit";
        const auto pending_iter = pending_.find(reservation_id);
        if (pending_iter == pending_.end() ||
            pending_iter->second.object_id != object_id) {
            SetError(error, CxlPoolErrorCode::kReservationConflict, "commit",
                     "reservation", "reservation is absent or does not match");
            RecordErrorLocked(CxlPoolErrorCode::kReservationConflict);
            return std::nullopt;
        }
        if (committed_.contains(object_id)) {
            SetError(error, CxlPoolErrorCode::kObjectAlreadyExists, "commit",
                     "opaque_object_id", "object became visible before commit");
            RecordErrorLocked(CxlPoolErrorCode::kObjectAlreadyExists);
            return std::nullopt;
        }

        CxlLocation location{
            config_.logical_pool_id, pending_iter->second.extent.offset,
            pending_iter->second.requested_length, next_generation_++};
        committed_.emplace(object_id, location);
        status_.reserved_bytes -= pending_iter->second.extent.length;
        status_.committed_bytes += pending_iter->second.extent.length;
        --status_.active_reservations;
        ++status_.committed_objects;
        ++status_.commit_count;
        pending_by_object_.erase(object_id);
        pending_.erase(pending_iter);
        status_.last_result = "committed";
        status_.last_error_code = CxlPoolErrorCode::kOk;
        return location;
    }

    void AbortReservation(std::uint64_t reservation_id,
                          const std::string& object_id) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            status_.last_operation = "abort";
            const auto pending_iter = pending_.find(reservation_id);
            if (pending_iter == pending_.end() ||
                pending_iter->second.object_id != object_id) {
                status_.last_result = "idempotent";
                status_.last_error_code = CxlPoolErrorCode::kOk;
                return;
            }
            status_.reserved_bytes -= pending_iter->second.extent.length;
            --status_.active_reservations;
            ++status_.abort_count;
            ReleaseExtentLocked(pending_iter->second.extent);
            pending_by_object_.erase(object_id);
            pending_.erase(pending_iter);
            status_.last_result = "aborted";
            status_.last_error_code = CxlPoolErrorCode::kOk;
        } catch (...) {
            // abort() is a cleanup path and must not escape from a destructor.
        }
    }

   private:
    struct Reservation {
        std::string object_id;
        Extent extent;
        std::uint64_t requested_length{0};
    };

    std::optional<std::uint64_t> AlignLength(std::uint64_t length,
                                             CxlPoolError* error) const {
        const std::uint64_t alignment = config_.allocation_alignment;
        const std::uint64_t mask = alignment - 1;
        if (AddOverflows(length, mask)) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "alloc", "length",
                     "allocation length overflows after alignment");
            return std::nullopt;
        }
        return (length + mask) & ~mask;
    }

    std::optional<Extent> ReserveExtentLocked(std::uint64_t length) {
        for (auto iter = free_extents_.begin(); iter != free_extents_.end();
             ++iter) {
            if (iter->length < length) {
                continue;
            }
            Extent result{iter->offset, length};
            iter->offset += length;
            iter->length -= length;
            if (iter->length == 0) {
                free_extents_.erase(iter);
            }
            return result;
        }
        if (next_offset_ > config_.capacity ||
            length > config_.capacity - next_offset_) {
            return std::nullopt;
        }
        Extent result{next_offset_, length};
        next_offset_ += length;
        return result;
    }

    void ReleaseExtentLocked(Extent extent) {
        free_extents_.push_back(extent);
        std::sort(free_extents_.begin(), free_extents_.end(),
                  [](const Extent& left, const Extent& right) {
                      return left.offset < right.offset;
                  });
        std::vector<Extent> coalesced;
        coalesced.reserve(free_extents_.size());
        for (const Extent& candidate : free_extents_) {
            if (!coalesced.empty() &&
                coalesced.back().offset + coalesced.back().length ==
                    candidate.offset) {
                coalesced.back().length += candidate.length;
            } else {
                coalesced.push_back(candidate);
            }
        }
        free_extents_ = std::move(coalesced);
        // Return a free tail to the bump frontier. Without this, repeated
        // aborts could report false exhaustion even when no bytes are live.
        while (!free_extents_.empty()) {
            const Extent& tail = free_extents_.back();
            if (tail.offset + tail.length != next_offset_) {
                break;
            }
            next_offset_ = tail.offset;
            free_extents_.pop_back();
        }
    }

    void RecordErrorLocked(CxlPoolErrorCode code) const {
        status_.last_result = "error";
        status_.last_error_code = code;
    }

    CxlPoolConfig config_;
    int fd_{-1};
    void* base_{nullptr};
    mutable std::mutex mutex_;
    mutable CxlPoolStatusSnapshot status_;
    std::uint64_t next_offset_{0};
    std::uint64_t next_reservation_id_{1};
    std::uint64_t next_generation_{1};
    std::vector<Extent> free_extents_;
    std::unordered_map<std::uint64_t, Reservation> pending_;
    std::unordered_map<std::string, std::uint64_t> pending_by_object_;
    std::unordered_map<std::string, CxlLocation> committed_;
};

CxlLookupResult MmapCxlAllocation::commit(CxlPoolError* error) {
    std::lock_guard<std::mutex> lock(terminal_mutex_);
    if (state_.load(std::memory_order_relaxed) !=
        CxlAllocationState::kReserved) {
        SetError(error, CxlPoolErrorCode::kReservationConflict, "commit",
                 "reservation", "allocation already reached a terminal state");
        return std::nullopt;
    }

    auto result = owner_->CommitReservation(reservation_id_, object_id_, error);
    if (result.has_value()) {
        location_ = *result;
        state_.store(CxlAllocationState::kCommitted, std::memory_order_release);
        return result;
    }

    owner_->AbortReservation(reservation_id_, object_id_);
    state_.store(CxlAllocationState::kAborted, std::memory_order_release);
    return std::nullopt;
}

void MmapCxlAllocation::abort() noexcept {
    try {
        std::lock_guard<std::mutex> lock(terminal_mutex_);
        if (state_.load(std::memory_order_relaxed) !=
            CxlAllocationState::kReserved) {
            return;
        }
        owner_->AbortReservation(reservation_id_, object_id_);
        state_.store(CxlAllocationState::kAborted, std::memory_order_release);
    } catch (...) {
        // Destruction must not throw. The backend's own cleanup is also
        // noexcept and idempotent.
    }
}

}  // namespace faketract

namespace {

// Mapping-only backend for the Mooncake-owned CXL topology. Unlike
// FakeTraCT, this object intentionally has no object index and no extent
// allocator: Mooncake Master owns both through SegmentManager and its replica
// metadata. The backend's sole data-plane job is safe shared-pool offset
// translation for CxlTransport.
class MooncakeMmapCxlPoolBackend final : public CxlPoolBackend {
   public:
    MooncakeMmapCxlPoolBackend(CxlPoolConfig config, int fd, void* base)
        : config_(std::move(config)), fd_(fd), base_(base) {
        status_.provider = std::string(kMooncakeProvider);
        status_.logical_pool_id = config_.logical_pool_id;
        status_.backend_kind = config_.backend_kind;
        status_.path = config_.path;
        status_.capacity = config_.capacity;
        status_.mapping_offset = config_.mapping_offset;
        status_.mapping_alignment = config_.mapping_alignment;
        status_.allocation_alignment = config_.allocation_alignment;
        status_.owned_offset = config_.owned_offset;
        status_.owned_capacity = config_.owned_capacity;
        status_.lifecycle = "ready";
        status_.last_operation = "open";
        status_.last_result = "ok";
    }

    ~MooncakeMmapCxlPoolBackend() override {
        if (base_ != nullptr && base_ != MAP_FAILED && config_.capacity != 0) {
            ::munmap(base_, static_cast<std::size_t>(config_.capacity));
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    const CxlPoolConfig& config() const noexcept override { return config_; }
    void* base() const noexcept override { return base_; }

    void* resolve(std::uint64_t offset, std::uint64_t length,
                  CxlPoolError* error) const override {
        ClearError(error);
        if (base_ == nullptr || base_ == MAP_FAILED) {
            SetError(error, CxlPoolErrorCode::kBackendClosed, "resolve",
                     "backend", "CXL pool mapping is not open");
            return nullptr;
        }
        if (offset > config_.capacity || length > config_.capacity - offset) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "resolve", "extent",
                     "offset and length exceed CXL pool capacity");
            return nullptr;
        }
        return static_cast<char*>(base_) + offset;
    }

    bool contains(const void* address,
                  std::uint64_t length) const noexcept override {
        if (address == nullptr || base_ == nullptr || base_ == MAP_FAILED) {
            return false;
        }
        const auto base_value = reinterpret_cast<std::uintptr_t>(base_);
        const auto address_value = reinterpret_cast<std::uintptr_t>(address);
        if (address_value < base_value) {
            return false;
        }
        const std::uint64_t offset = address_value - base_value;
        return offset <= config_.capacity &&
               length <= config_.capacity - offset;
    }

    std::optional<std::uint64_t> offset_of(const void* address,
                                           std::uint64_t length,
                                           CxlPoolError* error) const override {
        ClearError(error);
        if (!contains(address, length)) {
            SetError(error, CxlPoolErrorCode::kOutOfBounds, "offset_of",
                     "extent", "address range exceeds CXL pool capacity");
            return std::nullopt;
        }
        return reinterpret_cast<std::uintptr_t>(address) -
               reinterpret_cast<std::uintptr_t>(base_);
    }

    CxlLookupResult metadata_lookup(std::string_view,
                                    CxlPoolError* error) const override {
        SetError(error, CxlPoolErrorCode::kUnsupportedBackend,
                 "metadata_lookup", "ownership",
                 "Mooncake Master owns CXL object metadata");
        return std::nullopt;
    }

    std::unique_ptr<CxlAllocation> alloc(std::string_view, std::uint64_t,
                                         CxlPoolError* error) override {
        SetError(error, CxlPoolErrorCode::kUnsupportedBackend, "alloc",
                 "ownership", "Mooncake Master owns CXL extent allocation");
        return nullptr;
    }

    CxlPoolStatusSnapshot status() const override { return status_; }
    bool master_managed_allocation() const noexcept override { return true; }

   private:
    CxlPoolConfig config_;
    int fd_{-1};
    void* base_{nullptr};
    CxlPoolStatusSnapshot status_;
};

}  // namespace

const char* ToString(CxlPoolBackendKind kind) noexcept {
    switch (kind) {
        case CxlPoolBackendKind::kFile:
            return "file";
        case CxlPoolBackendKind::kDevDax:
            return "devdax";
        case CxlPoolBackendKind::kPrivate:
            return "private";
    }
    return "unknown";
}

const char* ToString(CxlPoolErrorCode code) noexcept {
    switch (code) {
        case CxlPoolErrorCode::kOk:
            return "ok";
        case CxlPoolErrorCode::kInvalidConfig:
            return "invalid_config";
        case CxlPoolErrorCode::kUnsupportedBackend:
            return "unsupported_backend";
        case CxlPoolErrorCode::kOpenFailed:
            return "open_failed";
        case CxlPoolErrorCode::kSizeDiscoveryFailed:
            return "size_discovery_failed";
        case CxlPoolErrorCode::kMapFailed:
            return "map_failed";
        case CxlPoolErrorCode::kOutOfBounds:
            return "out_of_bounds";
        case CxlPoolErrorCode::kAllocationExhausted:
            return "allocation_exhausted";
        case CxlPoolErrorCode::kObjectAlreadyExists:
            return "object_already_exists";
        case CxlPoolErrorCode::kReservationConflict:
            return "reservation_conflict";
        case CxlPoolErrorCode::kBackendClosed:
            return "backend_closed";
        case CxlPoolErrorCode::kProviderUnavailable:
            return "provider_unavailable";
        case CxlPoolErrorCode::kProviderRegistrationConflict:
            return "provider_registration_conflict";
    }
    return "unknown";
}

bool LoadCxlPoolConfigFromEnvironment(CxlPoolConfig* config,
                                      CxlPoolError* error) {
    ClearError(error);
    if (config == nullptr) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config",
                 "config", "configuration output pointer is null");
        return false;
    }

    const char* path = std::getenv("MC_CXL_DEV_PATH");
    if (path == nullptr || *path == '\0') {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config",
                 "MC_CXL_DEV_PATH", "MC_CXL_DEV_PATH is required");
        return false;
    }

    CxlPoolConfig parsed;
    parsed.path = path;
    const char* backend = std::getenv("MC_CXL_BACKEND_KIND");
    if (backend == nullptr || *backend == '\0') {
        parsed.backend_kind = parsed.path.rfind("/dev/dax", 0) == 0
                                  ? CxlPoolBackendKind::kDevDax
                                  : CxlPoolBackendKind::kFile;
    } else if (std::strcmp(backend, "file") == 0) {
        parsed.backend_kind = CxlPoolBackendKind::kFile;
    } else if (std::strcmp(backend, "devdax") == 0) {
        parsed.backend_kind = CxlPoolBackendKind::kDevDax;
    } else if (std::strcmp(backend, "private") == 0) {
        parsed.backend_kind = CxlPoolBackendKind::kPrivate;
    } else {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "parse_config",
                 "MC_CXL_BACKEND_KIND",
                 "backend kind must be file, devdax, or private");
        return false;
    }

    const char* pool_id = std::getenv("MC_CXL_POOL_ID");
    // Path fallback preserves old deployments. Multi-rack deployments should
    // always set an explicit stable pool ID.
    parsed.logical_pool_id =
        pool_id != nullptr && *pool_id != '\0' ? pool_id : parsed.path;

    CxlPoolError parse_error;
    const auto capacity =
        ParseUnsignedEnvironment("MC_CXL_DEV_SIZE", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.capacity = capacity.value_or(0);

    const auto mapping_offset =
        ParseUnsignedEnvironment("MC_CXL_MAP_OFFSET", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.mapping_offset = mapping_offset.value_or(0);

    const auto mapping_alignment =
        ParseUnsignedEnvironment("MC_CXL_MAP_ALIGNMENT", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.mapping_alignment = mapping_alignment.value_or(
        parsed.backend_kind == CxlPoolBackendKind::kDevDax
            ? kDefaultDevDaxAlignment
            : SystemPageSize());

    const auto allocation_alignment =
        ParseUnsignedEnvironment("MC_CXL_ALLOC_ALIGNMENT", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.allocation_alignment =
        allocation_alignment.value_or(kDefaultAllocationAlignment);

    const auto owned_offset =
        ParseUnsignedEnvironment("MC_CXL_OWNED_OFFSET", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.owned_offset = owned_offset.value_or(0);

    const auto owned_capacity =
        ParseUnsignedEnvironment("MC_CXL_OWNED_SIZE", false, &parse_error);
    if (parse_error) {
        if (error != nullptr) *error = std::move(parse_error);
        return false;
    }
    parsed.owned_capacity = owned_capacity.value_or(0);

    *config = std::move(parsed);
    return ValidateCxlPoolConfig(*config, error);
}

bool ValidateCxlPoolConfig(const CxlPoolConfig& config, CxlPoolError* error) {
    ClearError(error);
    if (config.logical_pool_id.empty()) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "logical_pool_id", "logical pool ID must not be empty");
        return false;
    }
    if (config.backend_kind == CxlPoolBackendKind::kPrivate) {
        SetError(error, CxlPoolErrorCode::kUnsupportedBackend,
                 "validate_config", "backend_kind",
                 "private backend must be supplied by an out-of-tree adapter");
        return false;
    }
    if (config.path.empty()) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "path", "CXL file/devdax path must not be empty");
        return false;
    }
    if (config.capacity == 0 &&
        config.backend_kind != CxlPoolBackendKind::kDevDax) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "capacity", "file-backed CXL capacity must be non-zero");
        return false;
    }
    if (!IsPowerOfTwo(config.mapping_alignment)) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "mapping_alignment",
                 "mapping alignment must be a non-zero power of two");
        return false;
    }
    if (config.mapping_alignment < SystemPageSize()) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "mapping_alignment",
                 "mapping alignment must be at least the system page size");
        return false;
    }
    if (!IsPowerOfTwo(config.allocation_alignment)) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "allocation_alignment",
                 "allocation alignment must be a non-zero power of two");
        return false;
    }
    if (config.mapping_offset % config.mapping_alignment != 0) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "mapping_offset",
                 "mapping offset must be aligned to mapping_alignment");
        return false;
    }
    if (config.capacity != 0 &&
        config.backend_kind == CxlPoolBackendKind::kDevDax &&
        config.capacity % config.mapping_alignment != 0) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "capacity",
                 "devdax capacity must be aligned to mapping_alignment");
        return false;
    }
    if (AddOverflows(config.mapping_offset, config.capacity)) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "capacity", "mapping offset plus capacity overflows");
        return false;
    }
    if (config.owned_capacity == 0 && config.owned_offset != 0) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                 "owned_capacity",
                 "owned offset requires a non-zero owned capacity");
        return false;
    }
    if (config.owned_capacity != 0) {
        if (config.owned_offset % config.allocation_alignment != 0 ||
            config.owned_capacity % config.allocation_alignment != 0) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                     "owned_extent",
                     "owned offset and capacity must be allocation aligned");
            return false;
        }
        if (AddOverflows(config.owned_offset, config.owned_capacity) ||
            (config.capacity != 0 &&
             (config.owned_offset > config.capacity ||
              config.owned_capacity > config.capacity - config.owned_offset))) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "validate_config",
                     "owned_extent",
                     "owned extent exceeds the mapped CXL pool");
            return false;
        }
    }
    return true;
}

std::string CxlPoolStatusSnapshot::ToJson() const {
    std::ostringstream output;
    output << "{\"component\":\"cxl_pool_backend\""
           << ",\"provider\":\"" << EscapeJson(provider)
           << "\",\"logical_pool_id\":\"" << EscapeJson(logical_pool_id)
           << "\",\"backend_kind\":\"" << ToString(backend_kind)
           << "\",\"path\":\"" << EscapeJson(path)
           << "\",\"capacity\":" << capacity
           << ",\"mapping_offset\":" << mapping_offset
           << ",\"mapping_alignment\":" << mapping_alignment
           << ",\"allocation_alignment\":" << allocation_alignment
           << ",\"owned_offset\":" << owned_offset
           << ",\"owned_capacity\":" << owned_capacity
           << ",\"reserved_bytes\":" << reserved_bytes
           << ",\"committed_bytes\":" << committed_bytes
           << ",\"active_reservations\":" << active_reservations
           << ",\"committed_objects\":" << committed_objects
           << ",\"commit_count\":" << commit_count
           << ",\"abort_count\":" << abort_count
           << ",\"lookup_hit_count\":" << lookup_hit_count
           << ",\"lookup_miss_count\":" << lookup_miss_count
           << ",\"lifecycle\":\"" << EscapeJson(lifecycle)
           << "\",\"last_operation\":\"" << EscapeJson(last_operation)
           << "\",\"last_result\":\"" << EscapeJson(last_result)
           << "\",\"last_error_code\":\"" << ToString(last_error_code) << "\"}";
    return output.str();
}

namespace faketract {

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackend(CxlPoolConfig config,
                                                       CxlPoolError* error) {
    ClearError(error);
    if (!ValidateCxlPoolConfig(config, error)) {
        return nullptr;
    }
    if (config.capacity == 0) {
        auto discovered = DiscoverDevDaxSize(config.path, error);
        if (!discovered.has_value()) {
            return nullptr;
        }
        if (*discovered <= config.mapping_offset) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "open",
                     "mapping_offset",
                     "mapping offset is outside discovered devdax capacity");
            return nullptr;
        }
        config.capacity = *discovered - config.mapping_offset;
        if (!ValidateCxlPoolConfig(config, error)) {
            return nullptr;
        }
    }

    if (config.capacity > std::numeric_limits<std::size_t>::max() ||
        config.mapping_offset >
            static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "open", "extent",
                 "mapping extent is not representable on this platform");
        return nullptr;
    }

    const int fd = ::open(config.path.c_str(), O_RDWR);
    if (fd < 0) {
        SetError(
            error, CxlPoolErrorCode::kOpenFailed, "open", "path",
            "cannot open CXL pool path: " + std::string(std::strerror(errno)),
            errno);
        return nullptr;
    }

    if (config.backend_kind == CxlPoolBackendKind::kFile) {
        struct stat stat_buffer{};
        if (::fstat(fd, &stat_buffer) != 0) {
            const int saved_errno = errno;
            ::close(fd);
            SetError(error, CxlPoolErrorCode::kOpenFailed, "fstat", "path",
                     "cannot inspect file-backed CXL pool: " +
                         std::string(std::strerror(saved_errno)),
                     saved_errno);
            return nullptr;
        }
        const std::uint64_t required_size =
            config.mapping_offset + config.capacity;
        if (stat_buffer.st_size < 0 ||
            static_cast<std::uint64_t>(stat_buffer.st_size) < required_size) {
            ::close(fd);
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "open",
                     "capacity",
                     "file-backed CXL pool is smaller than mapping extent");
            return nullptr;
        }
    }

    void* base = ::mmap(nullptr, static_cast<std::size_t>(config.capacity),
                        PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                        static_cast<off_t>(config.mapping_offset));
    if (base == MAP_FAILED) {
        const int saved_errno = errno;
        ::close(fd);
        SetError(
            error, CxlPoolErrorCode::kMapFailed, "mmap", "extent",
            "cannot map CXL pool: " + std::string(std::strerror(saved_errno)),
            saved_errno);
        return nullptr;
    }

    return std::make_shared<MmapCxlPoolBackend>(std::move(config), fd, base);
}

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackendFromEnvironment(
    CxlPoolError* error) {
    CxlPoolConfig config;
    if (!LoadCxlPoolConfigFromEnvironment(&config, error)) {
        return nullptr;
    }
    return faketract::OpenMmapCxlPoolBackend(std::move(config), error);
}

}  // namespace faketract

namespace {

std::shared_ptr<CxlPoolBackend> OpenMooncakeMmapCxlPoolBackend(
    CxlPoolConfig config, CxlPoolError* error) {
    ClearError(error);
    if (!ValidateCxlPoolConfig(config, error)) {
        return nullptr;
    }
    if (config.capacity == 0) {
        auto discovered = DiscoverDevDaxSize(config.path, error);
        if (!discovered.has_value()) {
            return nullptr;
        }
        if (*discovered <= config.mapping_offset) {
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "open",
                     "mapping_offset",
                     "mapping offset is outside discovered devdax capacity");
            return nullptr;
        }
        config.capacity = *discovered - config.mapping_offset;
        if (!ValidateCxlPoolConfig(config, error)) {
            return nullptr;
        }
    }
    if (config.owned_capacity == 0) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "open_backend",
                 "MC_CXL_OWNED_SIZE",
                 "the Mooncake provider requires a non-zero owned extent");
        return nullptr;
    }
    if (config.capacity > std::numeric_limits<std::size_t>::max() ||
        config.mapping_offset >
            static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "open", "extent",
                 "mapping extent is not representable on this platform");
        return nullptr;
    }

    const int fd = ::open(config.path.c_str(), O_RDWR);
    if (fd < 0) {
        SetError(
            error, CxlPoolErrorCode::kOpenFailed, "open", "path",
            "cannot open CXL pool path: " + std::string(std::strerror(errno)),
            errno);
        return nullptr;
    }
    if (config.backend_kind == CxlPoolBackendKind::kFile) {
        struct stat stat_buffer{};
        if (::fstat(fd, &stat_buffer) != 0) {
            const int saved_errno = errno;
            ::close(fd);
            SetError(error, CxlPoolErrorCode::kOpenFailed, "fstat", "path",
                     "cannot inspect file-backed CXL pool: " +
                         std::string(std::strerror(saved_errno)),
                     saved_errno);
            return nullptr;
        }
        const std::uint64_t required_size =
            config.mapping_offset + config.capacity;
        if (stat_buffer.st_size < 0 ||
            static_cast<std::uint64_t>(stat_buffer.st_size) < required_size) {
            ::close(fd);
            SetError(error, CxlPoolErrorCode::kInvalidConfig, "open",
                     "capacity",
                     "file-backed CXL pool is smaller than mapping extent");
            return nullptr;
        }
    }

    void* base = ::mmap(nullptr, static_cast<std::size_t>(config.capacity),
                        PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                        static_cast<off_t>(config.mapping_offset));
    if (base == MAP_FAILED) {
        const int saved_errno = errno;
        ::close(fd);
        SetError(
            error, CxlPoolErrorCode::kMapFailed, "mmap", "extent",
            "cannot map CXL pool: " + std::string(std::strerror(saved_errno)),
            saved_errno);
        return nullptr;
    }
    return std::make_shared<MooncakeMmapCxlPoolBackend>(std::move(config), fd,
                                                        base);
}

std::shared_ptr<CxlPoolBackend> OpenMooncakeMmapCxlPoolBackendFromEnvironment(
    CxlPoolError* error) {
    CxlPoolConfig config;
    if (!LoadCxlPoolConfigFromEnvironment(&config, error)) {
        return nullptr;
    }
    return OpenMooncakeMmapCxlPoolBackend(std::move(config), error);
}

}  // namespace

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackend(CxlPoolConfig config,
                                                       CxlPoolError* error) {
    return faketract::OpenMmapCxlPoolBackend(std::move(config), error);
}

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackendFromEnvironment(
    CxlPoolError* error) {
    return faketract::OpenMmapCxlPoolBackendFromEnvironment(error);
}

bool RegisterCxlPoolBackendProvider(std::string_view provider_name,
                                    CxlPoolBackendProviderFactory factory,
                                    CxlPoolError* error) {
    ClearError(error);
    if (provider_name.empty()) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "register_provider",
                 "provider_name",
                 "CXL backend provider name must not be empty");
        return false;
    }
    if (factory == nullptr) {
        SetError(error, CxlPoolErrorCode::kInvalidConfig, "register_provider",
                 "factory", "CXL backend provider factory must not be null");
        return false;
    }
    if (provider_name == kFakeTraCTProvider ||
        provider_name == kMooncakeProvider) {
        SetError(error, CxlPoolErrorCode::kProviderRegistrationConflict,
                 "register_provider", "provider_name",
                 "faketract and mooncake are reserved built-in providers");
        return false;
    }

    const std::string name(provider_name);
    std::lock_guard<std::mutex> lock(ProviderRegistryMutex());
    const auto [iter, inserted] = ProviderRegistry().emplace(name, factory);
    if (!inserted && iter->second != factory) {
        SetError(
            error, CxlPoolErrorCode::kProviderRegistrationConflict,
            "register_provider", "provider_name",
            "a different CXL backend factory already owns provider " + name);
        return false;
    }
    return true;
}

std::shared_ptr<CxlPoolBackend> OpenCxlPoolBackendFromEnvironment(
    CxlPoolError* error) {
    ClearError(error);
    const char* raw_provider = std::getenv("MC_CXL_PROVIDER");
    const std::string provider =
        raw_provider == nullptr || *raw_provider == '\0'
            ? std::string(kFakeTraCTProvider)
            : std::string(raw_provider);

    if (provider == kFakeTraCTProvider) {
        return faketract::OpenMmapCxlPoolBackendFromEnvironment(error);
    }
    if (provider == kMooncakeProvider) {
        return OpenMooncakeMmapCxlPoolBackendFromEnvironment(error);
    }

    CxlPoolBackendProviderFactory factory = nullptr;
    {
        std::lock_guard<std::mutex> lock(ProviderRegistryMutex());
        const auto iter = ProviderRegistry().find(provider);
        if (iter != ProviderRegistry().end()) {
            factory = iter->second;
        }
    }
    if (factory == nullptr) {
        SetError(error, CxlPoolErrorCode::kProviderUnavailable, "open_backend",
                 "MC_CXL_PROVIDER",
                 "CXL backend provider is not registered: " + provider);
        return nullptr;
    }

    try {
        CxlPoolError factory_error;
        auto backend = factory(&factory_error);
        if (factory_error) {
            if (error != nullptr) {
                *error = std::move(factory_error);
            }
            return nullptr;
        }
        if (backend == nullptr) {
            SetError(error, CxlPoolErrorCode::kOpenFailed, "open_backend",
                     "provider",
                     "CXL backend provider returned null: " + provider);
        }
        return backend;
    } catch (const std::exception& exception) {
        SetError(
            error, CxlPoolErrorCode::kOpenFailed, "open_backend", "provider",
            "CXL backend provider threw: " + std::string(exception.what()));
    } catch (...) {
        SetError(error, CxlPoolErrorCode::kOpenFailed, "open_backend",
                 "provider", "CXL backend provider threw an unknown error");
    }
    return nullptr;
}

}  // namespace mooncake
