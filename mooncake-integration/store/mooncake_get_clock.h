#pragma once

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <vector>

namespace mooncake::detail {

// Optional API-boundary clock used by the HBF overlay. Keeping this at the
// pybind boundary identifies the public Mooncake API LMCache actually invokes
// and includes metadata lookup plus the data transfer visible to the caller.
// Existing client metrics remain authoritative for internal/RPC counters.
enum class MooncakeGetClockApi : size_t {
    kGet,
    kGetBatch,
    kGetBuffer,
    kBatchGetBuffer,
    kGetInto,
    kBatchGetInto,
    kCount,
};

struct MooncakeGetClockConfig {
    bool enabled{false};
    uint64_t tokens_per_object{0};
    uint64_t log_every{1};
};

inline bool MooncakeGetEnvEnabled(const char* name) {
    const char* value = std::getenv(name);
    if (!value) return false;
    return std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
           std::strcmp(value, "TRUE") == 0 || std::strcmp(value, "yes") == 0 ||
           std::strcmp(value, "YES") == 0;
}

inline uint64_t MooncakeGetEnvUint64(const char* name, uint64_t fallback) {
    const char* value = std::getenv(name);
    if (!value || value[0] == '\0' || value[0] == '-') return fallback;
    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    return end && end[0] == '\0' ? parsed : fallback;
}

inline const MooncakeGetClockConfig& MooncakeGetClockConfiguration() {
    static const MooncakeGetClockConfig config{
        MooncakeGetEnvEnabled("MOONCAKE_GET_METRICS"),
        MooncakeGetEnvUint64("MOONCAKE_TOKENS_PER_OBJECT", 0),
        std::max<uint64_t>(MooncakeGetEnvUint64("MOONCAKE_GET_LOG_EVERY", 1),
                           1),
    };
    return config;
}

inline uint64_t MooncakeGetSumSizes(const std::vector<size_t>& sizes) {
    uint64_t total = 0;
    for (const size_t size : sizes) {
        if (size > std::numeric_limits<uint64_t>::max() - total) {
            return std::numeric_limits<uint64_t>::max();
        }
        total += static_cast<uint64_t>(size);
    }
    return total;
}

class MooncakeGetClock {
   public:
    MooncakeGetClock(MooncakeGetClockApi api, const char* api_name,
                     size_t requested_objects, uint64_t requested_bytes = 0)
        : config_(MooncakeGetClockConfiguration()),
          api_name_(api_name),
          requested_objects_(requested_objects),
          requested_bytes_(requested_bytes),
          start_(std::chrono::steady_clock::now()) {
        if (!config_.enabled) return;
        auto& counter = counters()[static_cast<size_t>(api)];
        call_ = counter.fetch_add(1, std::memory_order_relaxed) + 1;
        should_log_ = (call_ - 1) % config_.log_every == 0;
    }

    MooncakeGetClock(const MooncakeGetClock&) = delete;
    MooncakeGetClock& operator=(const MooncakeGetClock&) = delete;

    ~MooncakeGetClock() {
        if (config_.enabled && !finished_) Finish(0, 0, "exception");
    }

    void Finish(size_t completed_objects, uint64_t bytes, const char* status) {
        if (finished_) return;
        finished_ = true;
        if (!config_.enabled || !should_log_) return;

        const double elapsed_us = std::chrono::duration<double, std::micro>(
                                      std::chrono::steady_clock::now() - start_)
                                      .count();
        uint64_t tokens_est = 0;
        if (config_.tokens_per_object != 0) {
            const auto limit = std::numeric_limits<uint64_t>::max() /
                               config_.tokens_per_object;
            tokens_est = completed_objects > limit
                             ? std::numeric_limits<uint64_t>::max()
                             : completed_objects * config_.tokens_per_object;
        }
        const double elapsed_ms = elapsed_us / 1000.0;
        const double us_per_token =
            tokens_est == 0 ? 0.0 : elapsed_us / tokens_est;
        const double gbps =
            elapsed_us == 0.0
                ? 0.0
                : (static_cast<double>(bytes) / elapsed_us) / 1000.0;

        LOG(INFO) << "[clock=M1_mooncake_get]"
                  << " api=" << api_name_ << " pid=" << ::getpid()
                  << " call=" << call_
                  << " requested_objects=" << requested_objects_
                  << " completed_objects=" << completed_objects
                  << " tokens_est=" << tokens_est
                  << " requested_bytes=" << requested_bytes_
                  << " bytes=" << bytes << " elapsed_ms=" << elapsed_ms
                  << " us_per_token=" << us_per_token << " gbps=" << gbps
                  << " status=" << status;
    }

   private:
    static std::array<std::atomic<uint64_t>,
                      static_cast<size_t>(MooncakeGetClockApi::kCount)>&
    counters() {
        static std::array<std::atomic<uint64_t>,
                          static_cast<size_t>(MooncakeGetClockApi::kCount)>
            value{};
        return value;
    }

    const MooncakeGetClockConfig& config_;
    const char* api_name_;
    size_t requested_objects_;
    uint64_t requested_bytes_;
    std::chrono::steady_clock::time_point start_;
    uint64_t call_{0};
    bool should_log_{false};
    bool finished_{false};
};

}  // namespace mooncake::detail
