// Copyright 2026 Mooncake Authors
//
// Policy for materializing a Mooncake memory replica into GPU memory. The
// policy is deliberately separate from the transport implementation so vLLM
// and Store tests can validate routing without requiring CUDA, CXL, or RDMA
// hardware.

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>
#include <string_view>

namespace mooncake {

enum class GpuReadPath {
    kHostAddressable,
    kCxlCudaCopy,
    kRdmaHostStaged,
    kRdmaGpuDirect,
};

inline const char* ToString(GpuReadPath path) noexcept {
    switch (path) {
        case GpuReadPath::kHostAddressable:
            return "host_addressable";
        case GpuReadPath::kCxlCudaCopy:
            return "cxl_cuda_copy";
        case GpuReadPath::kRdmaHostStaged:
            return "rdma_host_staged";
        case GpuReadPath::kRdmaGpuDirect:
            return "rdma_gpu_direct";
    }
    return "unknown";
}

inline bool IsNetworkMemoryProtocol(std::string_view protocol) noexcept {
    return protocol == "rdma" || protocol == "efa" || protocol == "cxi";
}

inline GpuReadPath SelectGpuReadPath(std::string_view protocol,
                                     bool destination_is_gpu,
                                     bool rdma_gpu_direct_enabled) noexcept {
    if (!destination_is_gpu) return GpuReadPath::kHostAddressable;
    if (protocol == "cxl") return GpuReadPath::kCxlCudaCopy;
    if (IsNetworkMemoryProtocol(protocol)) {
        return rdma_gpu_direct_enabled ? GpuReadPath::kRdmaGpuDirect
                                       : GpuReadPath::kRdmaHostStaged;
    }
    // TCP, unknown, and future non-GDR transports must not receive a GPU
    // pointer until they explicitly advertise device-memory support.
    return GpuReadPath::kRdmaHostStaged;
}

inline bool RdmaGpuDirectEnabledFromEnvironment() {
    const char* raw = std::getenv("MC_STORE_RDMA_GPU_DIRECT");
    if (raw == nullptr) return false;
    std::string value(raw);
    std::transform(
        value.begin(), value.end(), value.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

inline bool GpuTransferTraceEnabledFromEnvironment() {
    const char* raw = std::getenv("MC_STORE_TRACE_GPU_TRANSFERS");
    if (raw == nullptr) return false;
    std::string value(raw);
    std::transform(
        value.begin(), value.end(), value.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

}  // namespace mooncake
