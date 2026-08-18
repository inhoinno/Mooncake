#include "gpu_transfer_policy.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(GpuTransferPolicyTest, CpuDestinationUsesHostAddressablePath) {
    EXPECT_EQ(SelectGpuReadPath("cxl", false, false),
              GpuReadPath::kHostAddressable);
    EXPECT_EQ(SelectGpuReadPath("rdma", false, true),
              GpuReadPath::kHostAddressable);
}

TEST(GpuTransferPolicyTest, CxlGpuDestinationUsesCudaCopy) {
    EXPECT_EQ(SelectGpuReadPath("cxl", true, false), GpuReadPath::kCxlCudaCopy);
    EXPECT_STREQ(ToString(GpuReadPath::kCxlCudaCopy), "cxl_cuda_copy");
}

TEST(GpuTransferPolicyTest, RdmaGpuDestinationStagesByDefault) {
    EXPECT_EQ(SelectGpuReadPath("rdma", true, false),
              GpuReadPath::kRdmaHostStaged);
    EXPECT_EQ(SelectGpuReadPath("tcp", true, false),
              GpuReadPath::kRdmaHostStaged);
}

TEST(GpuTransferPolicyTest, GpuDirectRequiresExplicitOptIn) {
    EXPECT_EQ(SelectGpuReadPath("rdma", true, true),
              GpuReadPath::kRdmaGpuDirect);
    EXPECT_EQ(SelectGpuReadPath("efa", true, true),
              GpuReadPath::kRdmaGpuDirect);
    EXPECT_EQ(SelectGpuReadPath("cxi", true, true),
              GpuReadPath::kRdmaGpuDirect);
}

TEST(GpuTransferPolicyTest, TraceFlagIsFailClosed) {
    unsetenv("MC_STORE_TRACE_GPU_TRANSFERS");
    EXPECT_FALSE(GpuTransferTraceEnabledFromEnvironment());
    setenv("MC_STORE_TRACE_GPU_TRANSFERS", "YES", 1);
    EXPECT_TRUE(GpuTransferTraceEnabledFromEnvironment());
    setenv("MC_STORE_TRACE_GPU_TRANSFERS", "invalid", 1);
    EXPECT_FALSE(GpuTransferTraceEnabledFromEnvironment());
    unsetenv("MC_STORE_TRACE_GPU_TRANSFERS");
}

}  // namespace
}  // namespace mooncake
