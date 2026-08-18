#include "multipath_placement.h"

#include <gtest/gtest.h>

#include <set>

namespace mooncake {
namespace {

TEST(MultipathPlacementTest, InvalidConfigurationFailsClosed) {
    EXPECT_FALSE(SelectHashMultipathSegment("key", "", "rdma"));
    EXPECT_FALSE(SelectHashMultipathSegment("key", "same", "same"));
}

TEST(MultipathPlacementTest, HashIsStableAndUsesBothSources) {
    EXPECT_EQ(StableKvPlacementHash("block-17"),
              StableKvPlacementHash("block-17"));
    std::set<std::string> selected;
    for (int index = 0; index < 128; ++index) {
        auto segment = SelectHashMultipathSegment(
            "block-" + std::to_string(index), "cxl", "rdma");
        ASSERT_TRUE(segment.has_value());
        selected.insert(*segment);
    }
    EXPECT_EQ(selected, (std::set<std::string>{"cxl", "rdma"}));
}

TEST(MultipathPlacementTest, EnvironmentIsExplicitOptIn) {
    unsetenv("MC_STORE_MULTIPATH_PLACEMENT");
    unsetenv("MC_STORE_MULTIPATH_CXL_SEGMENT");
    unsetenv("MC_STORE_MULTIPATH_NETWORK_SEGMENT");
    EXPECT_FALSE(MultipathSegmentFromEnvironment("key"));

    setenv("MC_STORE_MULTIPATH_PLACEMENT", "hash", 1);
    setenv("MC_STORE_MULTIPATH_CXL_SEGMENT", "cxl", 1);
    setenv("MC_STORE_MULTIPATH_NETWORK_SEGMENT", "rdma", 1);
    EXPECT_TRUE(MultipathSegmentFromEnvironment("key"));

    unsetenv("MC_STORE_MULTIPATH_PLACEMENT");
    unsetenv("MC_STORE_MULTIPATH_CXL_SEGMENT");
    unsetenv("MC_STORE_MULTIPATH_NETWORK_SEGMENT");
}

}  // namespace
}  // namespace mooncake
