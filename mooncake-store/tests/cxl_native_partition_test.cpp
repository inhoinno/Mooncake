#include "segment.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

namespace mooncake {
namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kPoolCapacity = 128ULL * kMiB;
constexpr uint64_t kPartitionCapacity = 64ULL * kMiB;

Segment MakeNativeCxlSegment(std::string name, std::string endpoint,
                             uint64_t offset,
                             std::string pool_id = "todo2-shared-pool") {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    // Master never dereferences this client-local mapping address. It is
    // retained for registration/unmount bookkeeping only.
    segment.base = 0x200000000ULL + offset;
    segment.size = kPoolCapacity;
    segment.te_endpoint = std::move(endpoint);
    segment.protocol = "cxl";
    segment.host_id = segment.name;
    segment.cxl_master_managed_allocation = true;
    segment.cxl_pool_id = std::move(pool_id);
    segment.cxl_pool_capacity = kPoolCapacity;
    segment.cxl_owned_offset = offset;
    segment.cxl_owned_capacity = kPartitionCapacity;
    return segment;
}

class CxlNativePartitionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        manager_ = std::make_unique<SegmentManager>(
            BufferAllocatorType::CACHELIB, true);
        manager_->initializeCxlAllocator("todo2-master-logical-pool",
                                         kPoolCapacity);
    }

    ErrorCode Mount(const Segment& segment, const UUID& client_id) {
        auto access = manager_->getSegmentAccess();
        return access.MountSegment(segment, client_id);
    }

    std::unique_ptr<SegmentManager> manager_;
};

TEST_F(CxlNativePartitionTest, FunctionalDisjointAllocationAndReclaim) {
    const Segment node0 = MakeNativeCxlSegment("node0", "10.0.0.1:19000", 0);
    const Segment node1 =
        MakeNativeCxlSegment("node1", "10.0.0.2:19000", kPartitionCapacity);
    ASSERT_EQ(Mount(node0, generate_uuid()), ErrorCode::OK);
    ASSERT_EQ(Mount(node1, generate_uuid()), ErrorCode::OK);

    CxlAllocationStrategy strategy;
    for (const Segment* segment : {&node0, &node1}) {
        uint64_t allocated_offset = 0;
        {
            auto access = manager_->getAllocatorAccess();
            const auto& allocator_manager = access.getAllocatorManager();
            auto replicas =
                strategy.Allocate(allocator_manager, 4096, 1, {segment->name});
            ASSERT_TRUE(replicas.has_value());
            ASSERT_EQ(replicas->size(), 1U);
            const auto descriptor = replicas->front().get_descriptor();
            ASSERT_TRUE(descriptor.is_memory_replica());
            const auto& buffer =
                descriptor.get_memory_descriptor().buffer_descriptor;
            allocated_offset = buffer.buffer_address_;
            EXPECT_EQ(buffer.protocol_, "cxl");
            EXPECT_EQ(buffer.transport_endpoint_, segment->te_endpoint);
            EXPECT_GE(allocated_offset, segment->cxl_owned_offset);
            EXPECT_LT(
                allocated_offset + buffer.size_,
                segment->cxl_owned_offset + segment->cxl_owned_capacity + 1);

            const auto* allocators =
                allocator_manager.getAllocators(segment->name);
            ASSERT_NE(allocators, nullptr);
            ASSERT_EQ(allocators->size(), 1U);
            EXPECT_EQ((*allocators)[0]->capacity(), kPartitionCapacity);
            EXPECT_EQ((*allocators)[0]->size(), 4096U);
        }

        // Replica destruction is the Master-side deallocation boundary.
        size_t used = 1;
        size_t capacity = 0;
        {
            auto access = manager_->getSegmentAccess();
            ASSERT_EQ(access.QuerySegments(segment->name, used, capacity),
                      ErrorCode::OK);
        }
        EXPECT_EQ(used, 0U);
        EXPECT_EQ(capacity, kPartitionCapacity);

        // Reallocation after reclaim must stay in the same owned subrange.
        auto access = manager_->getAllocatorAccess();
        auto replacement = strategy.Allocate(access.getAllocatorManager(), 4096,
                                             1, {segment->name});
        ASSERT_TRUE(replacement.has_value());
        const auto address = replacement->front()
                                 .get_descriptor()
                                 .get_memory_descriptor()
                                 .buffer_descriptor.buffer_address_;
        EXPECT_GE(address, segment->cxl_owned_offset);
        EXPECT_LT(address,
                  segment->cxl_owned_offset + segment->cxl_owned_capacity);
    }
}

TEST_F(CxlNativePartitionTest, FailureRejectsOverlapBoundsAndPoolMismatch) {
    const Segment node0 = MakeNativeCxlSegment("node0", "10.0.0.1:19000", 0);
    ASSERT_EQ(Mount(node0, generate_uuid()), ErrorCode::OK);

    Segment overlap =
        MakeNativeCxlSegment("overlap", "10.0.0.2:19000", 32 * kMiB);
    EXPECT_EQ(Mount(overlap, generate_uuid()), ErrorCode::INVALID_PARAMS);

    Segment out_of_bounds =
        MakeNativeCxlSegment("oob", "10.0.0.3:19000", kPoolCapacity);
    EXPECT_EQ(Mount(out_of_bounds, generate_uuid()), ErrorCode::INVALID_PARAMS);

    Segment wrong_pool = MakeNativeCxlSegment("wrong-pool", "10.0.0.4:19000",
                                              kPartitionCapacity, "other-pool");
    EXPECT_EQ(Mount(wrong_pool, generate_uuid()), ErrorCode::INVALID_PARAMS);

    Segment legacy = node0;
    legacy.id = generate_uuid();
    legacy.name = "legacy-full-pool";
    legacy.te_endpoint = "10.0.0.5:19000";
    legacy.cxl_master_managed_allocation = false;
    legacy.cxl_pool_id.clear();
    legacy.cxl_pool_capacity = 0;
    legacy.cxl_owned_offset = 0;
    legacy.cxl_owned_capacity = 0;
    EXPECT_EQ(Mount(legacy, generate_uuid()), ErrorCode::INVALID_PARAMS);

    std::vector<std::pair<Segment, UUID>> mounted;
    auto access = manager_->getSegmentAccess();
    ASSERT_EQ(access.GetAllSegments(mounted), ErrorCode::OK);
    ASSERT_EQ(mounted.size(), 1U);
    EXPECT_EQ(mounted.front().first.name, "node0");
}

TEST_F(CxlNativePartitionTest, ConcurrentOverlapHasSingleWinner) {
    const Segment left = MakeNativeCxlSegment("left", "10.0.0.1:19000", 0);
    const Segment right = MakeNativeCxlSegment("right", "10.0.0.2:19000", 0);
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    ErrorCode left_result = ErrorCode::INTERNAL_ERROR;
    ErrorCode right_result = ErrorCode::INTERNAL_ERROR;

    auto mount = [&](const Segment& segment, ErrorCode& result) {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        result = Mount(segment, generate_uuid());
    };
    std::thread left_thread(mount, std::cref(left), std::ref(left_result));
    std::thread right_thread(mount, std::cref(right), std::ref(right_result));
    while (ready.load(std::memory_order_acquire) != 2) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    left_thread.join();
    right_thread.join();

    const int success_count = (left_result == ErrorCode::OK ? 1 : 0) +
                              (right_result == ErrorCode::OK ? 1 : 0);
    EXPECT_EQ(success_count, 1);
    EXPECT_TRUE(left_result == ErrorCode::INVALID_PARAMS ||
                right_result == ErrorCode::INVALID_PARAMS);
}

TEST_F(CxlNativePartitionTest, StatusExposesPortableOwnershipFields) {
    const Segment node0 = MakeNativeCxlSegment("node0", "10.0.0.1:19000", 0);
    const Segment node1 =
        MakeNativeCxlSegment("node1", "10.0.0.2:19000", kPartitionCapacity);
    ASSERT_EQ(Mount(node0, generate_uuid()), ErrorCode::OK);
    ASSERT_EQ(Mount(node1, generate_uuid()), ErrorCode::OK);

    std::vector<std::pair<Segment, UUID>> mounted;
    auto access = manager_->getSegmentAccess();
    ASSERT_EQ(access.GetAllSegments(mounted), ErrorCode::OK);
    ASSERT_EQ(mounted.size(), 2U);
    for (const auto& [segment, owner] : mounted) {
        (void)owner;
        EXPECT_TRUE(segment.cxl_master_managed_allocation);
        EXPECT_EQ(segment.cxl_pool_id, "todo2-shared-pool");
        EXPECT_EQ(segment.cxl_pool_capacity, kPoolCapacity);
        EXPECT_EQ(segment.cxl_owned_capacity, kPartitionCapacity);
        // Portable ownership/debugging uses offsets and endpoint identity.
        EXPECT_FALSE(segment.te_endpoint.empty());
    }
}

}  // namespace
}  // namespace mooncake
