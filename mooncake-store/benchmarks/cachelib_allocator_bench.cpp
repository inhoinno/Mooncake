#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/mman.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_set>
#include <vector>

#include "allocator.h"

DEFINE_uint64(num_objects, 1000000, "Number of fixed-size objects to allocate");
DEFINE_uint64(object_size, 4096, "Requested bytes per object");
DEFINE_uint64(pool_size_bytes, 8ULL * 1024 * 1024 * 1024,
              "Fixed backing DRAM address-space size; must be a multiple of "
              "the CacheLib slab size");
DEFINE_bool(touch_memory, false,
            "memset every successful object (includes DRAM commit/write cost)");
DEFINE_bool(self_test, false,
            "Run four small allocator/mapping preflight checks and exit");

namespace {

using Clock = std::chrono::steady_clock;
constexpr size_t kSlabSize = facebook::cachelib::Slab::kSize;

class AlignedDramMapping {
   public:
    explicit AlignedDramMapping(size_t usable_size)
        : usable_size_(usable_size) {
        if (usable_size == 0 ||
            usable_size > std::numeric_limits<size_t>::max() - kSlabSize) {
            error_ = "mapping size is zero or overflows size_t";
            return;
        }
        mapping_size_ = usable_size + kSlabSize;
        mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mapping_ == MAP_FAILED) {
            mapping_ = nullptr;
            error_ = std::string("mmap failed: ") + std::strerror(errno);
            return;
        }
        const auto raw = reinterpret_cast<uintptr_t>(mapping_);
        const auto aligned = (raw + kSlabSize - 1) & ~(kSlabSize - 1);
        aligned_ = reinterpret_cast<void*>(aligned);
    }

    ~AlignedDramMapping() {
        if (mapping_) munmap(mapping_, mapping_size_);
    }

    AlignedDramMapping(const AlignedDramMapping&) = delete;
    AlignedDramMapping& operator=(const AlignedDramMapping&) = delete;

    bool valid() const { return aligned_ != nullptr; }
    void* data() const { return aligned_; }
    size_t size() const { return usable_size_; }
    const std::string& error() const { return error_; }

    bool contains(const void* pointer, size_t length) const {
        if (!valid()) return false;
        const auto base = reinterpret_cast<uintptr_t>(aligned_);
        const auto address = reinterpret_cast<uintptr_t>(pointer);
        return address >= base && length <= usable_size_ &&
               address - base <= usable_size_ - length;
    }

   private:
    void* mapping_{nullptr};
    void* aligned_{nullptr};
    size_t mapping_size_{0};
    size_t usable_size_{0};
    std::string error_;
};

double percentile(std::vector<double> values, double fraction) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    return values[std::min(values.size() - 1,
                           static_cast<size_t>(fraction * values.size()))];
}

std::shared_ptr<mooncake::CachelibBufferAllocator> make_allocator(
    const AlignedDramMapping& mapping, const std::string& name) {
    return std::make_shared<mooncake::CachelibBufferAllocator>(
        name, reinterpret_cast<uintptr_t>(mapping.data()), mapping.size(),
        "local-dram://cachelib-allocator-bench");
}

bool report_self_test(const char* name, bool passed) {
    std::cout << "self_test=" << name
              << " status=" << (passed ? "PASS" : "FAIL") << '\n';
    return passed;
}

int run_self_test() {
    const size_t pool_size = 4 * kSlabSize;
    constexpr size_t object_size = 4096;
    constexpr size_t object_count = 64;
    AlignedDramMapping mapping(pool_size);

    bool all_passed = true;
    all_passed &= report_self_test(
        "aligned_real_mapping",
        mapping.valid() &&
            reinterpret_cast<uintptr_t>(mapping.data()) % kSlabSize == 0);
    if (!mapping.valid()) {
        std::cerr << "mapping_error=" << mapping.error() << '\n';
        return 2;
    }

    auto allocator = make_allocator(mapping, "cachelib-self-test");
    std::vector<std::unique_ptr<mooncake::AllocatedBuffer>> handles;
    std::unordered_set<void*> addresses;
    handles.reserve(object_count);
    bool valid_allocations = true;
    for (size_t i = 0; i < object_count; ++i) {
        auto handle = allocator->allocate(object_size);
        if (!handle || !mapping.contains(handle->data(), handle->size()) ||
            !addresses.insert(handle->data()).second) {
            valid_allocations = false;
            break;
        }
        handles.push_back(std::move(handle));
    }
    all_passed &=
        report_self_test("unique_in_range_allocations",
                         valid_allocations && handles.size() == object_count);
    all_passed &=
        report_self_test("requested_byte_accounting",
                         allocator->size() == handles.size() * object_size);

    handles.clear();
    auto recycled = allocator->allocate(object_size);
    const bool recycled_ok =
        allocator->size() == object_size && recycled &&
        mapping.contains(recycled->data(), recycled->size());
    recycled.reset();
    all_passed &= report_self_test("deallocate_and_reallocate",
                                   recycled_ok && allocator->size() == 0);
    return all_passed ? 0 : 3;
}

bool validate_flags() {
    if (FLAGS_num_objects == 0 || FLAGS_object_size == 0 ||
        FLAGS_pool_size_bytes == 0) {
        std::cerr << "error: num_objects, object_size, and pool_size_bytes "
                     "must be non-zero\n";
        return false;
    }
    if (FLAGS_pool_size_bytes % kSlabSize != 0) {
        std::cerr << "error: pool_size_bytes must be a multiple of slab_bytes="
                  << kSlabSize << '\n';
        return false;
    }
    if (FLAGS_num_objects > std::numeric_limits<size_t>::max() ||
        FLAGS_object_size > std::numeric_limits<size_t>::max() ||
        FLAGS_pool_size_bytes > std::numeric_limits<size_t>::max()) {
        std::cerr << "error: a flag exceeds this process's size_t range\n";
        return false;
    }
    return true;
}

int run_benchmark() {
    if (!validate_flags()) return 2;

    AlignedDramMapping mapping(static_cast<size_t>(FLAGS_pool_size_bytes));
    if (!mapping.valid()) {
        std::cerr << "error: " << mapping.error() << '\n';
        return 2;
    }
    auto allocator = make_allocator(mapping, "cachelib-allocator-bench");
    std::vector<std::unique_ptr<mooncake::AllocatedBuffer>> handles;
    std::vector<double> latency_ns;
    try {
        handles.reserve(static_cast<size_t>(FLAGS_num_objects));
        latency_ns.reserve(static_cast<size_t>(FLAGS_num_objects));
    } catch (const std::bad_alloc&) {
        std::cerr << "error: unable to reserve benchmark bookkeeping arrays\n";
        return 2;
    }

    uint64_t failed = 0;
    bool out_of_range = false;
    const auto allocation_start = Clock::now();
    for (uint64_t i = 0; i < FLAGS_num_objects; ++i) {
        const auto start = Clock::now();
        auto handle =
            allocator->allocate(static_cast<size_t>(FLAGS_object_size));
        const auto end = Clock::now();
        latency_ns.push_back(
            std::chrono::duration<double, std::nano>(end - start).count());
        if (!handle) {
            failed = FLAGS_num_objects - i;
            break;
        }
        if (!mapping.contains(handle->data(), handle->size())) {
            out_of_range = true;
            break;
        }
        if (FLAGS_touch_memory) {
            std::memset(handle->data(), static_cast<int>(i & 0xff),
                        handle->size());
        }
        handles.push_back(std::move(handle));
    }
    const auto allocation_end = Clock::now();
    const double elapsed_ms = std::chrono::duration<double, std::milli>(
                                  allocation_end - allocation_start)
                                  .count();
    const uint64_t succeeded = handles.size();
    const double elapsed_seconds = elapsed_ms / 1000.0;
    const long double logical_bytes =
        static_cast<long double>(succeeded) * FLAGS_object_size;

    std::cout
        << std::fixed << std::setprecision(3)
        << "benchmark=cachelib_allocator phase=allocate"
        << " requested_objects=" << FLAGS_num_objects
        << " successful_objects=" << succeeded << " failed_objects=" << failed
        << " object_size=" << FLAGS_object_size
        << " pool_size_bytes=" << FLAGS_pool_size_bytes
        << " touch_memory=" << (FLAGS_touch_memory ? 1 : 0)
        << " elapsed_ms=" << elapsed_ms << " objects_per_second="
        << (elapsed_seconds > 0 ? succeeded / elapsed_seconds : 0)
        << " logical_gbps="
        << (elapsed_seconds > 0
                ? static_cast<double>(logical_bytes / elapsed_seconds / 1.0e9L)
                : 0)
        << " mean_ns="
        << (latency_ns.empty()
                ? 0
                : std::accumulate(latency_ns.begin(), latency_ns.end(), 0.0) /
                      latency_ns.size())
        << " p50_ns=" << percentile(latency_ns, 0.50)
        << " p99_ns=" << percentile(latency_ns, 0.99)
        << " allocator_reported_bytes=" << allocator->size() << '\n';

    const auto deallocation_start = Clock::now();
    handles.clear();
    const auto deallocation_end = Clock::now();
    const double deallocation_ms = std::chrono::duration<double, std::milli>(
                                       deallocation_end - deallocation_start)
                                       .count();
    std::cout << "benchmark=cachelib_allocator phase=deallocate"
              << " objects=" << succeeded << " elapsed_ms=" << deallocation_ms
              << " objects_per_second="
              << (deallocation_ms > 0 ? succeeded / (deallocation_ms / 1000.0)
                                      : 0)
              << " allocator_reported_bytes=" << allocator->size() << '\n';

    if (out_of_range) {
        std::cerr << "error: allocator returned an address outside the backing "
                     "DRAM mapping\n";
        return 4;
    }
    return succeeded == 0 ? 3 : 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    static_assert((kSlabSize & (kSlabSize - 1)) == 0,
                  "CacheLib slab size must be a power of two");
    return FLAGS_self_test ? run_self_test() : run_benchmark();
}
