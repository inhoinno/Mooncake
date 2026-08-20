// Raw CXL(devdax) -> GPU read-bandwidth ceiling microbench.
//
// mmap a devdax region (or malloc DRAM), OPTIONALLY cudaHostRegister it so the
// copy is a real pinned DMA, then N host threads each cudaMemcpyAsync a BLOCK
// from rotating offsets into their own device buffer on their own stream for
// DURATION seconds. Reports aggregate GB/s.
//
// IMPORTANT: without --pin 1 the source is *pageable* and cudaMemcpy routes
// through CUDA's internal ~4-6 GB/s bounce -- that is the file-like copy ceiling,
// NOT the CXL memory bandwidth. dax24.0 is a CXL NUMA node (host-coherent), so
// --pin 1 (cudaHostRegister) gives the true DMA number. Compare against
// --src dram --pin 1 for the plain PCIe/NVLink H2D ceiling.
//
// Build (H200 host, CUDA 13):
//   nvcc -O3 -o cxl_gpu_bw scripts/cxl_gpu_bw.cu -lpthread
// Run:
//   ./cxl_gpu_bw --dev /dev/dax24.0 --dev-size 34359738368 --block 16777216 \
//                --threads 4 --seconds 10 --gpu 0 --src devdax --pin 1
//   ./cxl_gpu_bw --src dram --dev-size 34359738368 --block 16777216 --pin 1  # DRAM ref
//
// Sweep --threads 1 2 4 8; sweep --pin 0/1 to see the pageable-vs-pinned gap.
// Keep --dev-size modest (e.g. 32 GiB) when --pin 1: registering the whole
// region can be slow/fail for very large mappings.

#include <cuda_runtime.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#define CK(call)                                                             \
    do {                                                                     \
        cudaError_t _e = (call);                                             \
        if (_e != cudaSuccess) {                                             \
            fprintf(stderr, "CUDA error %s at %s:%d\n",                      \
                    cudaGetErrorString(_e), __FILE__, __LINE__);            \
            exit(1);                                                         \
        }                                                                    \
    } while (0)

static long arg_long(int argc, char** argv, const char* k, long d) {
    for (int i = 1; i + 1 < argc; ++i)
        if (!strcmp(argv[i], k)) return atol(argv[i + 1]);
    return d;
}
static const char* arg_str(int argc, char** argv, const char* k, const char* d) {
    for (int i = 1; i + 1 < argc; ++i)
        if (!strcmp(argv[i], k)) return argv[i + 1];
    return d;
}

int main(int argc, char** argv) {
    const char* dev = arg_str(argc, argv, "--dev", "/dev/dax24.0");
    const char* src = arg_str(argc, argv, "--src", "devdax");  // devdax | dram
    uint64_t dev_size = (uint64_t)arg_long(argc, argv, "--dev-size", 1LL << 30);
    uint64_t block = (uint64_t)arg_long(argc, argv, "--block", 16 * 1024 * 1024);
    int threads = (int)arg_long(argc, argv, "--threads", 4);
    double seconds = (double)arg_long(argc, argv, "--seconds", 10);
    int gpu = (int)arg_long(argc, argv, "--gpu", 0);
    int pin = (int)arg_long(argc, argv, "--pin", 1);  // cudaHostRegister the source

    if (block == 0 || block > dev_size) {
        fprintf(stderr, "block must be >0 and <= dev-size\n");
        return 2;
    }
    CK(cudaSetDevice(gpu));

    // Acquire the source region.
    void* base = nullptr;
    int fd = -1;
    if (!strcmp(src, "dram")) {
        if (posix_memalign(&base, 2 * 1024 * 1024, dev_size) != 0) {
            perror("posix_memalign dram");
            return 2;
        }
        memset(base, 1, dev_size);  // fault it in
    } else {
        fd = open(dev, O_RDWR);
        if (fd < 0) {
            perror("open devdax");
            return 2;
        }
        base = mmap(nullptr, dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (base == MAP_FAILED) {
            perror("mmap devdax");
            return 2;
        }
    }

    // Pin the source so cudaMemcpy issues a real DMA instead of a pageable
    // bounce. Without this the number is the ~4-6 GB/s pageable-copy ceiling.
    bool pinned = false;
    if (pin) {
        cudaError_t re = cudaHostRegister(base, dev_size, cudaHostRegisterDefault);
        if (re == cudaSuccess) {
            pinned = true;
        } else {
            fprintf(stderr,
                    "[warn] cudaHostRegister(%s, %llu) failed: %s -- falling "
                    "back to pageable copy (number is NOT CXL bandwidth)\n",
                    src, (unsigned long long)dev_size, cudaGetErrorString(re));
            cudaGetLastError();  // clear
        }
    }

    // How many distinct block-aligned slots fit; threads stride through them so
    // concurrent readers touch different regions (not one hot cacheline).
    uint64_t slots = dev_size / block;
    if (slots == 0) slots = 1;

    std::atomic<uint64_t> total_bytes{0};
    std::atomic<bool> stop{false};

    auto worker = [&](int tid) {
        CK(cudaSetDevice(gpu));
        cudaStream_t s;
        CK(cudaStreamCreate(&s));
        void* dbuf;
        CK(cudaMalloc(&dbuf, block));
        uint64_t local = 0;
        uint64_t slot = (uint64_t)tid % slots;
        while (!stop.load(std::memory_order_relaxed)) {
            const char* src = (const char*)base + slot * block;
            CK(cudaMemcpyAsync(dbuf, src, block, cudaMemcpyHostToDevice, s));
            CK(cudaStreamSynchronize(s));
            local += block;
            slot += (uint64_t)threads;      // stride so readers diverge
            if (slot >= slots) slot %= slots;
        }
        total_bytes.fetch_add(local, std::memory_order_relaxed);
        CK(cudaFree(dbuf));
        CK(cudaStreamDestroy(s));
    };

    // Warmup one pass per thread is implicit in the first iterations; start timer
    // and let each thread run until the deadline.
    std::vector<std::thread> pool;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < threads; ++i) pool.emplace_back(worker, i);

    // Sleep the measurement window, then signal stop.
    struct timespec req;
    req.tv_sec = (time_t)seconds;
    req.tv_nsec = (long)((seconds - (double)req.tv_sec) * 1e9);
    nanosleep(&req, nullptr);
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : pool) th.join();
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double elapsed =
        (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    uint64_t bytes = total_bytes.load();
    double gbps = elapsed > 0 ? (double)bytes / elapsed / 1e9 : 0.0;
    double gibps = elapsed > 0 ? (double)bytes / elapsed / (1024.0 * 1024 * 1024) : 0.0;

    printf(
        "{\"src\":\"%s\",\"dev\":\"%s\",\"pinned\":%s,\"block_bytes\":%llu,"
        "\"threads\":%d,\"elapsed_sec\":%.3f,\"bytes\":%llu,"
        "\"throughput_GBps\":%.3f,\"throughput_GiBps\":%.3f}\n",
        src, dev, pinned ? "true" : "false", (unsigned long long)block, threads,
        elapsed, (unsigned long long)bytes, gbps, gibps);

    if (pinned) cudaHostUnregister(base);
    if (fd >= 0) {
        munmap(base, dev_size);
        close(fd);
    } else {
        free(base);
    }
    return 0;
}
