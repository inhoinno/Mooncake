// Raw CXL(devdax) -> GPU read-bandwidth ceiling microbench.
//
// mmap a devdax region, then N host threads each cudaMemcpyAsync a BLOCK from
// rotating pool offsets into their own device buffer on their own stream, for
// DURATION seconds. Reports aggregate GB/s. This is the hardware/DMA ceiling for
// the CXL->GPU leg, independent of Mooncake Store/Transfer Engine overhead.
//
// Build (H200 host, CUDA 13):
//   nvcc -O3 -o cxl_gpu_bw scripts/cxl_gpu_bw.cu -lpthread
// Run (exact 16 MiB blocks, 4 concurrent copy threads, 10 s):
//   ./cxl_gpu_bw --dev /dev/dax24.0 --dev-size 107374182400 \
//                --block 16777216 --threads 4 --seconds 10 --gpu 0
//
// Sweep --threads 1 2 4 8 to see how concurrent readers scale on one GPU.

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
    uint64_t dev_size = (uint64_t)arg_long(argc, argv, "--dev-size", 1LL << 30);
    uint64_t block = (uint64_t)arg_long(argc, argv, "--block", 16 * 1024 * 1024);
    int threads = (int)arg_long(argc, argv, "--threads", 4);
    double seconds = (double)arg_long(argc, argv, "--seconds", 10);
    int gpu = (int)arg_long(argc, argv, "--gpu", 0);

    if (block == 0 || block > dev_size) {
        fprintf(stderr, "block must be >0 and <= dev-size\n");
        return 2;
    }
    CK(cudaSetDevice(gpu));

    int fd = open(dev, O_RDWR);
    if (fd < 0) {
        perror("open devdax");
        return 2;
    }
    void* base = mmap(nullptr, dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        perror("mmap devdax");
        return 2;
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
        "{\"dev\":\"%s\",\"block_bytes\":%llu,\"threads\":%d,"
        "\"elapsed_sec\":%.3f,\"bytes\":%llu,"
        "\"throughput_GBps\":%.3f,\"throughput_GiBps\":%.3f}\n",
        dev, (unsigned long long)block, threads, elapsed,
        (unsigned long long)bytes, gbps, gibps);

    munmap(base, dev_size);
    close(fd);
    return 0;
}
