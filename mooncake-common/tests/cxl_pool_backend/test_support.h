// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef MOONCAKE_CXL_POOL_BACKEND_TEST_SUPPORT_H_
#define MOONCAKE_CXL_POOL_BACKEND_TEST_SUPPORT_H_

#include <fcntl.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "faketract/cxl_pool_backend.h"

namespace mooncake::cxl_test {

class TestContext {
   public:
    explicit TestContext(std::string name, std::string tier = "T0",
                         std::string backend = "file")
        : name_(std::move(name)), tier_(std::move(tier)) {
        std::cout << "test=" << name_ << " tier=" << tier_
                  << " backend=" << backend << " status=START\n";
    }

    void Expect(bool condition, std::string_view message) {
        std::cout << (condition ? "PASS " : "FAIL ") << message << '\n';
        if (!condition) {
            ++failures_;
        }
    }

    int Finish() const {
        std::cout << "test=" << name_ << " tier=" << tier_
                  << " status=" << (failures_ == 0 ? "PASS" : "FAIL")
                  << " failures=" << failures_ << '\n';
        return failures_ == 0 ? 0 : 1;
    }

   private:
    std::string name_;
    std::string tier_;
    int failures_{0};
};

class TemporaryPoolFile {
   public:
    explicit TemporaryPoolFile(std::uint64_t size) {
        std::string pattern = "/tmp/mooncake_cxl_pool_test_XXXXXX";
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        fd_ = ::mkstemp(writable.data());
        if (fd_ < 0) {
            return;
        }
        path_ = writable.data();
        if (::ftruncate(fd_, static_cast<off_t>(size)) != 0) {
            ::close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            path_.clear();
        }
    }

    ~TemporaryPoolFile() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    TemporaryPoolFile(const TemporaryPoolFile&) = delete;
    TemporaryPoolFile& operator=(const TemporaryPoolFile&) = delete;

    bool valid() const noexcept { return fd_ >= 0 && !path_.empty(); }
    const std::string& path() const noexcept { return path_; }

   private:
    int fd_{-1};
    std::string path_;
};

inline CxlPoolConfig MakeFileConfig(const std::string& path,
                                    std::uint64_t capacity,
                                    std::string pool_id = "test-pool") {
    const long page_size = ::sysconf(_SC_PAGESIZE);
    return CxlPoolConfig{
        .logical_pool_id = std::move(pool_id),
        .backend_kind = CxlPoolBackendKind::kFile,
        .path = path,
        .capacity = capacity,
        .mapping_offset = 0,
        .mapping_alignment =
            page_size > 0 ? static_cast<std::uint64_t>(page_size) : 4096,
        .allocation_alignment = 64};
}

inline std::uint64_t Checksum(const void* data, std::size_t length) {
    const auto* bytes = static_cast<const unsigned char*>(data);
    std::uint64_t value = 1469598103934665603ULL;
    for (std::size_t index = 0; index < length; ++index) {
        value ^= bytes[index];
        value *= 1099511628211ULL;
    }
    return value;
}

inline void FillDeterministic(void* data, std::size_t length) {
    auto* bytes = static_cast<unsigned char*>(data);
    for (std::size_t index = 0; index < length; ++index) {
        bytes[index] = static_cast<unsigned char>((index * 193U + 29U) & 0xffU);
    }
}

inline bool ErrorIs(const CxlPoolError& error, CxlPoolErrorCode code,
                    std::string_view field) {
    return error.code == code && error.field == field &&
           !error.operation.empty() && !error.message.empty();
}

}  // namespace mooncake::cxl_test

#endif  // MOONCAKE_CXL_POOL_BACKEND_TEST_SUPPORT_H_
