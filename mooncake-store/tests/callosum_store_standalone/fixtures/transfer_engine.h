#pragma once
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>
namespace mooncake {
struct Status { bool success = true; bool ok() const { return success; } };
struct TransferRequest { enum OpCode { WRITE, READ }; };
struct TransferMetadata {
    struct NotifyDesc { std::string name, notify_msg; };
    struct SegmentDesc { std::string protocol = "rdma"; };
    std::shared_ptr<SegmentDesc> getSegmentDescByName(const std::string&, bool) {
        auto result = std::make_shared<SegmentDesc>(); result->protocol = protocol;
        return result;
    }
    std::string protocol = "rdma";
};
class TransferEngine {
 public:
    struct ScatterTransferRange {
        TransferRequest::OpCode opcode;
        std::string remote_segment;
        uint64_t remote_base_offset;
        size_t remote_size;
        void* local_buffer;
        size_t local_capacity;
        std::span<const size_t> local_offsets, remote_offsets, lengths;
        std::function<void(size_t, const Status&)> on_fragment_complete;
    };
    struct ScatterTransferOperation {
        std::vector<ScatterTransferRange> ranges;
        TransferEngine* engine;
        Status wait() {
            ++engine->batches;
            if (engine->delay_ms) std::this_thread::sleep_for(
                std::chrono::milliseconds(engine->delay_ms));
            for (const auto& range : ranges) {
                if (range.opcode != TransferRequest::WRITE) throw std::runtime_error("not WRITE");
                for (size_t i = 0; i < range.lengths.size(); ++i) {
                    const auto length = range.lengths[i];
                    if (range.local_offsets[i] + length > range.local_capacity ||
                        range.remote_offsets[i] + length > range.remote_size)
                        throw std::runtime_error("range overflow");
                    std::memcpy(reinterpret_cast<char*>(range.remote_base_offset) + range.remote_offsets[i],
                                static_cast<char*>(range.local_buffer) + range.local_offsets[i], length);
                    engine->bytes += length;
                }
                if (engine->corrupt) *(reinterpret_cast<char*>(range.remote_base_offset) + range.remote_offsets[0]) ^= 1;
            }
            return {!engine->fail};
        }
    };
    explicit TransferEngine(std::string name) : name(std::move(name)) {
        std::lock_guard lock(network_mutex); network[this->name] = this;
    }
    ~TransferEngine() { std::lock_guard lock(network_mutex); network.erase(name); }
    int sendNotifyByName(const std::string& target, TransferMetadata::NotifyDesc message) {
        std::lock_guard lock(network_mutex);
        auto peer = network.find(target);
        if (peer == network.end()) return -1;
        if (drop_prepare && message.notify_msg.find("\"prepare\"") != std::string::npos) return 0;
        peer->second->messages.push_back(std::move(message)); return 0;
    }
    int getNotifies(std::vector<TransferMetadata::NotifyDesc>& out) {
        std::lock_guard lock(network_mutex);
        while (!messages.empty()) { out.push_back(std::move(messages.front())); messages.pop_front(); }
        return 0;
    }
    TransferMetadata* getMetadata() { return &metadata; }
    bool isUsingTent() const { return false; }
    void* getTransport(const std::string& protocol) { return protocol=="rdma" && rdma_available ? this : nullptr; }
    ScatterTransferOperation submitScatter(const std::vector<ScatterTransferRange>& ranges) {
        if (on_submit) on_submit();
        if (throw_submit) throw std::runtime_error("unknown submission");
        return {ranges, this};
    }
    std::string name;
    std::atomic<size_t> bytes{0}, batches{0};
    int delay_ms = 0;
    bool corrupt = false, fail = false, drop_prepare = false, throw_submit = false;
    bool rdma_available = true;
    TransferMetadata metadata;
    std::function<void()> on_submit;
 private:
    std::deque<TransferMetadata::NotifyDesc> messages;
    static inline std::mutex network_mutex;
    static inline std::map<std::string, TransferEngine*> network;
};
}
