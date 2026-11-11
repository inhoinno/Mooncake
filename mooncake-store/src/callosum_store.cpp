#include "callosum_store.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <future>
#include <tuple>

#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "client_service.h"
#include "crc_checksum.h"
#include "transfer_engine.h"
#include "utils.h"

namespace mooncake {
namespace {
constexpr const char* kNotify = "callosum.store.v1";
constexpr size_t kLedgerLimit = 4096;
constexpr size_t kChunk = 1024 * 1024;
constexpr size_t kBatch = 32;

void Require(bool valid, const char* message) {
    if (!valid) throw std::runtime_error(message);
}

std::string Read(const std::string& path) {
    std::ifstream input(path);
    Require(input.good(), "cannot read Callosum input file");
    return {std::istreambuf_iterator<char>(input), {}};
}

// Each journal belongs to one owned arena and holds a process lock. Pending
// records survive a crash. No TTL or new process epoch clears those records.
class Journal {
   public:
    Journal(const std::string& path, const std::string& identity) {
        fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
        Require(fd_ >= 0, "cannot open Callosum journal");
        struct stat info{};
        if (fstat(fd_, &info) != 0 || !S_ISREG(info.st_mode) ||
            (info.st_mode & 077) != 0 || info.st_uid != geteuid()) {
            close(fd_);
            fd_ = -1;
            throw std::runtime_error(
                "Callosum journal must be a private owned regular file");
        }
        if (flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            close(fd_);
            fd_ = -1;
            throw std::runtime_error(
                "Callosum arena already has a journal owner");
        }
        try {
            const auto contents = Read(path);
            Require(contents.empty() || contents.back() == '\n',
                    "truncated Callosum journal tail");
            std::istringstream input(contents);
            std::string line;
            if (!contents.empty()) {
                std::getline(input, line);
                Require(
                    line == "IDENTITY " + identity,
                    "Callosum journal belongs to another arena or campaign");
            } else {
                const auto header = "IDENTITY " + identity + "\n";
                Require(write(fd_, header.data(), header.size()) ==
                                static_cast<ssize_t>(header.size()) &&
                            fsync(fd_) == 0,
                        "cannot initialize Callosum journal identity");
            }
            while (std::getline(input, line)) {
                std::istringstream record(line);
                std::string id, state, extra;
                Require(bool(record >> id >> state) && !(record >> extra),
                        "invalid Callosum journal record");
                Require(state == "PENDING" || state == "DRAINED" ||
                            state == "NO_POST" || state == "VALIDATED",
                        "invalid Callosum journal record");
                states_[id] = state;
            }
            Require(!Pending(),
                    "Callosum journal requires positive drain recovery");
        } catch (...) {
            close(fd_);
            fd_ = -1;
            throw;
        }
    }
    ~Journal() {
        if (fd_ >= 0) close(fd_);
    }
    void Mark(const std::string& id, const std::string& state) {
        std::lock_guard lock(mutex_);
        const auto line = id + " " + state + "\n";
        size_t offset = 0;
        while (offset < line.size()) {
            auto written =
                write(fd_, line.data() + offset, line.size() - offset);
            if (written < 0 && errno == EINTR) continue;
            Require(written > 0, "Callosum journal write failed");
            offset += static_cast<size_t>(written);
        }
        Require(fsync(fd_) == 0, "Callosum journal sync failed");
        states_[id] = state;
    }
    bool Pending() const {
        std::lock_guard lock(mutex_);
        for (const auto& [id, state] : states_)
            if (state == "PENDING") return true;
        return false;
    }

   private:
    int fd_ = -1;
    mutable std::mutex mutex_;
    std::map<std::string, std::string> states_;
};

struct Message {
    std::string id, phase, sender, pool, epoch, secret, campaign, key, tenant;
    uint64_t address = 0, length = 0, checksum = 0;
    uint64_t pool_offset = 0;
    uint64_t source_generation = 0, target_generation = 0;
    bool ok = false;
};

bool SameTarget(const Message& a, const Message& b) {
    return a.id == b.id && a.address == b.address && a.length == b.length &&
           a.checksum == b.checksum &&
           a.source_generation == b.source_generation &&
           a.target_generation == b.target_generation && a.key == b.key &&
           a.tenant == b.tenant;
}
}  // namespace

std::unique_ptr<CallosumStoreConfig> CallosumStoreConfig::Load() {
    const char* path = std::getenv("MC_CALLOSUM_STORE_CONFIG");
    if (path == nullptr) return nullptr;
    Require(*path != '\0', "MC_CALLOSUM_STORE_CONFIG is empty");
    auto result = std::make_unique<CallosumStoreConfig>();
    struct_json::from_json(*result, Read(path));
    Require(!result->campaign.empty() && !result->pool_id.empty() &&
                !result->path.empty() && !result->journal.empty() &&
                !result->token_file.empty() && result->capacity > 0 &&
                result->owned_size > 0 && result->alignment > 0 &&
                result->wait_ms > 0 && result->wait_ms <= 300000 &&
                result->mux_capacity > 0 && result->mux_capacity <= 32 &&
                result->mux_queue_limit > 0 && result->mux_queue_limit <= 4096,
            "incomplete Callosum Store configuration");
    Require(result->backend_kind == "devdax" || result->backend_kind == "file",
            "Callosum requires an explicit devdax or file mapping");
    return result;
}

std::shared_ptr<CxlPoolBackend> CallosumStoreConfig::Map() const {
    CxlPoolConfig config;
    config.logical_pool_id = pool_id;
    config.path = path;
    config.backend_kind = backend_kind == "file" ? CxlPoolBackendKind::kFile
                                                 : CxlPoolBackendKind::kDevDax;
    config.capacity = capacity;
    config.mapping_offset = mapping_offset;
    config.mapping_alignment = alignment;
    config.owned_offset = owned_offset;
    config.owned_capacity = owned_size;
    CxlPoolError error;
    auto pool = OpenMooncakeMapping(config, &error);
    if (!pool)
        throw std::runtime_error(
            std::string("Callosum mapping failed: ") + ToString(error.code) +
            " field=" + error.field + " operation=" + error.operation);
    return pool;
}

class CallosumStoreBridge::Impl {
   public:
    Impl(Client& client, TransferEngine& engine, CallosumStoreConfig config,
         std::shared_ptr<CxlPoolBackend> pool)
        : client(client),
          engine(engine),
          config(std::move(config)),
          pool(std::move(pool)),
          journal(this->config.journal,
                  this->config.campaign + ":" + this->config.pool_id),
          epoch(UuidToString(generate_uuid())),
          mux([this](const auto& plan,
                     const auto& group) { return Write(plan, group); },
              this->config.mux_capacity, this->config.mux_queue_limit,
              [](const pool_relay::MuxEvent& event) {
                  const char* name = event.state == "QUEUED" ? "mux_queued"
                                     : event.state == "DISPATCHED"
                                         ? "mux_admitted"
                                     : event.state == "DRAINED" ? "mux_gathered"
                                                                : "mux_failed";
                  LOG(INFO) << "component=callosum_store event=" << name
                            << " attempt=" << event.transfer_id
                            << " state=" << event.state
                            << " clock=source_steady timestamp_ns="
                            << event.timestamp_ns
                            << " queue_wait_ns=" << event.queue_wait_ns
                            << " dispatch_ns=" << event.dispatch_ns
                            << " bytes=" << event.bytes;
              }) {
        Require(!engine.isUsingTent() && engine.getTransport("rdma") != nullptr,
                "Callosum Store requires the classic native RDMA transport; no "
                "fallback");
        struct stat info{};
        Require(stat(this->config.token_file.c_str(), &info) == 0 &&
                    S_ISREG(info.st_mode) && (info.st_mode & 077) == 0,
                "Callosum token file must be private and regular");
        secret = Read(this->config.token_file);
        while (!secret.empty() &&
               (secret.back() == '\n' || secret.back() == '\r'))
            secret.pop_back();
        Require(secret.size() >= 32, "Callosum token is too short");
        progress = std::thread([this] { Progress(); });
    }
    ~Impl() {
        running = false;
        if (progress.joinable()) progress.join();
        // Do not proceed into Client's unmount/free path on unknown DMA.
        // Restart must also pass the retained journal's drain gate.
        if (journal.Pending()) {
            LOG(ERROR) << "component=callosum_store event=quarantine_shutdown "
                          "reason=positive_drain_required";
            std::terminate();
        }
    }

    bool Owns(const Replica::Descriptor& replica) const {
        if (!replica.is_memory_replica()) return false;
        const auto& b = replica.get_memory_descriptor().buffer_descriptor;
        return b.protocol_ == "rdma" &&
               b.transport_endpoint_ == client.GetSegmentEndpoint() &&
               Owned(b.buffer_address_, b.size_);
    }
    bool Owned(uint64_t address, uint64_t size) const {
        if (size == 0) return false;
        auto offset = pool->offset_of(reinterpret_cast<void*>(address), size);
        return offset && *offset >= config.owned_offset &&
               *offset - config.owned_offset <= config.owned_size &&
               size <= config.owned_size - (*offset - config.owned_offset);
    }
    void Send(const std::string& endpoint, Message message) {
        message.sender = client.GetSegmentEndpoint();
        message.pool = config.pool_id;
        message.secret = secret;
        message.campaign = config.campaign;
        std::string json;
        struct_json::to_json(message, json);
        Require(engine.sendNotifyByName(endpoint, {kNotify, json}) == 0,
                "Callosum control notification failed");
    }
    Message Exchange(const std::string& endpoint, Message message) {
        const auto key = message.id + ":" + message.phase + "_ack";
        {
            std::lock_guard lock(mutex);
            Require(replies.size() < kLedgerLimit,
                    "Callosum reply ledger is full");
            replies.emplace(key, std::nullopt);
        }
        try {
            Send(endpoint, message);
            std::unique_lock lock(mutex);
            const bool arrived =
                cv.wait_for(lock, std::chrono::milliseconds(config.wait_ms),
                            [&] { return replies.at(key).has_value(); });
            Require(arrived, "Callosum control reply timed out");
            auto reply = *replies.at(key);
            replies.erase(key);
            Require(reply.sender == endpoint && SameTarget(reply, message) &&
                        reply.pool != config.pool_id && !reply.epoch.empty() &&
                        reply.ok,
                    "Callosum receiver rejected target or validation");
            if (!message.epoch.empty())
                Require(reply.epoch == message.epoch,
                        "Callosum receiver epoch changed");
            return reply;
        } catch (...) {
            std::lock_guard lock(mutex);
            replies.erase(key);
            throw;
        }
    }

    void Handle(Message message) {
        if (message.secret != secret || message.campaign != config.campaign ||
            message.pool == config.pool_id ||
            message.tenant != client.tenant_id() || message.id.empty() ||
            message.sender.empty())
            return;
        if (message.phase.ends_with("_ack")) {
            std::lock_guard lock(mutex);
            auto it = replies.find(message.id + ":" + message.phase);
            if (it != replies.end()) {
                it->second = std::move(message);
                cv.notify_all();
            }
            return;
        }
        const auto peer = message.sender;
        auto reply = message;
        reply.phase += "_ack";
        reply.epoch = epoch;
        if (Owned(message.address, message.length))
            reply.pool_offset = *pool->offset_of(
                reinterpret_cast<void*>(message.address), message.length);
        reply.ok = false;
        if (message.phase == "prepare") {
            if (Owned(message.address, message.length) &&
                client.ValidateCallosumTarget(
                    message.key, message.source_generation,
                    message.target_generation, message.address,
                    message.length)) {
                auto existing = targets.find(message.id);
                if (existing != targets.end()) {
                    reply.ok = existing->second.phase == "prepare" &&
                               SameTarget(existing->second, message) &&
                               existing->second.sender == peer;
                } else if (targets.size() < kLedgerLimit) {
                    bool overlap = false;
                    for (const auto& [id, other] : targets) {
                        if (other.phase != "prepare") continue;
                        overlap |=
                            message.address < other.address + other.length &&
                            other.address < message.address + message.length;
                    }
                    if (!overlap) {
                        journal.Mark(message.id + "-rx", "PENDING");
                        targets.emplace(message.id, message);
                        reply.ok = true;
                    }
                }
            }
        } else if (message.phase == "abort" &&
                   Owned(message.address, message.length)) {
            // The sender may abort only before posting or after positive drain.
            // Keep a tombstone so an earlier, delayed prepare cannot resurrect
            // it.
            auto it = targets.find(message.id);
            if (it == targets.end() && targets.size() < kLedgerLimit) {
                it = targets.emplace(message.id, message).first;
            }
            if (it != targets.end() && SameTarget(it->second, message) &&
                it->second.sender == peer) {
                journal.Mark(message.id + "-rx", "DRAINED");
                it->second.phase = "aborted";
                reply.ok = true;
            }
        } else if (message.phase == "verify" && message.epoch == epoch) {
            auto it = targets.find(message.id);
            if (it != targets.end() && it->second.phase != "aborted" &&
                SameTarget(it->second, message) && it->second.sender == peer) {
                // The authenticated sender sends verify only after all native
                // WRITE batches drain. Read every byte before publication.
                std::atomic_thread_fence(std::memory_order_seq_cst);
                reply.ok =
                    ComputeCrcChecksum(reinterpret_cast<void*>(message.address),
                                       message.length) == message.checksum;
                journal.Mark(message.id + "-rx", "VALIDATED");
                it->second.phase = "validated";
                LOG(INFO) << "component=callosum_store event=receiver_validated"
                          << " attempt=" << message.id
                          << " bytes=" << message.length
                          << " content_ok=" << reply.ok << " epoch=" << epoch;
            }
        }
        Send(peer, std::move(reply));
    }
    void Progress() {
        while (running) {
            std::vector<TransferMetadata::NotifyDesc> notifications;
            engine.getNotifies(notifications);
            for (const auto& notification : notifications) {
                if (notification.name != kNotify) continue;
                try {
                    Message message;
                    struct_json::from_json(message, notification.notify_msg);
                    Handle(std::move(message));
                } catch (...) {
                    LOG(ERROR)
                        << "component=callosum_store event=control_rejected";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::vector<pool_relay::Completion> Write(
        const pool_relay::Plan& plan, const pool_relay::SenderPlan& group) {
        // Source membership currently exposes the Store task's source endpoint.
        // Additional physical senders require a real source-grant adapter, not
        // another replica or an invented remote VA.
        if (group.sender != client.GetSegmentEndpoint())
            throw pool_relay::MuxFailure(
                "sender lacks local Store source grant", true);
        for (const auto& target : plan.targets) {
            auto message =
                *std::static_pointer_cast<Message>(target.registration);
            try {
                const auto live = Exchange(target.segment, message);
                Require(live.epoch == target.epoch &&
                            live.pool_offset == target.extent.offset,
                        "receiver authorization changed while queued");
                const auto remote = engine.getMetadata()->getSegmentDescByName(
                    target.segment, true);
                Require(remote && remote->protocol == "rdma",
                        "receiver route is no longer classic RDMA");
            } catch (const std::exception& e) {
                throw pool_relay::MuxFailure(e.what(), true);
            }
        }
        std::vector<pool_relay::Completion> completed;
        size_t done = 0;
        for (size_t start = 0; start < group.writes.size();) {
            const size_t count = std::min(kBatch, group.writes.size() - start);
            std::vector<size_t> offsets, remote_offsets, lengths;
            offsets.reserve(count);
            remote_offsets.reserve(count);
            lengths.reserve(count);
            std::vector<TransferEngine::ScatterTransferRange> ranges;
            ranges.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                const auto& w = group.writes[start + i];
                const auto& t = plan.targets[w.target_index];
                offsets.push_back(w.source.offset - plan.source_extent.offset);
                remote_offsets.push_back(w.destination.offset -
                                         t.extent.offset);
                lengths.push_back(w.source.length);
                ranges.push_back(
                    {TransferRequest::WRITE,
                     t.segment,
                     t.remote_base,
                     t.extent.length,
                     pool->resolve(plan.source_extent.offset,
                                   plan.source_extent.length),
                     plan.source_extent.length,
                     std::span<const size_t>(&offsets.back(), 1),
                     std::span<const size_t>(&remote_offsets.back(), 1),
                     std::span<const size_t>(&lengths.back(), 1),
                     {}});
            }
            const auto began = std::chrono::steady_clock::now();
            auto operation = engine.submitScatter(ranges);
            const auto status = operation.wait();
            if (!status.ok())
                throw pool_relay::MuxFailure("native WRITE failed after drain",
                                             true);
            const auto finished = std::chrono::steady_clock::now();
            const auto duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(finished -
                                                                     began)
                    .count();
            const auto bytes =
                std::accumulate(lengths.begin(), lengths.end(), size_t{0});
            done += bytes;
            const auto& authorization = *std::static_pointer_cast<Message>(
                plan.targets[0].registration);
            LOG(INFO) << "component=callosum_store event=write_drained attempt="
                      << plan.transfer_id << " batch=" << count
                      << " source_generation="
                      << authorization.source_generation
                      << " target_generation="
                      << authorization.target_generation
                      << " batch_bytes=" << bytes << " completed_end=" << done
                      << " native_write_ns=" << duration << " native_start_ns="
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(
                             began.time_since_epoch())
                             .count()
                      << " native_end_ns="
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(
                             finished.time_since_epoch())
                             .count()
                      << " sender=" << group.sender
                      << " protocol=rdma opcode=WRITE";
            for (size_t i = 0; i < count; ++i)
                completed.push_back(
                    {group.writes[start + i].chunk, lengths[i], true});
            start += count;
        }
        return completed;
    }

    CopyResult CopyOnce(const std::string& key,
                        const Replica::Descriptor& source,
                        const std::vector<Replica::Descriptor>& destinations) {
        CopyResult result{false, true};
        if (!Owns(source) || destinations.size() != 1 ||
            !destinations[0].is_memory_replica())
            return result;
        const auto& src = source.get_memory_descriptor().buffer_descriptor;
        const auto& dst =
            destinations[0].get_memory_descriptor().buffer_descriptor;
        if (dst.protocol_ != "rdma" || dst.size_ != src.size_ ||
            dst.transport_endpoint_ == src.transport_endpoint_)
            return result;
        auto remote = engine.getMetadata()->getSegmentDescByName(
            dst.transport_endpoint_, true);
        if (!remote || remote->protocol != "rdma") return result;
        Message message;
        message.id = UuidToString(generate_uuid());
        message.phase = "prepare";
        message.key = key;
        message.tenant = client.tenant_id();
        message.address = dst.buffer_address_;
        message.length = dst.size_;
        message.source_generation = source.id;
        message.target_generation = destinations[0].id;
        message.checksum = ComputeCrcChecksum(
            reinterpret_cast<void*>(src.buffer_address_), src.size_);
        try {
            const auto prepared = Exchange(dst.transport_endpoint_, message);
            message.epoch = prepared.epoch;
            message.pool_offset = prepared.pool_offset;
            LOG(INFO) << "component=callosum_store event=receiver_authorized"
                      << " attempt=" << message.id
                      << " source_pool=" << config.pool_id
                      << " destination_pool=" << prepared.pool
                      << " receiver=" << dst.transport_endpoint_
                      << " source_offset="
                      << *pool->offset_of(
                             reinterpret_cast<void*>(src.buffer_address_),
                             src.size_)
                      << " destination_offset=" << prepared.pool_offset
                      << " receiver_epoch=" << prepared.epoch
                      << " source_generation=" << message.source_generation
                      << " target_generation=" << message.target_generation
                      << " bytes=" << src.size_ << " key_crc="
                      << ComputeCrcChecksum(key.data(), key.size());
            journal.Mark(message.id + "-tx", "PENDING");
            pool_relay::Plan plan;
            plan.transfer_id = message.id;
            plan.key = key;
            plan.worker = client.GetSegmentName();
            plan.requester = dst.transport_endpoint_;
            plan.source_version = std::to_string(source.id);
            plan.source_extent = {
                config.pool_id,
                *pool->offset_of(reinterpret_cast<void*>(src.buffer_address_),
                                 src.size_),
                src.size_};
            plan.backing_lifetime = pool;
            plan.targets.push_back(
                {dst.transport_endpoint_,
                 {prepared.pool, prepared.pool_offset, dst.size_},
                 dst.transport_endpoint_,
                 dst.buffer_address_,
                 prepared.epoch,
                 std::make_shared<Message>(message),
                 "classic_va",
                 message.id,
                 std::to_string(destinations[0].id)});
            plan.senders.push_back({client.GetSegmentEndpoint(), {}});
            for (size_t pos = 0; pos < src.size_;) {
                const auto length = std::min<uint64_t>(kChunk, src.size_ - pos);
                plan.senders[0].writes.push_back(
                    {plan.senders[0].writes.size(),
                     {config.pool_id, plan.source_extent.offset + pos, length},
                     {prepared.pool, prepared.pool_offset + pos, length},
                     0,
                     dst.buffer_address_ + pos});
                pos += length;
            }
            result.safe_to_revoke = false;
            try {
                mux.executeAndDrain(plan);
                result.safe_to_revoke = true;
            } catch (const pool_relay::MuxFailure& e) {
                result.safe_to_revoke = e.drained;
                throw;
            }
            journal.Mark(message.id + "-tx", "DRAINED");
            message.phase = "verify";
            Exchange(dst.transport_endpoint_, message);
            result.success = true;
            LOG(INFO) << "component=callosum_store event=copy_verified"
                      << " attempt=" << message.id << " key_crc="
                      << ComputeCrcChecksum(key.data(), key.size())
                      << " bytes=" << src.size_;
        } catch (...) {
            if (result.safe_to_revoke) {
                journal.Mark(message.id + "-tx", "DRAINED");
                try {
                    message.phase = "abort";
                    Exchange(dst.transport_endpoint_, message);
                } catch (...) {
                    // Receiver retains its pending authorization if the abort
                    // receipt is unavailable. A later retry cannot clear it.
                }
            }
            LOG(ERROR) << "component=callosum_store event=copy_failed"
                       << " attempt=" << message.id
                       << " safe_to_revoke=" << result.safe_to_revoke;
        }
        return result;
    }

    CopyResult Copy(const std::string& key, const Replica::Descriptor& source,
                    const std::vector<Replica::Descriptor>& targets) {
        if (targets.size() != 1 || !source.is_memory_replica() ||
            !targets[0].is_memory_replica())
            return {false, true};
        // A Store replica generation is immutable. Repeated notifications for
        // the same grant join one protocol and cannot create a second writer.
        const auto identity =
            std::make_tuple(client.tenant_id(), key, source.id, targets[0].id);
        std::shared_ptr<std::promise<CopyResult>> promise;
        std::shared_future<CopyResult> future;
        {
            std::lock_guard lock(copy_mutex);
            auto it = copies.find(identity);
            if (it != copies.end()) {
                const auto same = [](const Replica::Descriptor& a,
                                     const Replica::Descriptor& b) {
                    const auto& x = a.get_memory_descriptor().buffer_descriptor;
                    const auto& y = b.get_memory_descriptor().buffer_descriptor;
                    return x.buffer_address_ == y.buffer_address_ &&
                           x.size_ == y.size_ && x.protocol_ == y.protocol_ &&
                           x.transport_endpoint_ == y.transport_endpoint_;
                };
                if (!same(source, it->second.source) ||
                    !same(targets[0], it->second.target))
                    return {false, false};
                future = it->second.result;
            } else {
                if (copies.size() >= kLedgerLimit) return {false, true};
                promise = std::make_shared<std::promise<CopyResult>>();
                future = promise->get_future().share();
                copies.emplace(identity,
                               CopyRecord{source, targets[0], future});
            }
        }
        if (promise) {
            try {
                promise->set_value(CopyOnce(key, source, targets));
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        }
        return future.get();
    }

    bool Materialize(const std::string& key) {
        // One worker/rank owns this narrow synchronous load seam. Serializing
        // materialization also joins repeated keys without a second directory.
        const auto began = std::chrono::steady_clock::now();
        const auto ready = [&](const char* phase) {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - began)
                    .count();
            LOG(INFO) << "component=callosum_store event=replica_usable phase="
                      << phase << " key_crc="
                      << ComputeCrcChecksum(key.data(), key.size())
                      << " request_to_usable_replica_ns=" << elapsed
                      << " clock=requester_steady";
            return true;
        };
        std::lock_guard load_lock(load_mutex);
        auto query = client.Query(key);
        if (!query) return false;
        for (const auto& replica : query->replicas)
            if (Owns(replica)) return ready("local");
        LOG(INFO) << "component=callosum_store event=initial_local_miss"
                  << " pool=" << config.pool_id
                  << " key_crc=" << ComputeCrcChecksum(key.data(), key.size());
        auto task = client.CreateCopyTask(key, {client.GetSegmentName()});
        if (!task) return false;
        LOG(INFO) << "component=callosum_store event=remote_materialization"
                  << " task=" << UuidToString(*task)
                  << " key_crc=" << ComputeCrcChecksum(key.data(), key.size());
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(config.wait_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            auto status = client.QueryTask(*task);
            if (!status || status->status == TaskStatus::FAILED) return false;
            if (status->status == TaskStatus::SUCCESS) {
                auto local = client.Query(key);
                if (local)
                    for (const auto& replica : local->replicas)
                        if (Owns(replica)) return ready("remote");
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        // Only CXL is a possible WRITE target. No GPU load has started.
        // Keep the copy task, source pin, reservation and journal intact.
        return false;
    }

    Client& client;
    TransferEngine& engine;
    CallosumStoreConfig config;
    std::shared_ptr<CxlPoolBackend> pool;
    Journal journal;
    std::string epoch, secret;
    std::atomic<bool> running{true};
    std::thread progress;
    std::mutex mutex, load_mutex;
    std::condition_variable cv;
    std::map<std::string, std::optional<Message>> replies;
    std::map<std::string, Message> targets;  // progress thread is sole mutator.
    pool_relay::SourceMux mux;
    std::mutex copy_mutex;
    struct CopyRecord {
        Replica::Descriptor source, target;
        std::shared_future<CopyResult> result;
    };
    std::map<std::tuple<std::string, std::string, uint64_t, uint64_t>,
             CopyRecord>
        copies;
};

CallosumStoreBridge::CallosumStoreBridge(Client& client, TransferEngine& engine,
                                         CallosumStoreConfig config,
                                         std::shared_ptr<CxlPoolBackend> pool)
    : impl_(std::make_unique<Impl>(client, engine, std::move(config),
                                   std::move(pool))) {}
CallosumStoreBridge::~CallosumStoreBridge() = default;
CallosumStoreBridge::CopyResult CallosumStoreBridge::Copy(
    const std::string& key, const Replica::Descriptor& source,
    const std::vector<Replica::Descriptor>& targets) {
    return impl_->Copy(key, source, targets);
}
bool CallosumStoreBridge::Materialize(const std::string& key) {
    return impl_->Materialize(key);
}
std::vector<pool_relay::MuxEvent> CallosumStoreBridge::MuxStatus() const {
    return impl_->mux.snapshot();
}
bool CallosumStoreBridge::Owns(const Replica::Descriptor& replica) const {
    return impl_->Owns(replica);
}
}  // namespace mooncake
