#pragma once

#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mooncake::pool_relay {

struct Extent {
    std::string pool;
    uint64_t offset = 0;
    uint64_t length = 0;
};

struct Member {
    std::string id;
    std::string pool;
    uint64_t capacity = 0;
    bool can_send = false;
    bool can_receive = false;
};

// Immutable during an operation. Membership means the same physical pool and
// offset namespace, not merely matching device names on different hosts.
class Registry {
   public:
    void add(Member member);
    const Member& member(const std::string& id) const;
    std::vector<Member> candidates(const Extent& extent, bool send) const;

   private:
    std::vector<Member> members_;
};

struct Source {
    Extent extent;
    std::string version;
    std::string checksum;
    // Adapter-owned lease. Destruction releases it, only after DMA is drained.
    std::shared_ptr<void> pin;
};

class Reservation {
   public:
    // An uncommitted reservation must abort on destruction. commit() must be
    // atomic: throwing leaves no published object; success retains residency.
    virtual ~Reservation() = default;
    virtual Extent extent() const = 0;
    virtual void commit() = 0;
};

class Authority {
   public:
    virtual ~Authority() = default;
    virtual Source lookupAndPin(const std::string& key,
                                const std::string& source_pool) = 0;
    virtual std::unique_ptr<Reservation> reserve(const std::string& key,
                                                 const std::string& target_pool,
                                                 uint64_t length) = 0;
    // Must establish destination visibility, compare full-object checksum and
    // revalidate source version/lease. A sender CQE alone is insufficient.
    virtual bool verify(const Source& source, const Reservation& target) = 0;
};

struct ReceiverTarget {
    std::string receiver;
    Extent extent;
    std::string
        segment;  // Engine segment reference, never an application rkey.
    uint64_t remote_base = 0;  // Receiver's engine address for extent.offset.
    std::string epoch;
    // Local registration or remote registration lease, released after drain.
    std::shared_ptr<void> registration;
    std::string address_kind;
    std::string plan_id;
};

class Receivers {
   public:
    virtual ~Receivers() = default;
    // Runs on / calls the helper, which resolves and registers its own mapping.
    virtual ReceiverTarget prepare(const std::string& transfer_id,
                                   const Member& receiver,
                                   const Extent& assigned) = 0;
};

struct Write {
    uint64_t chunk = 0;
    Extent source;
    Extent destination;
    size_t target_index = 0;
    uint64_t remote_address = 0;
};

struct SenderPlan {
    std::string sender;
    std::vector<Write> writes;
};

struct Plan {
    std::string transfer_id;
    std::string key;
    std::string worker;
    std::string requester;
    std::string source_version;
    std::vector<ReceiverTarget> targets;
    std::vector<SenderPlan> senders;
};

struct Completion {
    uint64_t chunk = 0;
    uint64_t bytes = 0;
    bool success = false;
};

class Writers {
   public:
    virtual ~Writers() = default;
    // Dispatch each group to its sender. The adapter may run groups in
    // parallel. MUST drain/fence all dispatched DMA before returning OR
    // throwing; do not return on a mere timeout. Return one physical completion
    // per planned chunk.
    virtual std::vector<Completion> executeAndDrain(const Plan& plan) = 0;
};

// Minimal fan-out dispatcher. The callback is the sender RPC/agent boundary:
// it executes only that sender's group and obeys executeAndDrain's lifetime
// contract. All groups are joined, including when another sender fails.
class ParallelWriters final : public Writers {
   public:
    using Send =
        std::function<std::vector<Completion>(const Plan&, const SenderPlan&)>;
    explicit ParallelWriters(Send send);
    std::vector<Completion> executeAndDrain(const Plan& plan) override;

   private:
    Send send_;
};

enum class Stage {
    Lookup,
    Reserved,
    TargetsReady,
    Dispatched,
    WritesComplete,
    Verified,
    Published,
    Released,
    Failed
};
struct Event {
    std::string transfer_id;
    std::string worker;
    Stage stage;
    uint64_t bytes;
    uint64_t monotonic_ns;
};
using Observer = std::function<void(const Event&)>;

struct Request {
    // Caller supplies a globally unique operation ID; no process-local counter
    // is used as a distributed identity.
    std::string transfer_id, key, source_pool, requester, worker;
    uint64_t chunk_bytes = 1024 * 1024;
};
struct Result {
    Extent destination;
    uint64_t chunks = 0;
    size_t senders = 0, receivers = 0;
};

// Synchronous orchestration core with no RPC/verbs ownership. Adapters and
// registry must outlive run(); callers serialize mutations to their authority.
class Coordinator {
   public:
    Coordinator(const Registry& registry, Authority& authority,
                Receivers& receivers, Writers& writers, Observer observer = {});
    Result run(const Request& request);

   private:
    void emit(const Request&, Stage, uint64_t bytes) const noexcept;
    const Registry& registry_;
    Authority& authority_;
    Receivers& receivers_;
    Writers& writers_;
    Observer observer_;
};

}  // namespace mooncake::pool_relay
