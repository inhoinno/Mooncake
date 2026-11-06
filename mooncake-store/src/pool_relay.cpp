#include "pool_relay.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake::pool_relay {
namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
bool valid(const Extent& e) {
    return !e.pool.empty() && e.length &&
           e.offset <= std::numeric_limits<uint64_t>::max() - e.length;
}
bool same(const Extent& a, const Extent& b) {
    return a.pool == b.pool && a.offset == b.offset && a.length == b.length;
}
}  // namespace

ParallelWriters::ParallelWriters(Send send) : send_(std::move(send)) {
    require(bool(send_), "missing sender dispatch callback");
}

std::vector<Completion> ParallelWriters::executeAndDrain(const Plan& plan) {
    std::vector<std::future<std::vector<Completion>>> pending;
    pending.reserve(plan.senders.size());
    // std::async future destruction also joins already launched operations if
    // launching a later group throws. The plan remains alive for every join.
    for (const auto& sender : plan.senders) {
        const auto* group = &sender;
        pending.push_back(std::async(std::launch::async, [this, &plan, group] {
            return send_(plan, *group);
        }));
    }
    std::vector<Completion> completed;
    std::exception_ptr failure;
    for (auto& task : pending) {
        try {
            auto result = task.get();
            completed.insert(completed.end(), result.begin(), result.end());
        } catch (...) {
            if (!failure) failure = std::current_exception();
        }
    }
    if (failure) std::rethrow_exception(failure);
    return completed;
}

void Registry::add(Member member) {
    require(!member.id.empty() && !member.pool.empty() && member.capacity,
            "invalid pool member");
    for (const auto& old : members_) {
        require(old.id != member.id, "duplicate member");
        require(old.pool != member.pool || old.capacity == member.capacity,
                "pool members disagree on mapped capacity");
    }
    members_.push_back(std::move(member));
}

const Member& Registry::member(const std::string& id) const {
    for (const auto& member : members_)
        if (member.id == id) return member;
    throw std::runtime_error("unknown requester");
}

std::vector<Member> Registry::candidates(const Extent& extent,
                                         bool send) const {
    require(valid(extent), "invalid pool extent");
    std::vector<Member> result;
    for (const auto& member : members_)
        if (member.pool == extent.pool &&
            (send ? member.can_send : member.can_receive) &&
            extent.offset + extent.length <= member.capacity)
            result.push_back(member);
    std::sort(result.begin(), result.end(),
              [](const Member& a, const Member& b) { return a.id < b.id; });
    return result;
}

Coordinator::Coordinator(const Registry& registry, Authority& authority,
                         Receivers& receivers, Writers& writers,
                         Observer observer)
    : registry_(registry),
      authority_(authority),
      receivers_(receivers),
      writers_(writers),
      observer_(std::move(observer)) {}

void Coordinator::emit(const Request& request, Stage stage,
                       uint64_t bytes) const noexcept {
    try {
        if (observer_)
            observer_(Event{
                request.transfer_id, request.worker, stage, bytes,
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count())});
    } catch (...) {
        // Observation cannot turn a committed transfer into a reported failure.
    }
}

Result Coordinator::run(const Request& request) {
    try {
        require(!request.transfer_id.empty() && !request.key.empty() &&
                    !request.worker.empty() && request.chunk_bytes,
                "missing transfer identity or chunk size");
        const auto requester = registry_.member(request.requester);
        require(requester.pool != request.source_pool,
                "local pool hit must use the local read path");
        // Destruction order: targets -> reservation -> source pin. The writer
        // contract guarantees physical drain before any of these are released.
        auto source = authority_.lookupAndPin(request.key, request.source_pool);
        require(valid(source.extent) &&
                    source.extent.pool == request.source_pool && source.pin &&
                    !source.version.empty() && !source.checksum.empty(),
                "lookup did not return a pinned versioned source");
        auto senders = registry_.candidates(source.extent, true);
        require(!senders.empty(), "no eligible sender");
        emit(request, Stage::Lookup, source.extent.length);
        auto reservation = authority_.reserve(request.key, requester.pool,
                                              source.extent.length);
        require(bool(reservation), "destination reservation failed");
        auto destination = reservation->extent();
        require(
            valid(destination) && destination.pool == requester.pool &&
                destination.length == source.extent.length &&
                destination.offset + destination.length <= requester.capacity,
            "invalid destination reservation");
        auto receivers = registry_.candidates(destination, false);
        require(!receivers.empty(), "no eligible receiver");
        emit(request, Stage::Reserved, destination.length);
        Plan plan{request.transfer_id,
                  request.key,
                  request.worker,
                  request.requester,
                  source.version,
                  {},
                  {}};
        const size_t count = static_cast<size_t>(
            std::min<uint64_t>(receivers.size(), destination.length));
        uint64_t offset = destination.offset;
        for (size_t i = 0; i < count; ++i) {
            const uint64_t length = destination.length / count +
                                    (i < destination.length % count ? 1 : 0);
            Extent range{destination.pool, offset, length};
            auto target =
                receivers_.prepare(request.transfer_id, receivers[i], range);
            require(target.receiver == receivers[i].id &&
                        same(target.extent, range) && !target.segment.empty() &&
                        !target.epoch.empty() && target.registration &&
                        target.address_kind == "classic_va" &&
                        target.plan_id == request.transfer_id &&
                        target.remote_base <=
                            std::numeric_limits<uint64_t>::max() - length,
                    "receiver advertised an invalid registered target");
            plan.targets.push_back(std::move(target));
            offset += length;
        }
        emit(request, Stage::TargetsReady, destination.length);
        for (const auto& sender : senders)
            plan.senders.push_back({sender.id, {}});
        std::vector<uint64_t> expected;
        for (size_t i = 0; i < plan.targets.size(); ++i) {
            const auto& target = plan.targets[i];
            for (uint64_t pos = 0; pos < target.extent.length;) {
                const auto length =
                    std::min(request.chunk_bytes, target.extent.length - pos);
                const auto relative =
                    target.extent.offset - destination.offset + pos;
                const uint64_t chunk = expected.size();
                plan.senders[chunk % senders.size()].writes.push_back(Write{
                    chunk,
                    {source.extent.pool, source.extent.offset + relative,
                     length},
                    {destination.pool, destination.offset + relative, length},
                    i,
                    target.remote_base + pos});
                expected.push_back(length);
                pos += length;
            }
        }
        plan.senders.erase(
            std::remove_if(
                plan.senders.begin(), plan.senders.end(),
                [](const SenderPlan& p) { return p.writes.empty(); }),
            plan.senders.end());
        emit(request, Stage::Dispatched, destination.length);
        const auto completions = writers_.executeAndDrain(plan);
        require(completions.size() == expected.size(),
                "missing WRITE completions");
        std::vector<bool> seen(expected.size(), false);
        for (const auto& completion : completions) {
            require(completion.chunk < expected.size(),
                    "unknown WRITE completion");
            require(!seen[completion.chunk] && completion.success &&
                        completion.bytes == expected[completion.chunk],
                    "duplicate, failed or short WRITE completion");
            seen[completion.chunk] = true;
        }
        emit(request, Stage::WritesComplete, destination.length);
        require(authority_.verify(source, *reservation),
                "destination visibility/checksum or source version "
                "verification failed");
        emit(request, Stage::Verified, destination.length);
        reservation->commit();
        emit(request, Stage::Published, destination.length);
        Result result{destination, expected.size(), plan.senders.size(),
                      plan.targets.size()};
        plan.targets.clear();
        reservation.reset();
        source.pin.reset();
        emit(request, Stage::Released, destination.length);
        return result;
    } catch (...) {
        emit(request, Stage::Failed, 0);
        throw;
    }
}
}  // namespace mooncake::pool_relay
