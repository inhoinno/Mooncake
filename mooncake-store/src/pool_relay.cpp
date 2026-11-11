#include "pool_relay.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <stdexcept>
#include <utility>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <atomic>

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

namespace {
// This primitive joins callbacks, not physical DMA. ParallelWriters supplies
// drain-guaranteeing callbacks; SourceMux classifies callback uncertainty.
std::vector<Completion> joinSenderGroups(const Plan& plan,
                                         const ParallelWriters::Send& send) {
    std::vector<std::future<std::vector<Completion>>> pending;
    pending.reserve(plan.senders.size());
    // std::async future destruction also joins already launched operations if
    // launching a later group throws. The plan remains alive for every join.
    for (const auto& sender : plan.senders) {
        const auto* group = &sender;
        pending.push_back(std::async(std::launch::async, [&send, &plan, group] {
            return send(plan, *group);
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
}  // namespace

std::vector<Completion> ParallelWriters::executeAndDrain(const Plan& plan) {
    return joinSenderGroups(plan, send_);
}

namespace {
uint64_t muxNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::string muxManifest(const Plan& p) {
    require(!p.transfer_id.empty() && !p.key.empty() &&
                !p.source_version.empty() && valid(p.source_extent) &&
                !p.targets.empty() && !p.senders.empty() &&
                p.senders.size() <= 4,
            "invalid mux identity/source/participants");
    std::ostringstream encoded;
    auto text = [&](const std::string& s) { encoded << s.size() << ':' << s; };
    auto extent = [&](const Extent& e) {
        text(e.pool);
        encoded << ',' << e.offset << ',' << e.length << ';';
    };
    text(p.key);
    text(p.worker);
    text(p.requester);
    text(p.source_version);
    extent(p.source_extent);
    for (const auto& t : p.targets) {
        require(valid(t.extent) && !t.receiver.empty() && !t.segment.empty() &&
                    !t.epoch.empty() && t.registration &&
                    t.address_kind == "classic_va" &&
                    t.plan_id == p.transfer_id &&
                    t.remote_base <=
                        std::numeric_limits<uint64_t>::max() - t.extent.length,
                "invalid mux receiver authorization");
        text(t.receiver);
        text(t.segment);
        text(t.epoch);
        text(t.generation);
        extent(t.extent);
        encoded << t.remote_base << ';';
    }
    std::set<std::string> members;
    std::set<uint64_t> chunks;
    std::vector<std::pair<uint64_t, uint64_t>> source;
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> targets(
        p.targets.size());
    const auto destination_start =
        std::min_element(p.targets.begin(), p.targets.end(),
                         [](const auto& a, const auto& b) {
                             return a.extent.offset < b.extent.offset;
                         })
            ->extent.offset;
    for (const auto& group : p.senders) {
        require(!group.sender.empty() && members.insert(group.sender).second &&
                    !group.writes.empty(),
                "duplicate/empty mux sender");
        text(group.sender);
        for (const auto& w : group.writes) {
            require(w.target_index < p.targets.size() &&
                        chunks.insert(w.chunk).second && valid(w.source) &&
                        valid(w.destination),
                    "invalid mux assignment");
            const auto& t = p.targets[w.target_index];
            require(
                w.source.pool == p.source_extent.pool &&
                    w.source.offset >= p.source_extent.offset &&
                    w.source.offset - p.source_extent.offset <=
                        p.source_extent.length &&
                    w.source.length <=
                        p.source_extent.length -
                            (w.source.offset - p.source_extent.offset) &&
                    w.destination.pool == t.extent.pool &&
                    w.destination.offset >= t.extent.offset &&
                    w.destination.offset - t.extent.offset <= t.extent.length &&
                    w.destination.length <=
                        t.extent.length -
                            (w.destination.offset - t.extent.offset) &&
                    w.source.length == w.destination.length &&
                    w.source.offset - p.source_extent.offset ==
                        w.destination.offset - destination_start &&
                    w.remote_address == t.remote_base + (w.destination.offset -
                                                         t.extent.offset),
                "mux assignment outside source/target grant");
            source.emplace_back(w.source.offset, w.source.length);
            targets[w.target_index].emplace_back(w.destination.offset,
                                                 w.destination.length);
            encoded << w.chunk << ',' << w.target_index << ','
                    << w.remote_address << ';';
            extent(w.source);
            extent(w.destination);
        }
    }
    auto covers = [&](auto ranges, const Extent& e) {
        std::sort(ranges.begin(), ranges.end());
        uint64_t cursor = e.offset;
        for (const auto& [start, length] : ranges) {
            require(start == cursor, "mux coverage gap/overlap");
            cursor += length;
        }
        require(cursor == e.offset + e.length, "incomplete mux coverage");
    };
    covers(source, p.source_extent);
    for (size_t i = 0; i < targets.size(); ++i)
        covers(targets[i], p.targets[i].extent);
    std::vector<std::pair<uint64_t, uint64_t>> destination_ranges;
    for (const auto& t : p.targets) {
        require(t.extent.pool == p.targets[0].extent.pool,
                "one Store object requires one destination pool");
        destination_ranges.emplace_back(t.extent.offset, t.extent.length);
    }
    require(destination_start <=
                std::numeric_limits<uint64_t>::max() - p.source_extent.length,
            "mux destination overflow");
    covers(destination_ranges,
           Extent{p.targets[0].extent.pool, destination_start,
                  p.source_extent.length});
    // Separate target descriptors must not authorize overlapping physical VAs.
    for (size_t i = 0; i < p.targets.size(); ++i)
        for (size_t j = 0; j < i; ++j) {
            const auto& a = p.targets[i];
            const auto& b = p.targets[j];
            require(a.segment != b.segment ||
                        a.remote_base >= b.remote_base + b.extent.length ||
                        b.remote_base >= a.remote_base + a.extent.length,
                    "mux target aliases");
        }
    return encoded.str();
}
}  // namespace

struct SourceMux::Impl {
    struct Job {
        Plan plan;
        std::string manifest;
        MuxEvent event;
        uint64_t queued = 0, admitted = 0;
        std::vector<Completion> result;
        std::exception_ptr failure;
    };
    ParallelWriters::Send send;
    size_t capacity, limit, active = 0;
    Observer observer;
    mutable std::mutex mutex;
    std::condition_variable changed;
    std::map<std::string, std::shared_ptr<Job>> jobs;
    std::deque<std::shared_ptr<Job>> queue;
    void emit(const MuxEvent& e) noexcept {
        try {
            if (observer) observer(e);
        } catch (...) {
        }
    }
};

SourceMux::SourceMux(ParallelWriters::Send send, size_t capacity, size_t limit,
                     Observer observer)
    : impl_(std::make_unique<Impl>()) {
    require(bool(send) && capacity > 0 && limit > 0,
            "invalid mux callback/capacity");
    impl_->send = std::move(send);
    impl_->capacity = capacity;
    impl_->limit = limit;
    impl_->observer = std::move(observer);
}
SourceMux::~SourceMux() {
    std::lock_guard lock(impl_->mutex);
    for (const auto& [id, job] : impl_->jobs)
        if (job->event.state == "QUEUED" || job->event.state == "DISPATCHED" ||
            job->event.state == "QUARANTINED")
            std::terminate();  // A destructor cannot prove drain or revoke DMA.
}
std::vector<MuxEvent> SourceMux::snapshot() const {
    std::lock_guard lock(impl_->mutex);
    std::vector<MuxEvent> out;
    for (const auto& [id, job] : impl_->jobs) out.push_back(job->event);
    return out;
}
bool SourceMux::cancelQueued(const std::string& id) {
    MuxEvent event;
    {
        std::lock_guard lock(impl_->mutex);
        auto it = impl_->jobs.find(id);
        if (it == impl_->jobs.end() || it->second->event.state != "QUEUED")
            return false;
        auto job = it->second;
        impl_->queue.erase(
            std::find(impl_->queue.begin(), impl_->queue.end(), job));
        job->failure = std::make_exception_ptr(
            MuxFailure("cancelled before submission", true));
        event = {id, "CANCELLED_NOT_POSTED", muxNow(), muxNow() - job->queued,
                 0,  job->event.bytes};
        job->event = event;
        job->plan = Plan{};
        impl_->changed.notify_all();
    }
    impl_->emit(event);
    return true;
}
std::vector<Completion> SourceMux::executeAndDrain(const Plan& plan) {
    auto& s = *impl_;
    std::string manifest;
    try {
        manifest = muxManifest(plan);
    } catch (const std::exception& e) {
        std::lock_guard lock(s.mutex);
        const auto old = s.jobs.find(plan.transfer_id);
        const bool drained =
            old == s.jobs.end() || (old->second->event.state != "QUEUED" &&
                                    old->second->event.state != "DISPATCHED" &&
                                    old->second->event.state != "QUARANTINED");
        throw MuxFailure(e.what(), drained);
    }
    std::shared_ptr<Impl::Job> job;
    MuxEvent queued_event;
    {
        std::unique_lock lock(s.mutex);
        auto old = s.jobs.find(plan.transfer_id);
        if (old != s.jobs.end()) {
            job = old->second;
            if (job->manifest != manifest)
                throw MuxFailure("conflicting mux replay",
                                 job->event.state == "DRAINED" ||
                                     job->event.state == "FAILED_DRAINED");
            s.changed.wait(lock, [&] {
                return job->event.state != "QUEUED" &&
                       job->event.state != "DISPATCHED";
            });
            if (job->failure) std::rethrow_exception(job->failure);
            return job->result;
        }
        if (s.queue.size() >= s.limit || s.jobs.size() >= 4096)
            throw MuxFailure("mux queue/replay ledger full; no work posted",
                             true);
        for (const auto& [id, other] : s.jobs)
            for (const auto& a : plan.targets)
                for (const auto& b : other->plan.targets) {
                    if (!(a.extent.pool != b.extent.pool ||
                          a.extent.offset >=
                              b.extent.offset + b.extent.length ||
                          b.extent.offset >= a.extent.offset + a.extent.length))
                        throw MuxFailure(
                            "mux target overlaps active/quarantined "
                            "reservation",
                            false);
                }
        job = std::make_shared<Impl::Job>();
        job->plan = plan;
        job->manifest = manifest;
        job->queued = muxNow();
        job->event = {plan.transfer_id,         "QUEUED", job->queued, 0, 0,
                      plan.source_extent.length};
        s.jobs.emplace(plan.transfer_id, job);
        s.queue.push_back(job);
        queued_event = job->event;
    }
    s.emit(queued_event);
    {
        std::unique_lock lock(s.mutex);
        s.changed.wait(lock, [&] {
            return job->failure ||
                   (!s.queue.empty() && s.queue.front() == job &&
                    s.active < s.capacity);
        });
        if (job->failure) std::rethrow_exception(job->failure);
        s.queue.pop_front();
        ++s.active;
        job->admitted = muxNow();
        job->event = {plan.transfer_id,
                      "DISPATCHED",
                      job->admitted,
                      job->admitted - job->queued,
                      0,
                      plan.source_extent.length};
        s.changed.notify_all();
    }
    s.emit(job->event);
    std::atomic<bool> uncertain{false};
    bool drained = true;
    try {
        auto results = joinSenderGroups(
            job->plan, [&](const Plan& p, const SenderPlan& g) {
                try {
                    return s.send(p, g);
                } catch (const MuxFailure& e) {
                    if (!e.drained) uncertain = true;
                    throw;
                } catch (...) {
                    uncertain = true;
                    throw;
                }
            });
        std::map<uint64_t, uint64_t> expected;
        for (const auto& g : plan.senders)
            for (const auto& w : g.writes)
                expected.emplace(w.chunk, w.source.length);
        std::map<uint64_t, Completion> seen;
        for (const auto& r : results) {
            require(expected.count(r.chunk) && r.success &&
                        r.bytes == expected.at(r.chunk),
                    "failed/short/unknown mux completion");
            auto [it, inserted] = seen.emplace(r.chunk, r);
            require(inserted || (it->second.bytes == r.bytes &&
                                 it->second.success == r.success),
                    "conflicting mux completion");
        }
        require(seen.size() == expected.size(), "missing mux completions");
        for (const auto& [id, r] : seen) job->result.push_back(r);
    } catch (...) {
        drained = !uncertain.load();
        try {
            throw;
        } catch (const std::exception& e) {
            job->failure =
                std::make_exception_ptr(MuxFailure(e.what(), drained));
        } catch (...) {
            job->failure = std::make_exception_ptr(
                MuxFailure("unknown mux failure", false));
            drained = false;
        }
    }
    MuxEvent terminal;
    {
        std::lock_guard lock(s.mutex);
        const auto finished = muxNow();
        terminal = {plan.transfer_id,
                    job->failure ? (drained ? "FAILED_DRAINED" : "QUARANTINED")
                                 : "DRAINED",
                    finished,
                    job->admitted - job->queued,
                    finished - job->admitted,
                    plan.source_extent.length};
        job->event = terminal;
        --s.active;
        if (drained)
            job->plan =
                Plan{};  // Unknown writers retain backing and authorizations.
        s.changed.notify_all();
    }
    s.emit(terminal);
    if (job->failure) std::rethrow_exception(job->failure);
    return job->result;
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
