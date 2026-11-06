#include "pool_relay.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

using namespace mooncake::pool_relay;
namespace {
void check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

// Only the adapters simulate memory/registration. The tested coordinator is
// the same C++ source compiled into mooncake_store, not the Python prototype.
struct Fixture : Authority, Receivers, Writers {
    Registry registry;
    std::vector<unsigned char> source = std::vector<unsigned char>(8192, 0);
    std::vector<unsigned char> target = std::vector<unsigned char>(8192, 0);
    std::map<std::string, Extent> published;
    std::vector<Event> events;
    uint64_t next = 37;
    int pins = 0, registrations = 0, reserves = 0, aborts = 0, verifies = 0;
    std::atomic<int> writes{0};
    bool corrupt = false, short_write = false;
    size_t used_senders = 0, used_receivers = 0;

    explicit Fixture(int senders = 2, int receivers = 2) {
        for (int i = 0; i < senders; ++i)
            registry.add(
                {"s" + std::to_string(i), "A", source.size(), true, false});
        for (int i = 0; i < receivers; ++i)
            registry.add(
                {"h" + std::to_string(i), "B", target.size(), false, true});
        // The consuming worker has no receiving NIC capability in this test.
        registry.add({"consumer", "B", target.size(), false, false});
        for (size_t i = 0; i < 257; ++i) source[11 + i] = (i * 17 + 3) % 256;
    }

    struct Allocation : Reservation {
        Fixture& owner;
        std::string key;
        Extent location;
        bool committed = false;
        Allocation(Fixture& owner, std::string key, Extent location)
            : owner(owner),
              key(std::move(key)),
              location(std::move(location)) {}
        ~Allocation() override {
            if (!committed) ++owner.aborts;
        }
        Extent extent() const override { return location; }
        void commit() override {
            check(owner.pins == 1 && owner.registrations > 0,
                  "transfer resources released before commit");
            owner.published.emplace(key, location);
            committed = true;
        }
    };

    Source lookupAndPin(const std::string&, const std::string& pool) override {
        ++pins;
        auto pin = std::shared_ptr<void>(new int(1), [this](void* ptr) {
            delete static_cast<int*>(ptr);
            --pins;
        });
        // Checksum encoding is authority-owned. This fixture verifies all bytes
        // directly; the live authority must supply its checksum implementation.
        return {
            {pool, 11, 257}, "version-7", "fixture-full-byte-equality", pin};
    }
    std::unique_ptr<Reservation> reserve(const std::string& key,
                                         const std::string& pool,
                                         uint64_t length) override {
        check(!published.count(key), "already published key");
        Extent extent{pool, next, length};
        next += length;
        ++reserves;
        return std::make_unique<Allocation>(*this, key, extent);
    }
    bool verify(const Source& src, const Reservation& reservation) override {
        ++verifies;
        const auto dst = reservation.extent();
        check(pins == 1 && registrations > 0,
              "verification lost resource lease");
        return src.version == "version-7" &&
               std::equal(
                   source.begin() + src.extent.offset,
                   source.begin() + src.extent.offset + src.extent.length,
                   target.begin() + dst.offset);
    }
    ReceiverTarget prepare(const std::string& plan_id, const Member& member,
                           const Extent& range) override {
        check(member.id != "consumer", "requester unexpectedly receives WRITE");
        ++registrations;
        auto lease = std::shared_ptr<void>(new int(1), [this](void* ptr) {
            delete static_cast<int*>(ptr);
            --registrations;
        });
        // Each helper advertises a different virtual mapping. Coordinator has
        // no mapping bases and must use exactly the advertised target address.
        const uint64_t base = 0x1000000 * (1 + std::stoul(member.id.substr(1)));
        return {member.id,
                range,
                "engine-" + member.id,
                base + range.offset,
                "registration-epoch-3",
                lease, "classic_va", plan_id};
    }
    std::vector<Completion> executeAndDrain(const Plan& plan) override {
        check(!published.count(plan.key), "published before transfer");
        check(
            plan.requester == "consumer" && plan.source_version == "version-7",
            "lost requester or source version");
        used_senders = plan.senders.size();
        used_receivers = plan.targets.size();
        ParallelWriters dispatcher([this](const Plan& plan,
                                          const SenderPlan& sender) {
            std::vector<Completion> completed;
            check(registry.member(sender.sender).pool == "A",
                  "wrong sender pool");
            for (const auto& write : sender.writes) {
                const auto& remote = plan.targets.at(write.target_index);
                const uint64_t base =
                    0x1000000 * (1 + std::stoul(remote.receiver.substr(1)));
                const auto offset = write.remote_address - base;
                check(offset == write.destination.offset,
                      "wrong receiver mapping");
                check(offset >= remote.extent.offset &&
                          offset + write.source.length <=
                              remote.extent.offset + remote.extent.length,
                      "WRITE outside assigned receiver region");
                std::copy_n(source.begin() + write.source.offset,
                            write.source.length, target.begin() + offset);
                completed.push_back({write.chunk, write.source.length, true});
                ++writes;
            }
            return completed;
        });
        auto completed = dispatcher.executeAndDrain(plan);
        if (corrupt) target[plan.targets.front().extent.offset] ^= 1;
        if (short_write) --completed.front().bytes;
        std::reverse(completed.begin(), completed.end());
        return completed;
    }
    Request request(std::string id = "op-1", std::string key = "kv") {
        return {std::move(id), std::move(key), "A", "consumer", "worker-0", 16};
    }
    Coordinator coordinator() {
        return Coordinator(
            registry, *this, *this, *this,
            [this](const Event& event) { events.push_back(event); });
    }
    void localRead(const std::string& key) {
        const auto extent = published.at(key);
        check(std::equal(source.begin() + 11, source.begin() + 268,
                         target.begin() + extent.offset),
              "local KV bytes differ");
    }
};

void preflight() {
    Fixture f;
    check(f.registry.candidates({"A", 11, 257}, true).size() == 2,
          "sender discovery");
    check(f.registry.candidates({"B", 37, 257}, false).size() == 2,
          "helper discovery");
    bool rejected = false;
    try {
        f.registry.add({"bad", "B", 1, false, true});
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    check(rejected, "accepted inconsistent pool mapping");
}

void functional() {
    for (const auto& counts : {std::pair{1, 1}, {1, 2}, {2, 2}, {4, 2}, {2, 4}}) {
        Fixture f(counts.first, counts.second);
        auto c = f.coordinator();
        auto result = c.run(f.request());
        check(result.senders == static_cast<size_t>(counts.first) &&
                  result.receivers == static_cast<size_t>(counts.second),
              "missing participant");
        check(
            result.destination.offset == 37 && result.destination.length == 257,
            "reservation projection");
        check(f.reserves == 1 && f.aborts == 0 && f.verifies == 1,
              "reservation lifecycle");
        check(f.pins == 0 && f.registrations == 0, "resource leak");
        int writes = f.writes;
        f.localRead("kv");
        f.localRead("kv");
        check(f.writes == writes, "local reuse issued network WRITE");
        c.run(f.request("op-2", "kv2"));
        f.localRead("kv2");
        check(f.published.at("kv").offset + 257 <= f.published.at("kv2").offset,
              "multi-key reservations overlap");
    }
}

void failure() {
    for (bool checksum_failure : {false, true}) {
        Fixture f;
        f.corrupt = checksum_failure;
        f.short_write = !checksum_failure;
        auto c = f.coordinator();
        bool rejected = false;
        try {
            c.run(f.request());
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        check(rejected && f.published.empty() && f.aborts == 1,
              "invalid bytes published");
        check(f.pins == 0 && f.registrations == 0, "failure resource leak");
        check(f.events.back().stage == Stage::Failed, "failure status missing");
    }
    // A failed sender must not release reservation/MRs while another is live.
    std::atomic<int> drained{0};
    ParallelWriters dispatcher([&](const Plan&, const SenderPlan& sender) {
        ++drained;
        if (sender.sender == "s0")
            throw std::runtime_error("sender failed after drain");
        return std::vector<Completion>{};
    });
    Fixture f;
    Coordinator c(f.registry, f, f, dispatcher);
    bool rejected = false;
    try {
        c.run(f.request());
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    check(rejected && drained == 2 && f.aborts == 1 && f.registrations == 0,
          "fan-out did not drain every sender");

    // A caller's wait deadline is not cancellation of the active coordinator.
    // No source pin, reservation or target is released while the writer waits.
    struct DelayedWriter : Writers {
        Fixture& fixture;
        std::promise<void> entered, release;
        explicit DelayedWriter(Fixture& f) : fixture(f) {}
        std::vector<Completion> executeAndDrain(const Plan& p) override {
            auto wait = release.get_future();
            entered.set_value();
            wait.wait();
            return fixture.executeAndDrain(p);
        }
    };
    Fixture delayed(1, 1);
    DelayedWriter writer(delayed);
    Coordinator delayed_coordinator(delayed.registry, delayed, delayed, writer);
    auto entered = writer.entered.get_future();
    auto running = std::async(std::launch::async, [&] {
        return delayed_coordinator.run(delayed.request());
    });
    entered.wait();
    const bool pending = running.wait_for(std::chrono::milliseconds(5)) ==
                         std::future_status::timeout;
    const bool retained = delayed.pins == 1 && delayed.registrations == 1 &&
                          delayed.aborts == 0 && delayed.published.empty();
    // Always release the fixture writer, including if the assertion will fail.
    writer.release.set_value();
    running.get();
    check(pending && retained, "caller timeout released undrained resources");
    check(delayed.pins == 0 && delayed.registrations == 0 &&
              delayed.published.size() == 1,
          "late successful completion did not release exactly after commit");
}

void status() {
    Fixture f;
    auto c = f.coordinator();
    c.run(f.request());
    const std::vector<Stage> stages{Stage::Lookup,         Stage::Reserved,
                                    Stage::TargetsReady,   Stage::Dispatched,
                                    Stage::WritesComplete, Stage::Verified,
                                    Stage::Published,      Stage::Released};
    check(f.events.size() == stages.size(), "missing phase events");
    uint64_t previous = 0;
    for (size_t i = 0; i < stages.size(); ++i) {
        const auto& event = f.events[i];
        check(event.stage == stages[i] && event.transfer_id == "op-1" &&
                  event.worker == "worker-0" && event.bytes == 257 &&
                  event.monotonic_ns >= previous,
              "incorrect correlated event");
        previous = event.monotonic_ns;
    }
}
}  // namespace

int main(int argc, char** argv) {
    try {
        check(argc == 2, "select preflight|functional|failure|status");
        const std::string mode = argv[1];
        if (mode == "preflight")
            preflight();
        else if (mode == "functional")
            functional();
        else if (mode == "failure")
            failure();
        else if (mode == "status")
            status();
        else
            throw std::runtime_error("unknown case");
        std::cout << "pool_relay tier=T0 case=" << mode << " status=PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "pool_relay tier=T0 status=FAIL error=" << error.what()
                  << '\n';
        return 1;
    }
}
