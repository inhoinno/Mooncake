#include "callosum_store.h"
#include "client_service.h"
#include "transfer_engine.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cassert>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <mutex>
#include <type_traits>

using namespace mooncake;
static_assert(!std::is_base_of_v<pool_relay::Writers, pool_relay::SourceMux>);
namespace {
void Check(bool condition) {
    if (!condition) throw std::runtime_error("assertion failed");
}
struct Fixture {
    std::string directory;
    CallosumStoreConfig ca, cb;
    std::shared_ptr<CxlPoolBackend> pa, pb;
    Client a{"receiver"}, b{"sender"};
    TransferEngine ea{"receiver"}, eb{"sender"};
    std::unique_ptr<CallosumStoreBridge> ba, bb;
    Replica::Descriptor source, target;
    std::atomic<bool> published{false};
    explicit Fixture(size_t length = 1024 * 1024 + 31,
                     uint64_t mux_capacity = 2) {
        char pattern[] = "/tmp/callosum-store-adapter-XXXXXX";
        directory = mkdtemp(pattern);
        const auto token = directory + "/token";
        std::ofstream(token) << std::string(48, 'x');
        chmod(token.c_str(), 0600);
        for (auto* config : {&ca, &cb}) {
            config->campaign = directory;
            config->pool_id = config == &ca ? "A" : "B";
            config->path = directory + "/" + config->pool_id;
            config->backend_kind = "file";
            config->capacity = 64 * 1024 * 1024;
            config->owned_size = config->capacity;
            config->alignment = sysconf(_SC_PAGESIZE);
            config->token_file = token;
            config->journal = config->path + ".journal";
            config->wait_ms = 2000;
            config->mux_capacity = mux_capacity;
            std::ofstream file(config->path);
            file.seekp(config->capacity - 1);
            file.put(0);
        }
        pa = ca.Map();
        pb = cb.Map();
        auto* bytes = static_cast<unsigned char*>(pb->resolve(0, length));
        for (size_t i = 0; i < length; ++i) bytes[i] = (i * 19 + 7) % 251;
        source = {
            11,
            {{length, reinterpret_cast<uintptr_t>(bytes), "rdma", "sender"}}};
        target = {22,
                  {{length, reinterpret_cast<uintptr_t>(pa->resolve(0, length)),
                    "rdma", "receiver"}}};
        ba = std::make_unique<CallosumStoreBridge>(a, ea, ca, pa);
        bb = std::make_unique<CallosumStoreBridge>(b, eb, cb, pb);
        a.query = [&](const std::string& key) -> std::optional<QueryResult> {
            if (key != "connector-real-key-fixture") return std::nullopt;
            return QueryResult{published ? std::vector{source, target}
                                         : std::vector{source}};
        };
        a.copy = [&](const std::string& key) {
            const auto result = bb->Copy(key, source, {target});
            published = result.success;
            return result.success;
        };
    }
    ~Fixture() {
        ba.reset();
        bb.reset();
        pa.reset();
        pb.reset();
        std::filesystem::remove_all(directory);
    }
};
void Preflight() {
    Fixture f;
    Check(f.pa->master_managed_allocation());
    CxlPoolError error;
    Check(!f.pa->alloc("not-a-second-allocator", 16, &error));
    Check(!f.pa->metadata_lookup("not-a-second-directory", &error));
    auto bad = f.target;
    bad.memory.buffer_descriptor.buffer_address_ += f.ca.capacity;
    auto rejected = f.bb->Copy("outside-arena", f.source, {bad});
    Check(!rejected.success && rejected.safe_to_revoke && f.eb.bytes == 0);
    f.ea.metadata.protocol = "tcp";
    f.eb.metadata.protocol = "tcp";
    rejected = f.bb->Copy("tcp-is-not-rdma", f.source, {f.target});
    Check(!rejected.success && rejected.safe_to_revoke && f.eb.bytes == 0);
    // A retained journal must reject process restart before any new allocation.
    std::ofstream(f.directory + "/pending")
        << "IDENTITY " << f.ca.campaign << ":A\nunknown-rx PENDING\n";
    auto pending = f.ca;
    pending.journal = f.directory + "/pending";
    bool blocked = false;
    try {
        CallosumStoreBridge another(f.a, f.ea, pending, f.pa);
    } catch (const std::exception&) {
        blocked = true;
    }
    Check(blocked);
    std::ofstream(f.directory + "/truncated") << "unknown";
    pending.journal = f.directory + "/truncated";
    blocked = false;
    try {
        CallosumStoreBridge another(f.a, f.ea, pending, f.pa);
    } catch (const std::exception&) {
        blocked = true;
    }
    Check(blocked);
}
void Function() {
    for (size_t length : {size_t(4096), size_t(1024 * 1024 + 31),
                          size_t(33 * 1024 * 1024 + 3)}) {
        Fixture f(length);
        Check(!f.ba->Owns(f.source) && f.bb->Owns(f.source));
        Check(f.ba->Materialize("connector-real-key-fixture"));
        Check(f.published && f.a.tasks == 1 && f.eb.bytes == length);
        Check(f.eb.batches ==
              (length + 32 * 1024 * 1024 - 1) / (32 * 1024 * 1024));
        Check(std::memcmp(f.pa->base(), f.pb->base(), length) == 0);
        Check(f.ba->Materialize("connector-real-key-fixture"));
        Check(f.a.tasks == 1 && f.eb.bytes == length);
        Check(!f.ba->Materialize("authoritative-miss"));
    }
}

void RequestLifecycle() {
    // Execute the production Materialize polling path. Store RPC/task state is
    // a double: this proves the client/bridge contract, not MasterService RPCs
    // or a production Store-to-multiplexer connection.
    Fixture f;
    std::promise<void> queued;
    auto accepted = queued.get_future();
    f.a.copy = [&](const std::string&) {
        queued.set_value();
        return true;
    };
    f.a.query_task = [&]() -> std::optional<QueryTaskResponse> {
        return QueryTaskResponse{f.published.load() ? TaskStatus::SUCCESS
                                                    : TaskStatus::PENDING};
    };
    auto demand = std::async(std::launch::async, [&] {
        return f.ba->Materialize("connector-real-key-fixture");
    });
    const bool enqueued =
        accepted.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    if (!enqueued) {
        demand.wait();
        Check(false);
    }
    Check(f.a.last_key == "connector-real-key-fixture");
    Check(f.a.last_targets == std::vector<std::string>{"receiver"});
    Check(!f.published && f.eb.bytes == 0);
    Check(demand.wait_for(std::chrono::milliseconds(20)) ==
          std::future_status::timeout);
    const auto copied = f.bb->Copy(f.a.last_key, f.source, {f.target});
    // Emulate Store CopyEnd only after the production bridge validates content.
    f.published = copied.success;
    Check(copied.success && copied.safe_to_revoke && demand.get());
    Check(f.ba->Materialize(f.a.last_key));
    Check(f.a.tasks == 1 &&
          f.eb.bytes == f.source.memory.buffer_descriptor.size_);
}

void StoreMuxIntegration() {
    // Production Store bridge load and copy methods, not two isolated unit
    // tests. RPC/Store authority and DMA are the external test-double
    // boundaries.
    std::ostringstream trace;
    auto* original = std::cerr.rdbuf(trace.rdbuf());
    try {
        {
            Fixture f(1024 * 1024 + 31);
            f.eb.on_submit = [&] {
                const auto status = f.bb->MuxStatus();
                Check(status.size() == 1 && status[0].state == "DISPATCHED");
                Check(!f.published);  // DMA cannot start after publication.
            };
            Check(f.ba->Materialize("connector-real-key-fixture"));
            Check(f.published && f.eb.bytes == 1024 * 1024 + 31);
            Check(f.ba->Materialize("connector-real-key-fixture"));
            Check(f.a.tasks == 1);
            const auto replay =
                f.bb->Copy("connector-real-key-fixture", f.source, {f.target});
            Check(replay.success && replay.safe_to_revoke);
            Check(f.eb.bytes == 1024 * 1024 + 31 &&
                  f.bb->MuxStatus().size() == 1);
            Check(f.bb->MuxStatus()[0].state == "DRAINED");
        }
        std::cerr.rdbuf(original);
    } catch (...) {
        std::cerr.rdbuf(original);
        throw;
    }
    Check(trace.str().find("event=mux_queued") != std::string::npos);
    Check(trace.str().find("event=mux_admitted") != std::string::npos);
    Check(trace.str().find("event=mux_gathered") != std::string::npos);
}

pool_relay::Plan MuxPlan(const std::string& id, uint64_t destination = 8192) {
    using namespace pool_relay;
    Plan p;
    p.transfer_id = id;
    p.key = "actual-store-key";
    p.source_version = "11";
    p.source_extent = {"B", 4096, 97};
    p.backing_lifetime = std::make_shared<int>(1);
    p.targets = {{"a1",
                  {"A", destination, 49},
                  "receiver-1",
                  0x100000 + destination,
                  "epoch-1",
                  std::make_shared<int>(1),
                  "classic_va",
                  id,
                  "22"},
                 {"a2",
                  {"A", destination + 49, 48},
                  "receiver-2",
                  0x200000 + destination + 49,
                  "epoch-2",
                  std::make_shared<int>(1),
                  "classic_va",
                  id,
                  "22"}};
    p.senders = {{"b1", {}}, {"b2", {}}};
    uint64_t source = 4096, chunk = 0;
    for (size_t i = 0; i < p.targets.size(); ++i) {
        const auto& t = p.targets[i];
        for (uint64_t pos = 0; pos < t.extent.length;) {
            const auto n = std::min<uint64_t>(32, t.extent.length - pos);
            p.senders[chunk % 2].writes.push_back(
                {chunk,
                 {"B", source, n},
                 {"A", t.extent.offset + pos, n},
                 i,
                 t.remote_base + pos});
            source += n;
            pos += n;
            ++chunk;
        }
    }
    return p;
}
std::vector<pool_relay::Completion> Finished(const pool_relay::SenderPlan& g) {
    std::vector<pool_relay::Completion> out;
    for (const auto& w : g.writes)
        out.push_back({w.chunk, w.source.length, true});
    return out;
}
template <class Predicate>
bool Until(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}
void NativeMuxFunction() {
    using namespace pool_relay;
    auto plan = MuxPlan("scatter");
    std::weak_ptr<void> backing = plan.backing_lifetime;
    std::promise<void> permit;
    auto gate = permit.get_future().share();
    std::atomic<int> entered{0}, fast{0}, calls{0};
    SourceMux mux([&](const Plan& p, const SenderPlan& group) {
        ++entered;
        ++calls;
        Check(!backing.expired() && p.key == "actual-store-key" &&
              p.source_version == "11");
        Check(p.targets[0].generation == "22" &&
              p.targets[1].epoch == "epoch-2");
        if (group.sender == "b1")
            gate.wait();
        else
            ++fast;
        auto out = Finished(group);
        out.push_back(
            out[0]);  // Repeated identical terminal evidence counts once.
        return out;
    });
    auto running = std::async(std::launch::async,
                              [&] { return mux.executeAndDrain(plan); });
    const bool both = Until([&] { return entered == 2 && fast == 1; });
    const bool waits = running.wait_for(std::chrono::milliseconds(5)) ==
                       std::future_status::timeout;
    permit.set_value();
    const auto result = running.get();
    Check(both && waits && result.size() == 4 && calls == 2);
    Check(mux.snapshot()[0].state == "DRAINED");
    Check(mux.executeAndDrain(plan).size() == 4 && calls == 2);
    auto bad = plan;
    bad.source_version = "wrong-generation";
    bool rejected = false;
    try {
        mux.executeAndDrain(bad);
    } catch (...) {
        rejected = true;
    }
    Check(rejected && calls == 2);
    bad.backing_lifetime.reset();
    plan.backing_lifetime.reset();
    Check(backing.expired());  // No hidden retained successful DMA references.

    for (int fault : {0, 1, 2}) {
        auto p = MuxPlan("invalid-" + std::to_string(fault));
        if (fault == 0) p.senders[0].writes[0].source.offset++;
        if (fault == 1) p.senders[0].writes[0].remote_address++;
        if (fault == 2) p.targets[0].epoch.clear();
        bool invalid = false;
        try {
            mux.executeAndDrain(p);
        } catch (...) {
            invalid = true;
        }
        Check(invalid && calls == 2);
    }
    SourceMux short_write([](const Plan&, const SenderPlan& g) {
        auto result = Finished(g);
        result[0].bytes--;
        return result;
    });
    rejected = false;
    try {
        short_write.executeAndDrain(MuxPlan("short"));
    } catch (const MuxFailure& e) {
        rejected = e.drained;
    }
    Check(rejected && short_write.snapshot()[0].state == "FAILED_DRAINED");
}
void NativeMuxFifo() {
    using namespace pool_relay;
    std::promise<void> permit;
    auto gate = permit.get_future().share();
    std::atomic<int> entered{0};
    std::mutex mutex;
    std::vector<std::string> admission;
    SourceMux mux(
        [&](const Plan& p, const SenderPlan& group) {
            if (p.transfer_id == "first") {
                ++entered;
                gate.wait();
            }
            return Finished(group);
        },
        1, 4,
        [&](const MuxEvent& e) {
            if (e.state == "DISPATCHED") {
                std::lock_guard lock(mutex);
                admission.push_back(e.transfer_id);
            }
        });
    auto p = MuxPlan("first");
    auto first =
        std::async(std::launch::async, [&] { return mux.executeAndDrain(p); });
    const bool started = Until([&] { return entered == 2; });
    auto second = std::async(std::launch::async, [&] {
        return mux.executeAndDrain(MuxPlan("second", 16384));
    });
    const bool queued = Until([&] {
        auto s = mux.snapshot();
        return s.size() == 2;
    });
    auto third = std::async(std::launch::async, [&] {
        return mux.executeAndDrain(MuxPlan("third", 24576));
    });
    const bool all = Until([&] { return mux.snapshot().size() == 3; });
    const bool first_held = first.wait_for(std::chrono::milliseconds(5)) ==
                            std::future_status::timeout;
    permit.set_value();
    first.get();
    second.get();
    third.get();
    Check(started && queued && all && first_held);
    Check(admission == std::vector<std::string>({"first", "second", "third"}));
    Check(mux.snapshot()[1].queue_wait_ns > 0);
}

void NativeMuxProgressAndCancel() {
    using namespace pool_relay;
    std::promise<void> permit;
    auto gate = permit.get_future().share();
    std::atomic<int> entered{0}, other{0};
    SourceMux progress(
        [&](const Plan& p, const SenderPlan& g) {
            if (p.transfer_id == "blocked") {
                ++entered;
                gate.wait();
            } else
                ++other;
            return Finished(g);
        },
        2);
    auto held = std::async(std::launch::async, [&] {
        return progress.executeAndDrain(MuxPlan("blocked"));
    });
    const bool started = Until([&] { return entered == 2; });
    auto free = std::async(std::launch::async, [&] {
        return progress.executeAndDrain(MuxPlan("free", 16384));
    });
    const bool independent =
        free.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    permit.set_value();
    held.get();
    free.get();
    Check(started && independent && other == 2);

    std::promise<void> release;
    auto queued_gate = release.get_future().share();
    entered = 0;
    SourceMux serial(
        [&](const Plan&, const SenderPlan& g) {
            ++entered;
            queued_gate.wait();
            return Finished(g);
        },
        1);
    auto active = std::async(std::launch::async, [&] {
        return serial.executeAndDrain(MuxPlan("active"));
    });
    const bool running = Until([&] { return entered == 2; });
    auto cancelled = std::async(std::launch::async, [&] {
        try {
            serial.executeAndDrain(MuxPlan("cancelled", 16384));
        } catch (const MuxFailure& e) {
            return e.drained;
        }
        return false;
    });
    const bool pending = Until([&] { return serial.snapshot().size() == 2; });
    const bool not_active = serial.cancelQueued("active");
    const bool stopped = serial.cancelQueued("cancelled");
    release.set_value();
    active.get();
    Check(running && pending && !not_active && stopped && cancelled.get() &&
          entered == 2);
}
void QueuedStoreTargetRevalidation() {
    Fixture f(4096, 1);
    std::promise<void> entered, permit;
    auto gate = permit.get_future().share();
    auto started = entered.get_future();
    f.eb.on_submit = [&] {
        entered.set_value();
        gate.wait();
    };
    auto first = std::async(std::launch::async, [&] {
        return f.bb->Copy("first", f.source, {f.target});
    });
    const bool running =
        started.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    auto other = f.target;
    other.id = 23;
    other.memory.buffer_descriptor.buffer_address_ += 4 * 1024 * 1024;
    auto second = std::async(std::launch::async, [&] {
        return f.bb->Copy("second", f.source, {other});
    });
    const bool queued = Until([&] { return f.bb->MuxStatus().size() == 2; });
    f.a.authorize = false;
    permit.set_value();
    const auto a = first.get(), b = second.get();
    Check(running && queued && a.success && !b.success && b.safe_to_revoke);
    Check(f.eb.bytes == 4096 && f.eb.batches == 1);
}

void Failure() {
    {
        Fixture denied;
        denied.a.authorize = false;
        auto result =
            denied.bb->Copy("master-rejected", denied.source, {denied.target});
        Check(!result.success && result.safe_to_revoke && denied.eb.bytes == 0);
    }
    for (int fault : {0, 1, 2}) {
        Fixture f;
        f.eb.corrupt = fault == 0;
        f.eb.fail = fault == 1;
        f.eb.drop_prepare = fault == 2;
        auto result = f.bb->Copy("failure-fixture", f.source, {f.target});
        Check(!result.success && result.safe_to_revoke && !f.published);
        if (fault == 2) Check(f.eb.bytes == 0);
    }
    Fixture f;
    f.eb.delay_ms = 150;
    auto running = std::async(std::launch::async, [&] {
        return f.bb->Copy("late-writer", f.source, {f.target});
    });
    Check(running.wait_for(std::chrono::milliseconds(20)) ==
          std::future_status::timeout);
    Check(!f.published && f.eb.bytes == 0);
    auto result = running.get();
    Check(result.success && result.safe_to_revoke);
}

void UnknownSubmission() {
    // Isolate the deliberate crash from running progress threads. The parent
    // reads retained file-pool journals; no real DMA exists in this harness.
    int channel[2];
    Check(pipe(channel) == 0);
    const auto pid = fork();
    Check(pid >= 0);
    if (pid == 0) {
        close(channel[0]);
        Fixture child;
        const auto path = child.directory + "\n";
        Check(write(channel[1], path.data(), path.size()) ==
              static_cast<ssize_t>(path.size()));
        child.eb.throw_submit = true;
        const auto result =
            child.bb->Copy("unknown-submit", child.source, {child.target});
        const auto state = child.bb->MuxStatus();
        std::_Exit(!result.success && !result.safe_to_revoke &&
                           state.size() == 1 && state[0].state == "QUARANTINED"
                       ? 0
                       : 1);
    }
    close(channel[1]);
    std::string path;
    char byte;
    while (read(channel[0], &byte, 1) == 1 && byte != '\n') path += byte;
    close(channel[0]);
    int status = 0;
    Check(waitpid(pid, &status, 0) == pid);
    Check(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    for (const auto* pool : {"A", "B"}) {
        std::ifstream file(path + "/" + pool + ".journal");
        std::string data((std::istreambuf_iterator<char>(file)), {});
        Check(data.find("PENDING") != std::string::npos);
        Check(data.find("DRAINED") == std::string::npos);
    }
    std::filesystem::remove_all(path);  // Fake submit accepted no work.
}
void CheckStatus() {
    Fixture f;
    Check(f.ba->Materialize("connector-real-key-fixture"));
    std::ifstream input(f.ca.journal);
    std::string journal((std::istreambuf_iterator<char>(input)), {});
    Check(journal.find("PENDING") != std::string::npos);
    Check(journal.find("VALIDATED") != std::string::npos);
    Check(journal.find(std::string(48, 'x')) == std::string::npos);
    // Terminal journals allow a new bridge after the old registration owner
    // has stopped. Real Store additionally rejects duplicate arena allocation.
    f.ba.reset();
    f.ba = std::make_unique<CallosumStoreBridge>(f.a, f.ea, f.ca, f.pa);
}
}  // namespace
int main(int argc, char** argv) {
    try {
        Check(argc == 2);
        std::string category = argv[1];
        if (category == "preflight")
            Preflight();
        else if (category == "function")
            Function();
        else if (category == "request_lifecycle")
            RequestLifecycle();
        else if (category == "store_mux_integration")
            StoreMuxIntegration();
        else if (category == "native_mux_function")
            NativeMuxFunction();
        else if (category == "native_mux_fifo")
            NativeMuxFifo();
        else if (category == "native_mux_progress_cancel")
            NativeMuxProgressAndCancel();
        else if (category == "queued_target_revalidation")
            QueuedStoreTargetRevalidation();
        else if (category == "failure") {
            UnknownSubmission();
            Failure();
        } else if (category == "status")
            CheckStatus();
        else
            throw std::runtime_error("no tests selected");
        std::cout << "PASS T0 adapter " << category
                  << " (fake engine, file pools)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
