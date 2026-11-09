// Callosum's distributed adapters invoke the same coordinator as the T0 tests.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "pool_relay.h"
#include "relay_batch.h"
#include "delivery_policy.h"
#include "cxl_pool_identity.h"
#include "scatter_layout.h"

namespace py = pybind11;
using namespace mooncake::pool_relay;

namespace {
Extent extent(const py::dict& d) {
    return {d["pool"].cast<std::string>(), d["offset"].cast<uint64_t>(),
            d["length"].cast<uint64_t>()};
}
py::dict pack(const Extent& e) {
    py::dict d;
    d["pool"] = e.pool;
    d["offset"] = e.offset;
    d["length"] = e.length;
    return d;
}
// Remote resources are retained by the durable control session, not a Python
// object's destructor. Failures quarantine them; only the service releases
// them.
std::shared_ptr<void> retained() { return std::make_shared<int>(1); }
struct Allocation : Reservation {
    py::object callbacks;
    Extent location;
    Allocation(py::object cb, Extent e)
        : callbacks(std::move(cb)), location(std::move(e)) {}
    Extent extent() const override { return location; }
    void commit() override { callbacks.attr("commit")(); }
    // No remote release here: the control session owns quarantine/commit.
};
struct Adapter : Authority, Receivers, Writers {
    py::object cb;
    explicit Adapter(py::object c) : cb(std::move(c)) {}
    Source lookupAndPin(const std::string& key,
                        const std::string& pool) override {
        auto d = cb.attr("lookup_and_pin")(key, pool).cast<py::dict>();
        return {extent(d), d["version"].cast<std::string>(),
                d["checksum"].cast<std::string>(), retained()};
    }
    std::unique_ptr<Reservation> reserve(const std::string& key,
                                         const std::string& pool,
                                         uint64_t length) override {
        return std::make_unique<Allocation>(
            cb, extent(cb.attr("reserve")(key, pool, length).cast<py::dict>()));
    }
    bool verify(const Source&, const Reservation&) override {
        return cb.attr("verify")().cast<bool>();
    }
    ReceiverTarget prepare(const std::string& id, const Member& member,
                           const Extent& assigned) override {
        auto d =
            cb.attr("prepare")(id, member.id, pack(assigned)).cast<py::dict>();
        return {d["receiver"].cast<std::string>(),
                extent(d["extent"].cast<py::dict>()),
                d["segment"].cast<std::string>(),
                d["remote_base"].cast<uint64_t>(),
                d["epoch"].cast<std::string>(),
                retained(), d["address_kind"].cast<std::string>(),
                d["plan_id"].cast<std::string>()};
    }
    std::vector<Completion> executeAndDrain(const Plan& plan) override {
        py::list targets, senders;
        for (const auto& t : plan.targets) {
            py::dict d;
            d["receiver"] = t.receiver;
            d["extent"] = pack(t.extent);
            d["segment"] = t.segment;
            d["remote_base"] = t.remote_base;
            d["epoch"] = t.epoch;
            d["address_kind"] = t.address_kind;
            d["plan_id"] = t.plan_id;
            targets.append(d);
        }
        for (const auto& s : plan.senders) {
            py::list writes;
            for (const auto& w : s.writes) {
                py::dict d;
                d["chunk"] = w.chunk;
                d["source"] = pack(w.source);
                d["destination"] = pack(w.destination);
                d["target_index"] = w.target_index;
                d["remote_address"] = w.remote_address;
                writes.append(d);
            }
            py::dict d;
            d["sender"] = s.sender;
            d["writes"] = writes;
            senders.append(d);
        }
        py::dict p;
        p["transfer_id"] = plan.transfer_id;
        p["key"] = plan.key;
        p["worker"] = plan.worker;
        p["requester"] = plan.requester;
        p["source_version"] = plan.source_version;
        p["targets"] = targets;
        p["senders"] = senders;
        std::vector<Completion> result;
        for (auto item : cb.attr("execute")(p)) {
            auto d = py::cast<py::dict>(item);
            result.push_back({d["chunk"].cast<uint64_t>(),
                              d["bytes"].cast<uint64_t>(),
                              d["success"].cast<bool>()});
        }
        return result;
    }
};
}  // namespace

PYBIND11_MODULE(callosum_relay_native, m) {
    m.attr("ABI_VERSION") = 2;
    m.def("same_cxl_mapping", &mooncake::SameCxlMapping);
    m.def("validate_write_batch", &ValidateWriteBatch);
    py::class_<ReadinessLedger>(m, "ReadinessLedger")
        .def(py::init<std::string, std::string, uint64_t>())
        .def("assign", &ReadinessLedger::assign)
        .def("event", &ReadinessLedger::event)
        .def("ready", &ReadinessLedger::ready);
    m.def("project_scatter", [](const std::vector<std::vector<uint64_t>>& src,
                                const std::vector<std::vector<uint64_t>>& dst,
                                uint64_t bytes, uint64_t start, uint64_t length,
                                uint64_t max_fragment) {
        auto unpack = [](const auto& rows) {
            std::vector<TensorExtent> result;
            for (const auto& r : rows) {
                if (r.size() != 3)
                    throw std::invalid_argument(
                        "extent requires logical, VA, length");
                result.push_back({r[0], r[1], r[2]});
            }
            return result;
        };
        std::vector<std::vector<uint64_t>> result;
        for (const auto& f : ProjectScatter(unpack(src), unpack(dst), bytes,
                                            start, length, max_fragment))
            result.push_back({f.logical, f.source, f.destination, f.length});
        return result;
    });
    m.def("select_delivery", [](bool local, bool source, bool eligible, bool forced,
                               double transfer_us, double recompute_us,
                               double margin_us, double margin_fraction,
                               double uncertainty_us) {
        auto decision = SelectDelivery(local, source, eligible, forced, transfer_us,
                                       recompute_us, margin_us, margin_fraction,
                                       uncertainty_us);
        py::dict result;
        result["action"] = decision.action;
        result["reason"] = decision.reason;
        return result;
    });
    m.def("run", [](py::list members, py::dict request, py::object callbacks) {
        Registry registry;
        for (auto item : members) {
            auto d = py::cast<py::dict>(item);
            registry.add(
                {d["id"].cast<std::string>(), d["pool"].cast<std::string>(),
                 d["capacity"].cast<uint64_t>(), d["can_send"].cast<bool>(),
                 d["can_receive"].cast<bool>()});
        }
        Request r{request["transfer_id"].cast<std::string>(),
                  request["key"].cast<std::string>(),
                  request["source_pool"].cast<std::string>(),
                  request["requester"].cast<std::string>(),
                  request["worker"].cast<std::string>(),
                  request["chunk_bytes"].cast<uint64_t>()};
        Adapter adapter(callbacks);
        Coordinator coordinator(registry, adapter, adapter, adapter,
                                [&](const Event& e) {
                                    py::dict d;
                                    d["transfer_id"] = e.transfer_id;
                                    d["worker"] = e.worker;
                                    d["stage"] = static_cast<int>(e.stage);
                                    d["bytes"] = e.bytes;
                                    d["monotonic_ns"] = e.monotonic_ns;
                                    callbacks.attr("event")(d);
                                });
        auto result = coordinator.run(r);
        auto d = pack(result.destination);
        d["chunks"] = result.chunks;
        d["senders"] = result.senders;
        d["receivers"] = result.receivers;
        return d;
    });
}
