#pragma once

#include <algorithm>
#include <cmath>
#include <initializer_list>
#include <stdexcept>
#include <string>

namespace mooncake::pool_relay {
struct DeliveryDecision {
    std::string action;
    std::string reason;
};

// Pure M1 fixture policy. Missing estimates are -1, not zero or infinity.
// Eligibility must already include fresh authority/participant validation.
inline DeliveryDecision SelectDelivery(bool local, bool source, bool eligible,
                                      bool forced, double transfer_us,
                                      double recompute_us, double margin_us,
                                      double margin_fraction,
                                      double uncertainty_us) {
    for (double value : {transfer_us, recompute_us, margin_us,
                         margin_fraction, uncertainty_us})
        if (!std::isfinite(value))
            throw std::invalid_argument("non-finite delivery estimate");
    if (margin_us < 0 || margin_fraction < 0 || uncertainty_us < 0 ||
        (transfer_us < 0 && transfer_us != -1) ||
        (recompute_us < 0 && recompute_us != -1))
        throw std::invalid_argument("invalid delivery policy inputs");
    if (local) return {"LOCAL", "committed_local_object"};
    if (!source) return {"UNAVAILABLE", "no_authoritative_source"};
    if (!eligible) return {"UNAVAILABLE", "no_eligible_route"};
    if (forced) return {"TRANSFER", "forced_single_route_fixture"};
    if (transfer_us < 0 || recompute_us < 0)
        return {"UNAVAILABLE", "unknown_cost"};
    const auto margin = std::max(margin_us, margin_fraction * recompute_us);
    if (transfer_us + margin + uncertainty_us < recompute_us)
        return {"TRANSFER", "estimated_gpu_ready_cost"};
    return {"RECOMPUTE", "transfer_not_profitable"};
}
}  // namespace mooncake::pool_relay
