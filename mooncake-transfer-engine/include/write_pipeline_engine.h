// Copyright 2026. SPDX-License-Identifier: Apache-2.0
#pragma once
#include <transfer_engine.h>
#include "write_pipeline.h"

namespace mooncake::write_pipeline {
class EngineOperation final : public Operation {
 public:
    explicit EngineOperation(TransferEngine::ScatterTransferOperation operation)
        : operation_(std::move(operation)) {}
    bool poll(Completion& completion) override {
        Status status;
        if (!operation_.tryComplete(status)) return false;
        completion.success = status.ok();
        return true;
    }
 private:
    // Destruction drains on exceptional exit; never drop a live engine handle.
    TransferEngine::ScatterTransferOperation operation_;
};

inline Result run(TransferEngine& engine, const std::vector<Request>& requests,
                  Options options, const std::function<bool()>& cancelled = {},
                  const std::function<void()>& ready = {}) {
    return run(requests,options,[&](const std::vector<const Request*>& batch) {
        const size_t zero = 0;
        std::vector<TransferEngine::ScatterTransferRange> ranges;
        std::vector<size_t> lengths;
        std::vector<uint64_t> ids;
        ranges.reserve(batch.size());
        lengths.reserve(batch.size());
        ids.reserve(batch.size());
        for (const auto* r : batch) {
            lengths.push_back(static_cast<size_t>(r->bytes));
            ids.push_back(r->id+1);
            ranges.push_back({TransferRequest::WRITE,r->segment,r->destination,
                static_cast<size_t>(r->bytes),reinterpret_cast<void*>(r->source),
                static_cast<size_t>(r->bytes),std::span<const size_t>(&zero,1),
                std::span<const size_t>(&zero,1),std::span<const size_t>(&lengths.back(),1),{}});
        }
        return std::make_unique<EngineOperation>(engine.submitScatterWithIds(ranges,ids));
    },cancelled,ready);
}
}  // namespace mooncake::write_pipeline
