// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_FAKETRACT_CXL_POOL_BACKEND_H_
#define MOONCAKE_FAKETRACT_CXL_POOL_BACKEND_H_

#include <cxl_pool_backend.h>

namespace mooncake::faketract {

// Implementation types are intentionally opaque. Callers depend on
// CxlPoolBackend/CxlAllocation so the fake model can be replaced by a legacy
// TraCT adapter without changing Mooncake Transfer Engine.
class MmapCxlAllocation;
class MmapCxlPoolBackend;

// Opens the dependency-free FakeTraCT provider. It models the two public
// TraCT-facing operations, metadata_lookup() and alloc(), over a regular file
// or a device-DAX mapping. It is a test/development model, not the production
// allocator for a live TraCT-managed CXL pool.
std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackend(
    CxlPoolConfig config, CxlPoolError* error = nullptr);

std::shared_ptr<CxlPoolBackend> OpenMmapCxlPoolBackendFromEnvironment(
    CxlPoolError* error = nullptr);

}  // namespace mooncake::faketract

#endif  // MOONCAKE_FAKETRACT_CXL_POOL_BACKEND_H_
