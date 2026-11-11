#pragma once
#include <functional>
#include <atomic>
#include <optional>
#include <string>
#include <vector>
#include "replica.h"
#include "utils.h"
namespace mooncake {
struct QueryResult { std::vector<Replica::Descriptor> replicas; };
enum class TaskStatus { PENDING, SUCCESS, FAILED };
struct QueryTaskResponse { TaskStatus status = TaskStatus::SUCCESS; };
class Client {
 public:
    explicit Client(std::string name) : name(std::move(name)) {}
    const std::string& GetSegmentEndpoint() const { return name; }
    const std::string& GetSegmentName() const { return name; }
    std::string tenant_id() const { return "default"; }
    bool ValidateCallosumTarget(const std::string&, uint64_t, uint64_t, uint64_t, uint64_t) { return authorize; }
    std::optional<QueryResult> Query(const std::string& key) { return query(key); }
    std::optional<UUID> CreateCopyTask(const std::string& key, const std::vector<std::string>& targets) {
        last_key = key; last_targets = targets;
        ++tasks; return copy(key) ? std::optional<UUID>(1) : std::nullopt;
    }
    std::optional<QueryTaskResponse> QueryTask(UUID) {
        return query_task ? query_task() : std::optional<QueryTaskResponse>(QueryTaskResponse{});
    }
    std::function<std::optional<QueryResult>(const std::string&)> query;
    std::function<bool(const std::string&)> copy;
    std::function<std::optional<QueryTaskResponse>()> query_task;
    std::string last_key;
    std::vector<std::string> last_targets;
    std::string name;
    int tasks = 0;
    std::atomic<bool> authorize{true};
};
}
