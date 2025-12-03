#pragma once

#include <grpcpp/grpcpp.h>
#include "cache.grpc.pb.h"
#include "lockfree_hashtable.h"
#include "consistent_hash.h"
#include <thread>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <mutex>

// Forward declarations
class GossipProtocol;
class ReplicationManager;

// Main cache node that handles requests
class CacheNode {
public:
    CacheNode(std::string node_id, std::string address, uint32_t port,
              size_t replication_factor = 3);
    ~CacheNode();

    // Lifecycle
    void Start();
    void Stop();
    void Join(const std::string& seed_address, uint32_t seed_port);

    // Configuration
    void SetReplicationFactor(size_t factor) { replication_factor_ = factor; }
    void SetConsistencyLevel(cache::ConsistencyLevel level) { default_consistency_ = level; }

    // Access to components
    LockFreeHashTable& GetHashTable() { return hashtable_; }
    ConsistentHash& GetConsistentHash() { return consistent_hash_; }
    const Node& GetNodeInfo() const { return node_info_; }

private:
    // Node information
    Node node_info_;
    size_t replication_factor_;
    cache::ConsistencyLevel default_consistency_;

    // Core components
    LockFreeHashTable hashtable_;
    ConsistentHash consistent_hash_;
    std::unique_ptr<GossipProtocol> gossip_;
    std::unique_ptr<ReplicationManager> replication_;

    // gRPC server
    std::unique_ptr<grpc::Server> server_;
    std::atomic<bool> running_{false};
};

// gRPC service implementation
class CacheServiceImpl final : public cache::CacheService::Service {
public:
    explicit CacheServiceImpl(CacheNode* node) : node_(node) {}

    grpc::Status Get(grpc::ServerContext* context,
                     const cache::GetRequest* request,
                     cache::GetResponse* response) override;

    grpc::Status Put(grpc::ServerContext* context,
                     const cache::PutRequest* request,
                     cache::PutResponse* response) override;

    grpc::Status Delete(grpc::ServerContext* context,
                        const cache::DeleteRequest* request,
                        cache::DeleteResponse* response) override;

    grpc::Status BatchPut(grpc::ServerContext* context,
                          grpc::ServerReader<cache::PutRequest>* reader,
                          cache::BatchResponse* response) override;

    grpc::Status BatchGet(grpc::ServerContext* context,
                          grpc::ServerReader<cache::GetRequest>* reader,
                          grpc::ServerWriter<cache::GetResponse>* writer) override;

private:
    CacheNode* node_;
};

// Gossip protocol for failure detection
class GossipProtocol {
public:
    explicit GossipProtocol(CacheNode* node);
    ~GossipProtocol();

    void Start();
    void Stop();
    void AddNode(const Node& node);
    void RemoveNode(const std::string& node_id);
    std::vector<Node> GetAliveNodes() const;

private:
    void GossipLoop();
    void SendHeartbeat(const Node& target);
    void CheckFailures();

    CacheNode* node_;
    std::atomic<bool> running_{false};
    std::thread gossip_thread_;

    // Node states
    mutable std::mutex nodes_mutex_;
    std::unordered_map<std::string, uint64_t> last_heartbeat_;
    std::unordered_map<std::string, Node> known_nodes_;

    // Timing configuration
    static constexpr uint64_t GOSSIP_INTERVAL_MS = 1000;
    static constexpr uint64_t FAILURE_TIMEOUT_MS = 5000;
};

class GossipServiceImpl final : public cache::GossipService::Service {
public:
    explicit GossipServiceImpl(GossipProtocol* gossip) : gossip_(gossip) {}

    grpc::Status Heartbeat(grpc::ServerContext* context,
                           const cache::HeartbeatRequest* request,
                           cache::HeartbeatResponse* response) override;

    grpc::Status GetClusterState(grpc::ServerContext* context,
                                 const cache::ClusterStateRequest* request,
                                 cache::ClusterStateResponse* response) override;

    grpc::Status NotifyNodeJoin(grpc::ServerContext* context,
                                const cache::NodeInfo* request,
                                cache::AckResponse* response) override;

    grpc::Status NotifyNodeFailure(grpc::ServerContext* context,
                                   const cache::NodeInfo* request,
                                   cache::AckResponse* response) override;

private:
    GossipProtocol* gossip_;
};

// Replication manager
class ReplicationManager {
public:
    ReplicationManager(CacheNode* node, size_t replication_factor);
    ~ReplicationManager();

    void Start();
    void Stop();

    // Replication operations
    bool ReplicatePut(const std::string& key, const std::vector<uint8_t>& value,
                      uint32_t ttl, cache::ConsistencyLevel consistency);

    std::optional<std::vector<uint8_t>> ReplicatedGet(const std::string& key,
                                                      cache::ConsistencyLevel consistency);

    bool ReplicatedDelete(const std::string& key, cache::ConsistencyLevel consistency);

private:
    size_t GetRequiredAcks(cache::ConsistencyLevel consistency) const;
    std::vector<Node> GetReplicaNodes(const std::string& key) const;

    CacheNode* node_;
    size_t replication_factor_;
    std::atomic<bool> running_{false};
};

class ReplicationServiceImpl final : public cache::ReplicationService::Service {
public:
    explicit ReplicationServiceImpl(CacheNode* node) : node_(node) {}

    grpc::Status Replicate(grpc::ServerContext* context,
                           const cache::ReplicationRequest* request,
                           cache::ReplicationResponse* response) override;

    grpc::Status SyncData(grpc::ServerContext* context,
                          const cache::SyncRequest* request,
                          grpc::ServerWriter<cache::SyncResponse>* writer) override;

private:
    CacheNode* node_;
};