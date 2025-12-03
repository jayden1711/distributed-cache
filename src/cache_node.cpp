#include "cache_node.h"
#include <iostream>
#include <chrono>

// CacheNode Implementation

CacheNode::CacheNode(std::string node_id, std::string address, uint32_t port,
                     size_t replication_factor)
        : node_info_(std::move(node_id), std::move(address), port),
          replication_factor_(replication_factor),
          default_consistency_(cache::ConsistencyLevel::QUORUM),
          hashtable_(1024 * 1024) {

    gossip_ = std::make_unique<GossipProtocol>(this);
    replication_ = std::make_unique<ReplicationManager>(this, replication_factor_);
}

CacheNode::~CacheNode() {
    Stop();
}

void CacheNode::Start() {
    if (running_.exchange(true)) return;

    // Add self to consistent hash
    consistent_hash_.AddNode(node_info_);

    // Start gossip protocol
    gossip_->Start();

    // Start replication manager
    replication_->Start();

    // Build gRPC server
    std::string server_address = node_info_.address + ":" + std::to_string(node_info_.port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    auto cache_service = std::make_unique<CacheServiceImpl>(this);
    auto gossip_service = std::make_unique<GossipServiceImpl>(gossip_.get());
    auto repl_service = std::make_unique<ReplicationServiceImpl>(this);

    builder.RegisterService(cache_service.get());
    builder.RegisterService(gossip_service.get());
    builder.RegisterService(repl_service.get());

    server_ = builder.BuildAndStart();
    std::cout << "Cache node " << node_info_.id << " listening on " << server_address << std::endl;
}

void CacheNode::Stop() {
    if (!running_.exchange(false)) return;

    gossip_->Stop();
    replication_->Stop();

    if (server_) {
        server_->Shutdown();
        server_->Wait();
    }
}

void CacheNode::Join(const std::string& seed_address, uint32_t seed_port) {
    // Contact seed node to join cluster
    std::string target = seed_address + ":" + std::to_string(seed_port);

    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = cache::GossipService::NewStub(channel);

    cache::NodeInfo node_info;
    node_info.set_node_id(node_info_.id);
    node_info.set_address(node_info_.address);
    node_info.set_port(node_info_.port);
    node_info.set_status(cache::NodeStatus::ALIVE);

    cache::AckResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub->NotifyNodeJoin(&context, node_info, &response);

    if (status.ok()) {
        std::cout << "Successfully joined cluster via " << target << std::endl;
    } else {
        std::cerr << "Failed to join cluster: " << status.error_message() << std::endl;
    }
}

// CacheServiceImpl Implementation

grpc::Status CacheServiceImpl::Get(grpc::ServerContext* context,
                                   const cache::GetRequest* request,
                                   cache::GetResponse* response) {
    auto consistency = request->consistency();
    if (consistency == cache::ConsistencyLevel::ONE) {
        // Local read
        auto value = node_->GetHashTable().Get(request->key());
        if (value) {
            response->set_found(true);
            response->set_value(value->data(), value->size());
        } else {
            response->set_found(false);
        }
    } else {
        // Replicated read
        auto value = node_->replication_->ReplicatedGet(request->key(), consistency);
        if (value) {
            response->set_found(true);
            response->set_value(value->data(), value->size());
        } else {
            response->set_found(false);
        }
    }

    return grpc::Status::OK;
}

grpc::Status CacheServiceImpl::Put(grpc::ServerContext* context,
                                   const cache::PutRequest* request,
                                   cache::PutResponse* response) {
    std::vector<uint8_t> value(request->value().begin(), request->value().end());

    auto consistency = request->consistency();
    if (consistency == cache::ConsistencyLevel::ONE) {
        // Local write
        bool success = node_->GetHashTable().Put(request->key(), value, request->ttl_seconds());
        response->set_success(success);
    } else {
        // Replicated write
        bool success = node_->replication_->ReplicatePut(
                request->key(), value, request->ttl_seconds(), consistency);
        response->set_success(success);
    }

    return grpc::Status::OK;
}

grpc::Status CacheServiceImpl::Delete(grpc::ServerContext* context,
                                      const cache::DeleteRequest* request,
                                      cache::DeleteResponse* response) {
    auto consistency = request->consistency();
    if (consistency == cache::ConsistencyLevel::ONE) {
        bool success = node_->GetHashTable().Delete(request->key());
        response->set_success(success);
    } else {
        bool success = node_->replication_->ReplicatedDelete(request->key(), consistency);
        response->set_success(success);
    }

    return grpc::Status::OK;
}

grpc::Status CacheServiceImpl::BatchPut(grpc::ServerContext* context,
                                        grpc::ServerReader<cache::PutRequest>* reader,
                                        cache::BatchResponse* response) {
    cache::PutRequest request;
    uint32_t success = 0, failure = 0;

    while (reader->Read(&request)) {
        std::vector<uint8_t> value(request.value().begin(), request.value().end());
        bool ok = node_->GetHashTable().Put(request.key(), value, request.ttl_seconds());
        if (ok) ++success;
        else ++failure;
    }

    response->set_success_count(success);
    response->set_failure_count(failure);
    return grpc::Status::OK;
}

grpc::Status CacheServiceImpl::BatchGet(grpc::ServerContext* context,
                                        grpc::ServerReader<cache::GetRequest>* reader,
                                        grpc::ServerWriter<cache::GetResponse>* writer) {
    cache::GetRequest request;

    while (reader->Read(&request)) {
        cache::GetResponse response;
        auto value = node_->GetHashTable().Get(request.key());

        if (value) {
            response.set_found(true);
            response.set_value(value->data(), value->size());
        } else {
            response.set_found(false);
        }

        writer->Write(response);
    }

    return grpc::Status::OK;
}

// GossipProtocol Implementation

GossipProtocol::GossipProtocol(CacheNode* node) : node_(node) {}

GossipProtocol::~GossipProtocol() {
    Stop();
}

void GossipProtocol::Start() {
    if (running_.exchange(true)) return;
    gossip_thread_ = std::thread(&GossipProtocol::GossipLoop, this);
}

void GossipProtocol::Stop() {
    if (!running_.exchange(false)) return;
    if (gossip_thread_.joinable()) {
        gossip_thread_.join();
    }
}

void GossipProtocol::GossipLoop() {
    while (running_) {
        CheckFailures();

        auto nodes = GetAliveNodes();
        for (const auto& node : nodes) {
            if (node.id != node_->GetNodeInfo().id) {
                SendHeartbeat(node);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(GOSSIP_INTERVAL_MS));
    }
}

void GossipProtocol::SendHeartbeat(const Node& target) {
    std::string address = target.address + ":" + std::to_string(target.port);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = cache::GossipService::NewStub(channel);

    cache::HeartbeatRequest request;
    auto* node_info = request.mutable_node();
    node_info->set_node_id(node_->GetNodeInfo().id);
    node_info->set_address(node_->GetNodeInfo().address);
    node_info->set_port(node_->GetNodeInfo().port);

    request.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    cache::HeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));

    grpc::Status status = stub->Heartbeat(&context, request, &response);

    if (status.ok()) {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        last_heartbeat_[target.id] = request.timestamp();
    }
}

void GossipProtocol::CheckFailures() {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    std::lock_guard<std::mutex> lock(nodes_mutex_);
    std::vector<std::string> failed_nodes;

    for (const auto& [node_id, last_seen] : last_heartbeat_) {
        if (now - last_seen > FAILURE_TIMEOUT_MS) {
            failed_nodes.push_back(node_id);
        }
    }

    for (const auto& node_id : failed_nodes) {
        std::cout << "Detected failure of node: " << node_id << std::endl;
        node_->GetConsistentHash().RemoveNode(node_id);
        known_nodes_.erase(node_id);
        last_heartbeat_.erase(node_id);
    }
}

void GossipProtocol::AddNode(const Node& node) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    known_nodes_[node.id] = node;
    last_heartbeat_[node.id] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    node_->GetConsistentHash().AddNode(node);
}

void GossipProtocol::RemoveNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    known_nodes_.erase(node_id);
    last_heartbeat_.erase(node_id);
    node_->GetConsistentHash().RemoveNode(node_id);
}

std::vector<Node> GossipProtocol::GetAliveNodes() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    std::vector<Node> result;
    for (const auto& [id, node] : known_nodes_) {
        result.push_back(node);
    }
    return result;
}

// GossipServiceImpl Implementation

grpc::Status GossipServiceImpl::Heartbeat(grpc::ServerContext* context,
                                          const cache::HeartbeatRequest* request,
                                          cache::HeartbeatResponse* response) {
    const auto& node_info = request->node();
    Node node(node_info.node_id(), node_info.address(), node_info.port());

    gossip_->AddNode(node);
    response->set_ack(true);

    return grpc::Status::OK;
}

grpc::Status GossipServiceImpl::GetClusterState(grpc::ServerContext* context,
                                                const cache::ClusterStateRequest* request,
                                                cache::ClusterStateResponse* response) {
    auto nodes = gossip_->GetAliveNodes();
    for (const auto& node : nodes) {
        auto* node_info = response->add_nodes();
        node_info->set_node_id(node.id);
        node_info->set_address(node.address);
        node_info->set_port(node.port);
        node_info->set_status(cache::NodeStatus::ALIVE);
    }

    return grpc::Status::OK;
}

grpc::Status GossipServiceImpl::NotifyNodeJoin(grpc::ServerContext* context,
                                               const cache::NodeInfo* request,
                                               cache::AckResponse* response) {
    Node node(request->node_id(), request->address(), request->port());
    gossip_->AddNode(node);
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status GossipServiceImpl::NotifyNodeFailure(grpc::ServerContext* context,
                                                  const cache::NodeInfo* request,
                                                  cache::AckResponse* response) {
    gossip_->RemoveNode(request->node_id());
    response->set_success(true);
    return grpc::Status::OK;
}

// ReplicationManager Implementation

ReplicationManager::ReplicationManager(CacheNode* node, size_t replication_factor)
        : node_(node), replication_factor_(replication_factor) {}

ReplicationManager::~ReplicationManager() {
    Stop();
}

void ReplicationManager::Start() {
    running_ = true;
}

void ReplicationManager::Stop() {
    running_ = false;
}

size_t ReplicationManager::GetRequiredAcks(cache::ConsistencyLevel consistency) const {
    switch (consistency) {
        case cache::ConsistencyLevel::ONE:
            return 1;
        case cache::ConsistencyLevel::QUORUM:
            return (replication_factor_ / 2) + 1;
        case cache::ConsistencyLevel::ALL:
            return replication_factor_;
        default:
            return 1;
    }
}

std::vector<Node> ReplicationManager::GetReplicaNodes(const std::string& key) const {
    return node_->GetConsistentHash().GetNodes(key, replication_factor_);
}

bool ReplicationManager::ReplicatePut(const std::string& key,
                                      const std::vector<uint8_t>& value,
                                      uint32_t ttl,
                                      cache::ConsistencyLevel consistency) {
    auto replicas = GetReplicaNodes(key);
    size_t required_acks = GetRequiredAcks(consistency);
    std::atomic<size_t> acks{0};

    for (const auto& replica : replicas) {
        std::string address = replica.address + ":" + std::to_string(replica.port);
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        auto stub = cache::ReplicationService::NewStub(channel);

        cache::ReplicationRequest request;
        request.set_key(key);
        request.set_value(value.data(), value.size());
        request.set_ttl_seconds(ttl);

        cache::ReplicationResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

        grpc::Status status = stub->Replicate(&context, request, &response);
        if (status.ok() && response.success()) {
            acks.fetch_add(1);
        }
    }

    return acks >= required_acks;
}

std::optional<std::vector<uint8_t>> ReplicationManager::ReplicatedGet(
        const std::string& key,
        cache::ConsistencyLevel consistency) {

    auto replicas = GetReplicaNodes(key);
    size_t required_acks = GetRequiredAcks(consistency);

    for (const auto& replica : replicas) {
        if (replica.id == node_->GetNodeInfo().id) {
            return node_->GetHashTable().Get(key);
        }
    }

    return std::nullopt;
}

bool ReplicationManager::ReplicatedDelete(const std::string& key,
                                          cache::ConsistencyLevel consistency) {
    auto replicas = GetReplicaNodes(key);
    size_t required_acks = GetRequiredAcks(consistency);
    std::atomic<size_t> acks{0};

    for (const auto& replica : replicas) {
        if (replica.id == node_->GetNodeInfo().id) {
            if (node_->GetHashTable().Delete(key)) {
                acks.fetch_add(1);
            }
        }
    }

    return acks >= required_acks;
}

// ReplicationServiceImpl Implementation

grpc::Status ReplicationServiceImpl::Replicate(grpc::ServerContext* context,
                                               const cache::ReplicationRequest* request,
                                               cache::ReplicationResponse* response) {
    std::vector<uint8_t> value(request->value().begin(), request->value().end());
    bool success = node_->GetHashTable().Put(request->key(), value, request->ttl_seconds());
    response->set_success(success);
    return grpc::Status::OK;
}

grpc::Status ReplicationServiceImpl::SyncData(grpc::ServerContext* context,
                                              const cache::SyncRequest* request,
                                              grpc::ServerWriter<cache::SyncResponse>* writer) {
    // Sync implementation would iterate over hashtable and stream entries
    // Omitted for brevity
    return grpc::Status::OK;
}
