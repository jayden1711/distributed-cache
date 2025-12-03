#pragma once

#include <map>
#include <string>
#include <vector>
#include <functional>
#include <algorithm>
#include <memory>

// Node representation in the cluster
struct Node {
    std::string id;
    std::string address;
    uint32_t port;

    Node(std::string id_, std::string addr, uint32_t p)
            : id(std::move(id_)), address(std::move(addr)), port(p) {}

    bool operator==(const Node& other) const {
        return id == other.id;
    }
};

// Consistent hashing with virtual nodes
class ConsistentHash {
public:
    explicit ConsistentHash(size_t virtual_nodes_per_node = 150);

    // Node management
    void AddNode(const Node& node);
    void RemoveNode(const std::string& node_id);

    // Key routing
    std::vector<Node> GetNodes(const std::string& key, size_t count = 1) const;
    Node GetPrimaryNode(const std::string& key) const;

    // Cluster info
    size_t NodeCount() const { return nodes_.size(); }
    std::vector<Node> GetAllNodes() const;

private:
    uint64_t Hash(const std::string& str) const;

    // Virtual nodes per physical node
    size_t virtual_nodes_per_node_;

    // Ring: hash -> node_id
    std::map<uint64_t, std::string> ring_;

    // Node lookup: node_id -> Node
    std::map<std::string, Node> nodes_;
};

// Implementation

inline ConsistentHash::ConsistentHash(size_t virtual_nodes_per_node)
        : virtual_nodes_per_node_(virtual_nodes_per_node) {}

inline uint64_t ConsistentHash::Hash(const std::string& str) const {
    // MurmurHash3-like hash
    uint64_t hash = 0;
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;

    const char* data = str.data();
    const char* end = data + str.size();

    while (data != end) {
        uint64_t k = static_cast<uint64_t>(*data++);
        k *= m;
        k ^= k >> r;
        k *= m;

        hash ^= k;
        hash *= m;
    }

    hash ^= hash >> r;
    hash *= m;
    hash ^= hash >> r;

    return hash;
}

inline void ConsistentHash::AddNode(const Node& node) {
    nodes_[node.id] = node;

    // Add virtual nodes to the ring
    for (size_t i = 0; i < virtual_nodes_per_node_; ++i) {
        std::string vnode_key = node.id + ":" + std::to_string(i);
        uint64_t hash = Hash(vnode_key);
        ring_[hash] = node.id;
    }
}

inline void ConsistentHash::RemoveNode(const std::string& node_id) {
    // Remove from nodes map
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return;

    nodes_.erase(it);

    // Remove all virtual nodes from ring
    auto ring_it = ring_.begin();
    while (ring_it != ring_.end()) {
        if (ring_it->second == node_id) {
            ring_it = ring_.erase(ring_it);
        } else {
            ++ring_it;
        }
    }
}

inline std::vector<Node> ConsistentHash::GetNodes(const std::string& key, size_t count) const {
    if (ring_.empty()) return {};

    std::vector<Node> result;
    std::set<std::string> seen_node_ids;

    uint64_t hash = Hash(key);

    // Find the first node >= hash
    auto it = ring_.lower_bound(hash);

    // Wrap around if needed
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    auto start_it = it;

    do {
        const std::string& node_id = it->second;

        // Add unique physical nodes only
        if (seen_node_ids.find(node_id) == seen_node_ids.end()) {
            auto node_it = nodes_.find(node_id);
            if (node_it != nodes_.end()) {
                result.push_back(node_it->second);
                seen_node_ids.insert(node_id);

                if (result.size() >= count) {
                    break;
                }
            }
        }

        ++it;
        if (it == ring_.end()) {
            it = ring_.begin();
        }

    } while (it != start_it && result.size() < count);

    return result;
}

inline Node ConsistentHash::GetPrimaryNode(const std::string& key) const {
    auto nodes = GetNodes(key, 1);
    if (nodes.empty()) {
        throw std::runtime_error("No nodes available in consistent hash ring");
    }
    return nodes[0];
}

inline std::vector<Node> ConsistentHash::GetAllNodes() const {
    std::vector<Node> result;
    result.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
        result.push_back(node);
    }
    return result;
}