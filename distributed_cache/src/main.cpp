#include "cache_node.h"
#include <iostream>
#include <csignal>
#include <memory>

std::unique_ptr<CacheNode> g_cache_node;

void SignalHandler(int signal) {
    std::cout << "\nShutting down cache node..." << std::endl;
    if (g_cache_node) {
        g_cache_node->Stop();
    }
    exit(0);
}

int main(int argc, char** argv) {
    // Parse command line arguments
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <address> <port> [seed_address] [seed_port]" << std::endl;
        std::cerr << "Example: " << argv[0] << " node1 127.0.0.1 50051" << std::endl;
        std::cerr << "         " << argv[0] << " node2 127.0.0.1 50052 127.0.0.1 50051" << std::endl;
        return 1;
    }

    std::string node_id = argv[1];
    std::string address = argv[2];
    uint32_t port = std::stoi(argv[3]);

    std::string seed_address;
    uint32_t seed_port = 0;
    bool join_cluster = false;

    if (argc >= 6) {
        seed_address = argv[4];
        seed_port = std::stoi(argv[5]);
        join_cluster = true;
    }

    // Setup signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    std::cout << "Starting distributed cache node..." << std::endl;
    std::cout << "Node ID: " << node_id << std::endl;
    std::cout << "Address: " << address << ":" << port << std::endl;

    // Create and start cache node
    g_cache_node = std::make_unique<CacheNode>(node_id, address, port);

    // Configure replication and consistency
    g_cache_node->SetReplicationFactor(3);
    g_cache_node->SetConsistencyLevel(cache::ConsistencyLevel::QUORUM);

    // Start the node
    g_cache_node->Start();

    // Join existing cluster if seed node provided
    if (join_cluster) {
        std::cout << "Joining cluster via seed node " << seed_address << ":" << seed_port << std::endl;
        g_cache_node->Join(seed_address, seed_port);
    } else {
        std::cout << "Starting as seed node" << std::endl;
    }

    std::cout << "\nCache node is running. Press Ctrl+C to stop." << std::endl;
    std::cout << "============================================" << std::endl;

    // Keep the server running
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Print statistics periodically
        static int counter = 0;
        if (++counter % 30 == 0) {
            auto& ht = g_cache_node->GetHashTable();
            std::cout << "Cache stats - Size: " << ht.Size()
                      << ", Load factor: " << ht.LoadFactor()
                      << ", Cluster nodes: " << g_cache_node->GetConsistentHash().NodeCount()
                      << std::endl;
        }
    }

    return 0;
}
