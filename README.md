# Distributed Cache

A high-performance distributed cache system built in C++ with gRPC for inter-node communication and Protocol Buffers for serialization.
Features
Core Architecture

Lock-Free Hash Table: Thread-safe concurrent access using atomic operations
Consistent Hashing: Virtual nodes for balanced load distribution across cluster
Configurable Replication: Support for multiple consistency levels (ONE, QUORUM, ALL)
Gossip Protocol: Automatic failure detection and cluster membership management
gRPC Streaming: Batch operations to reduce network overhead

Performance Optimizations

Cache-line alignment for reduced false sharing
Memory prefetching hints for improved cache locality
Lock-free data structures using C++ atomics
Quadratic probing for hash collision resolution
Optimized for profiling with perf

Consistency Guarantees

ONE: Write/read to one replica (fastest, eventual consistency)
QUORUM: Write/read to majority of replicas (balanced)
ALL: Write/read to all replicas (strongest consistency)
