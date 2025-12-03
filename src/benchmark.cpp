#include <grpcpp/grpcpp.h>
#include "cache.grpc.pb.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <random>
#include <atomic>

class Benchmark {
public:
    Benchmark(const std::string& address) {
        channel_ = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        stub_ = cache::CacheService::NewStub(channel_);
    }

    void RunPutBenchmark(size_t num_operations, size_t num_threads) {
        std::cout << "\n=== PUT Benchmark ===" << std::endl;
        std::cout << "Operations: " << num_operations << std::endl;
        std::cout << "Threads: " << num_threads << std::endl;

        std::atomic<size_t> completed{0};
        std::atomic<size_t> failed{0};

        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        size_t ops_per_thread = num_operations / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t, ops_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(1, 1000000);

                for (size_t i = 0; i < ops_per_thread; ++i) {
                    std::string key = "key:" + std::to_string(t) + ":" + std::to_string(i);
                    std::string value = "value_" + std::to_string(dis(gen));

                    cache::PutRequest request;
                    request.set_key(key);
                    request.set_value(value);
                    request.set_consistency(cache::ConsistencyLevel::ONE);

                    cache::PutResponse response;
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                    grpc::Status status = stub_->Put(&context, request, &response);

                    if (status.ok() && response.success()) {
                        completed.fetch_add(1);
                    } else {
                        failed.fetch_add(1);
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        double ops_per_sec = (completed * 1000.0) / duration.count();
        double avg_latency = duration.count() / static_cast<double>(completed);

        std::cout << "\nResults:" << std::endl;
        std::cout << "  Completed: " << completed << std::endl;
        std::cout << "  Failed: " << failed << std::endl;
        std::cout << "  Duration: " << duration.count() << " ms" << std::endl;
        std::cout << "  Throughput: " << static_cast<int>(ops_per_sec) << " ops/sec" << std::endl;
        std::cout << "  Avg Latency: " << avg_latency << " ms" << std::endl;
    }

    void RunGetBenchmark(size_t num_operations, size_t num_threads) {
        std::cout << "\n=== GET Benchmark ===" << std::endl;
        std::cout << "Operations: " << num_operations << std::endl;
        std::cout << "Threads: " << num_threads << std::endl;

        // Pre-populate with data
        std::cout << "Pre-populating cache..." << std::endl;
        for (size_t i = 0; i < 10000; ++i) {
            cache::PutRequest request;
            request.set_key("bench:" + std::to_string(i));
            request.set_value("value_" + std::to_string(i));

            cache::PutResponse response;
            grpc::ClientContext context;
            stub_->Put(&context, request, &response);
        }

        std::atomic<size_t> completed{0};
        std::atomic<size_t> found{0};

        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        size_t ops_per_thread = num_operations / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, ops_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, 9999);

                for (size_t i = 0; i < ops_per_thread; ++i) {
                    std::string key = "bench:" + std::to_string(dis(gen));

                    cache::GetRequest request;
                    request.set_key(key);
                    request.set_consistency(cache::ConsistencyLevel::ONE);

                    cache::GetResponse response;
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                    grpc::Status status = stub_->Get(&context, request, &response);

                    if (status.ok()) {
                        completed.fetch_add(1);
                        if (response.found()) {
                            found.fetch_add(1);
                        }
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        double ops_per_sec = (completed * 1000.0) / duration.count();
        double avg_latency = duration.count() / static_cast<double>(completed);
        double hit_rate = (found * 100.0) / completed;

        std::cout << "\nResults:" << std::endl;
        std::cout << "  Completed: " << completed << std::endl;
        std::cout << "  Found: " << found << std::endl;
        std::cout << "  Hit Rate: " << hit_rate << "%" << std::endl;
        std::cout << "  Duration: " << duration.count() << " ms" << std::endl;
        std::cout << "  Throughput: " << static_cast<int>(ops_per_sec) << " ops/sec" << std::endl;
        std::cout << "  Avg Latency: " << avg_latency << " ms" << std::endl;
    }

    void RunMixedBenchmark(size_t num_operations, size_t num_threads, double read_ratio) {
        std::cout << "\n=== Mixed Benchmark (Read " << (read_ratio * 100) << "%) ===" << std::endl;
        std::cout << "Operations: " << num_operations << std::endl;
        std::cout << "Threads: " << num_threads << std::endl;

        std::atomic<size_t> reads{0};
        std::atomic<size_t> writes{0};

        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        size_t ops_per_thread = num_operations / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, ops_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_real_distribution<> ratio_dis(0.0, 1.0);
                std::uniform_int_distribution<> key_dis(0, 99999);

                for (size_t i = 0; i < ops_per_thread; ++i) {
                    std::string key = "mixed:" + std::to_string(key_dis(gen));

                    if (ratio_dis(gen) < read_ratio) {
                        // Read operation
                        cache::GetRequest request;
                        request.set_key(key);
                        request.set_consistency(cache::ConsistencyLevel::ONE);

                        cache::GetResponse response;
                        grpc::ClientContext context;
                        stub_->Get(&context, request, &response);
                        reads.fetch_add(1);
                    } else {
                        // Write operation
                        cache::PutRequest request;
                        request.set_key(key);
                        request.set_value("value");
                        request.set_consistency(cache::ConsistencyLevel::ONE);

                        cache::PutResponse response;
                        grpc::ClientContext context;
                        stub_->Put(&context, request, &response);
                        writes.fetch_add(1);
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        size_t total = reads + writes;
        double ops_per_sec = (total * 1000.0) / duration.count();

        std::cout << "\nResults:" << std::endl;
        std::cout << "  Reads: " << reads << std::endl;
        std::cout << "  Writes: " << writes << std::endl;
        std::cout << "  Total: " << total << std::endl;
        std::cout << "  Duration: " << duration.count() << " ms" << std::endl;
        std::cout << "  Throughput: " << static_cast<int>(ops_per_sec) << " ops/sec" << std::endl;
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<cache::CacheService::Stub> stub_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address> [operations] [threads]" << std::endl;
        std::cerr << "Example: " << argv[0] << " 127.0.0.1:50051 100000 8" << std::endl;
        return 1;
    }

    std::string server_address = argv[1];
    size_t operations = argc > 2 ? std::stoul(argv[2]) : 100000;
    size_t threads = argc > 3 ? std::stoul(argv[3]) : std::thread::hardware_concurrency();

    std::cout << "Distributed Cache Benchmark" << std::endl;
    std::cout << "============================" << std::endl;
    std::cout << "Server: " << server_address << std::endl;
    std::cout << "Hardware threads: " << std::thread::hardware_concurrency() << std::endl;

    Benchmark bench(server_address);

    bench.RunPutBenchmark(operations, threads);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    bench.RunGetBenchmark(operations, threads);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    bench.RunMixedBenchmark(operations, threads, 0.8); // 80% reads

    std::cout << "\nBenchmark completed!" << std::endl;

    return 0;
}
