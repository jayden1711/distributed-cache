#include <grpcpp/grpcpp.h>
#include "cache.grpc.pb.h"
#include <iostream>
#include <string>
#include <vector>

class CacheClient {
public:
    CacheClient(const std::string& address) {
        channel_ = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        stub_ = cache::CacheService::NewStub(channel_);
    }

    bool Put(const std::string& key, const std::string& value, uint32_t ttl = 0,
             cache::ConsistencyLevel consistency = cache::ConsistencyLevel::QUORUM) {
        cache::PutRequest request;
        request.set_key(key);
        request.set_value(value);
        request.set_ttl_seconds(ttl);
        request.set_consistency(consistency);

        cache::PutResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->Put(&context, request, &response);

        if (status.ok()) {
            return response.success();
        } else {
            std::cerr << "Put failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    std::string Get(const std::string& key,
                    cache::ConsistencyLevel consistency = cache::ConsistencyLevel::QUORUM) {
        cache::GetRequest request;
        request.set_key(key);
        request.set_consistency(consistency);

        cache::GetResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->Get(&context, request, &response);

        if (status.ok() && response.found()) {
            return response.value();
        } else if (!status.ok()) {
            std::cerr << "Get failed: " << status.error_message() << std::endl;
        }
        return "";
    }

    bool Delete(const std::string& key,
                cache::ConsistencyLevel consistency = cache::ConsistencyLevel::QUORUM) {
        cache::DeleteRequest request;
        request.set_key(key);
        request.set_consistency(consistency);

        cache::DeleteResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->Delete(&context, request, &response);

        if (status.ok()) {
            return response.success();
        } else {
            std::cerr << "Delete failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    void BatchPut(const std::vector<std::pair<std::string, std::string>>& items) {
        grpc::ClientContext context;
        cache::BatchResponse response;

        auto writer = stub_->BatchPut(&context, &response);

        for (const auto& [key, value] : items) {
            cache::PutRequest request;
            request.set_key(key);
            request.set_value(value);
            writer->Write(request);
        }

        writer->WritesDone();
        grpc::Status status = writer->Finish();

        if (status.ok()) {
            std::cout << "Batch put: " << response.success_count() << " succeeded, "
                      << response.failure_count() << " failed" << std::endl;
        } else {
            std::cerr << "Batch put failed: " << status.error_message() << std::endl;
        }
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<cache::CacheService::Stub> stub_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
        std::cerr << "Example: " << argv[0] << " 127.0.0.1:50051" << std::endl;
        return 1;
    }

    std::string server_address = argv[1];
    CacheClient client(server_address);

    std::cout << "Connected to cache server at " << server_address << std::endl;
    std::cout << "\nRunning example operations...\n" << std::endl;

    // Example 1: Simple Put/Get
    std::cout << "1. Simple Put/Get:" << std::endl;
    if (client.Put("user:1000", "John Doe")) {
        std::cout << "   PUT user:1000 = John Doe [OK]" << std::endl;
    }

    std::string value = client.Get("user:1000");
    if (!value.empty()) {
        std::cout << "   GET user:1000 = " << value << " [OK]" << std::endl;
    }

    // Example 2: Put with TTL
    std::cout << "\n2. Put with TTL (5 seconds):" << std::endl;
    if (client.Put("session:abc", "active", 5)) {
        std::cout << "   PUT session:abc = active (TTL=5s) [OK]" << std::endl;
    }

    // Example 3: Different consistency levels
    std::cout << "\n3. Consistency levels:" << std::endl;
    if (client.Put("config:version", "1.2.3", 0, cache::ConsistencyLevel::ALL)) {
        std::cout << "   PUT config:version = 1.2.3 (consistency=ALL) [OK]" << std::endl;
    }

    std::string config = client.Get("config:version", cache::ConsistencyLevel::ONE);
    if (!config.empty()) {
        std::cout << "   GET config:version = " << config << " (consistency=ONE) [OK]" << std::endl;
    }

    // Example 4: Batch operations
    std::cout << "\n4. Batch Put:" << std::endl;
    std::vector<std::pair<std::string, std::string>> batch = {
            {"product:1", "Laptop"},
            {"product:2", "Mouse"},
            {"product:3", "Keyboard"},
            {"product:4", "Monitor"},
            {"product:5", "Webcam"}
    };
    client.BatchPut(batch);

    // Example 5: Delete
    std::cout << "\n5. Delete:" << std::endl;
    if (client.Delete("user:1000")) {
        std::cout << "   DELETE user:1000 [OK]" << std::endl;
    }

    value = client.Get("user:1000");
    if (value.empty()) {
        std::cout << "   GET user:1000 = <not found> [OK]" << std::endl;
    }

    std::cout << "\nAll examples completed!" << std::endl;

    return 0;
}
