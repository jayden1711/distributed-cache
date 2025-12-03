cmake_minimum_required(VERSION 3.15)
project(DistributedCache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optimization flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Find required packages
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Generate protobuf and gRPC files
set(PROTO_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/cache.proto
)

set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/cache.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/cache.pb.h")
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/cache.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/cache.grpc.pb.h")

add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
        COMMAND protobuf::protoc
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_CURRENT_SOURCE_DIR}/proto"
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${PROTO_FILES}
        DEPENDS ${PROTO_FILES}
)

# Include directories
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

# Source files
set(SOURCES
        src/cache_node.cpp
        src/main.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
)

# Main executable
add_executable(cache_node ${SOURCES})

target_link_libraries(cache_node
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)

# Client example
add_executable(cache_client
        src/client_example.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
)

target_link_libraries(cache_client
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)

# Benchmarking tool
add_executable(cache_benchmark
        src/benchmark.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
)

target_link_libraries(cache_benchmark
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)

# Installation
install(TARGETS cache_node cache_client cache_benchmark
        RUNTIME DESTINATION bin)
