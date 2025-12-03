#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <cstring>

// Cache-line size for alignment
constexpr size_t CACHE_LINE_SIZE = 64;

// Entry in the hash table
struct alignas(CACHE_LINE_SIZE) CacheEntry {
    std::atomic<uint64_t> version{0};
    std::atomic<bool> valid{false};
    std::string key;
    std::vector<uint8_t> value;
    std::atomic<uint64_t> expiry_time{0};

    CacheEntry() = default;
    CacheEntry(const CacheEntry& other)
            : version(other.version.load()),
              valid(other.valid.load()),
              key(other.key),
              value(other.value),
              expiry_time(other.expiry_time.load()) {}
};

class LockFreeHashTable {
public:
    explicit LockFreeHashTable(size_t capacity = 1024 * 1024);
    ~LockFreeHashTable();

    // Core operations
    bool Put(const std::string& key, const std::vector<uint8_t>& value, uint32_t ttl_seconds = 0);
    std::optional<std::vector<uint8_t>> Get(const std::string& key);
    bool Delete(const std::string& key);

    // Statistics
    size_t Size() const { return size_.load(std::memory_order_relaxed); }
    size_t Capacity() const { return capacity_; }
    double LoadFactor() const { return static_cast<double>(Size()) / Capacity(); }

private:
    // Hash functions
    uint64_t Hash(const std::string& key) const;
    size_t ProbeSequence(size_t hash, size_t attempt) const;

    // Helper functions
    bool IsExpired(const CacheEntry& entry) const;
    uint64_t CurrentTimeMs() const;

    // Memory prefetching hint
    void PrefetchEntry(size_t index) const;

    // Table data
    std::unique_ptr<CacheEntry[]> table_;
    size_t capacity_;
    std::atomic<size_t> size_{0};

    // Constants for probing
    static constexpr size_t MAX_PROBE_LENGTH = 128;
};

// Implementation

inline LockFreeHashTable::LockFreeHashTable(size_t capacity)
        : capacity_(capacity) {
    // Ensure capacity is power of 2 for faster modulo
    size_t pow2 = 1;
    while (pow2 < capacity) pow2 <<= 1;
    capacity_ = pow2;

    table_ = std::make_unique<CacheEntry[]>(capacity_);
}

inline LockFreeHashTable::~LockFreeHashTable() = default;

inline uint64_t LockFreeHashTable::Hash(const std::string& key) const {
    // FNV-1a hash
    uint64_t hash = 14695981039346656037ULL;
    for (char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline size_t LockFreeHashTable::ProbeSequence(size_t hash, size_t attempt) const {
    // Quadratic probing
    return (hash + attempt * attempt) & (capacity_ - 1);
}

inline void LockFreeHashTable::PrefetchEntry(size_t index) const {
    __builtin_prefetch(&table_[index], 0, 3);
}

inline uint64_t LockFreeHashTable::CurrentTimeMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

inline bool LockFreeHashTable::IsExpired(const CacheEntry& entry) const {
    uint64_t expiry = entry.expiry_time.load(std::memory_order_relaxed);
    return expiry > 0 && CurrentTimeMs() > expiry;
}

inline bool LockFreeHashTable::Put(const std::string& key,
                                   const std::vector<uint8_t>& value,
                                   uint32_t ttl_seconds) {
    uint64_t hash = Hash(key);
    uint64_t expiry = ttl_seconds > 0 ? CurrentTimeMs() + ttl_seconds * 1000 : 0;

    for (size_t attempt = 0; attempt < MAX_PROBE_LENGTH; ++attempt) {
        size_t index = ProbeSequence(hash, attempt);
        PrefetchEntry(index);

        CacheEntry& entry = table_[index];

        // Try to claim an invalid slot
        bool expected = false;
        if (entry.valid.load(std::memory_order_acquire) == false) {
            if (entry.valid.compare_exchange_strong(expected, true,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                // Successfully claimed slot
                entry.key = key;
                entry.value = value;
                entry.expiry_time.store(expiry, std::memory_order_relaxed);
                entry.version.fetch_add(1, std::memory_order_release);
                size_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }

        // Update existing key
        if (entry.valid.load(std::memory_order_acquire) && entry.key == key) {
            entry.value = value;
            entry.expiry_time.store(expiry, std::memory_order_relaxed);
            entry.version.fetch_add(1, std::memory_order_release);
            return true;
        }

        // Check for expired entry to reclaim
        if (entry.valid.load(std::memory_order_acquire) && IsExpired(entry)) {
            expected = true;
            if (entry.valid.compare_exchange_strong(expected, false,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                size_.fetch_sub(1, std::memory_order_relaxed);
                // Retry insertion at this slot
                --attempt;
                continue;
            }
        }
    }

    return false; // Table full or max probe reached
}

inline std::optional<std::vector<uint8_t>> LockFreeHashTable::Get(const std::string& key) {
    uint64_t hash = Hash(key);

    for (size_t attempt = 0; attempt < MAX_PROBE_LENGTH; ++attempt) {
        size_t index = ProbeSequence(hash, attempt);
        PrefetchEntry(index);

        const CacheEntry& entry = table_[index];

        if (!entry.valid.load(std::memory_order_acquire)) {
            return std::nullopt; // Key not found
        }

        if (entry.key == key) {
            if (IsExpired(entry)) {
                return std::nullopt; // Expired
            }
            return entry.value;
        }
    }

    return std::nullopt;
}

inline bool LockFreeHashTable::Delete(const std::string& key) {
    uint64_t hash = Hash(key);

    for (size_t attempt = 0; attempt < MAX_PROBE_LENGTH; ++attempt) {
        size_t index = ProbeSequence(hash, attempt);

        CacheEntry& entry = table_[index];

        if (!entry.valid.load(std::memory_order_acquire)) {
            return false; // Key not found
        }

        if (entry.key == key) {
            bool expected = true;
            if (entry.valid.compare_exchange_strong(expected, false,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                size_.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        }
    }

    return false;
}