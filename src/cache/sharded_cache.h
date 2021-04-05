// Mian Qin
// 12/02/2019

#pragma once

#include <atomic>
#include <string>
#include <fstream>

#include "cache.h"

namespace wisckey {

// Single cache shard interface.
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  virtual Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value)) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) = 0;
  virtual void Release(Cache::Handle* handle) = 0;
  virtual bool Erase(const Slice& key, uint32_t hash) = 0;
  virtual void SetCapacity(size_t capacity) = 0;
  virtual size_t GetUsage() = 0;
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ShardedCache : public Cache {
 public:
  ShardedCache(size_t capacity, int num_shard_bits);
  ShardedCache(size_t capacity, int num_shard_bits, std::ifstream &ifile);
  virtual ~ShardedCache() = default;
  virtual CacheShard* GetShard(int shard) = 0;
  virtual const CacheShard* GetShard(int shard) const = 0;
  virtual void* Value(Handle* handle) override = 0;
  virtual size_t GetCharge(Handle* handle) const override = 0;

  virtual uint32_t GetHash(Handle* handle) const = 0;

  virtual void SetCapacity(size_t capacity) override;

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value)) override;
  virtual Handle* Lookup(const Slice& key) override;
  virtual void Release(Handle* handle) override;
  virtual bool Erase(const Slice& key) override;

  virtual size_t GetCapacity() const override;
  virtual size_t GetUsage() override;
  virtual size_t GetUsage(Handle* handle) override;

  int GetNumShardBits() const { return num_shard_bits_; }

 private:
  static inline uint32_t HashSlice(const Slice& s) {
    return static_cast<uint32_t>(GetSliceNPHash64(s.data(), s.size()));
  }

  uint32_t Shard(uint32_t hash) {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
  }

  int num_shard_bits_;
  size_t capacity_;
};

extern int GetDefaultCacheShardBits(size_t capacity);

} // end namespace wisckey
