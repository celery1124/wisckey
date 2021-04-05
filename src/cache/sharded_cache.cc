// Mian Qin
// 12/02/2019

#include <string.h>

#include "sharded_cache.h"

namespace wisckey {

ShardedCache::ShardedCache(size_t capacity, int num_shard_bits)
    : num_shard_bits_(num_shard_bits),
      capacity_(capacity) {}

void ShardedCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << num_shard_bits_;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  // don't guard by mutex
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  capacity_ = capacity;
}

Cache::Handle* ShardedCache::Insert(const Slice& key, void* value, size_t charge,
                            void (*deleter)(const Slice& key, void* value)) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Insert(key, hash, value, charge, deleter);
}

Cache::Handle* ShardedCache::Lookup(const Slice& key) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->Lookup(key, hash);
}

void ShardedCache::Release(Handle* handle) {
  uint32_t hash = GetHash(handle);
  GetShard(Shard(hash))->Release(handle);
}

bool ShardedCache::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key, hash);
}

size_t ShardedCache::GetCapacity() const {
  // don't guard by mutex
  return capacity_;
}

size_t ShardedCache::GetUsage() {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

size_t ShardedCache::GetUsage(Handle* handle) {
  return GetCharge(handle);
}

int GetDefaultCacheShardBits(size_t capacity) {
  int num_shard_bits = 0;
  size_t min_shard_size = 512L * 1024L;  // Every shard is at least 512KB.
  size_t num_shards = capacity / min_shard_size;
  while (num_shards >>= 1) {
    if (++num_shard_bits >= 6) {
      // No more than 6.
      return num_shard_bits;
    }
  }
  return num_shard_bits;
}

} // end namespace wisckey
