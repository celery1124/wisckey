// Mian Qin
// 12/02/2019

#pragma once

#include <string>
#include <mutex>

#include "sharded_cache.h"

namespace wisckey {

// LRU implementation

// LRU Tag array, implemented as a double circular link list
// next node is the LRU position
// prev node is the MRU position
class LRUHandle {
public:
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t key_length;
  size_t charge; 

  uint8_t inCache;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;
  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  bool InCache () { return (inCache == 0); }
  bool SetInCache (bool flag) { inCache = flag ? 0 : 1; }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  Slice key() const { return Slice(key_data, key_length); }

  void Free() {
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }

  // Caclculate the memory usage by metadata
  inline size_t CalcTotalCharge() {
    return charge + sizeof(LRUHandle) - 1 + key_length;
  }
};


// Hash Table for storing the LRUHandles pointers
// Fast lookup
class LRUHandleTable {
public:
  LRUHandleTable();
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);

private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle* FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  LRUHandle** list_;
  uint32_t length_;
  uint32_t elems_;
};

class LRUCacheShard : public CacheShard {
public: 
  LRUCacheShard(size_t capacity);
  ~LRUCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  void SetCapacity(size_t capacity) override;

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value)) override;
  Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;
  void Release(Cache::Handle* handle) override;
  bool Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  size_t GetUsage() override;
private:
  void LRU_Remove(LRUHandle* h);
  void LRU_Insert(LRUHandle* h);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, std::vector<LRUHandle*>& deleted);

  // Initialized before use.
  size_t capacity_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  LRUHandleTable table_;

  // Memory size for entries residing in the cache
  size_t usage_;

  // mutex_ protects the following state.
  std::mutex mutex_;
};

class LRUCache : public ShardedCache {
public:
  LRUCache(size_t capacity, int num_shard_bits);
  virtual ~LRUCache();
  CacheShard* GetShard(int shard) override;
  const CacheShard* GetShard(int shard) const override;
  void* Value(Handle* handle) override;
  uint32_t GetHash(Handle* handle) const override;
  size_t GetCharge(Handle* handle) const override;

private:
  LRUCacheShard* shards_;
  int num_shards_;
};

} // end namespace wisckey
