// Mian Qin
// 12/03/2019

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "lru_cache.h"

namespace wisckey {

LRUHandleTable::LRUHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

LRUHandleTable::~LRUHandleTable() {
  for (uint32_t i = 0; i < length_; i++) {
    LRUHandle *h = list_[i];
    while(h != NULL) {
      LRUHandle *next = h->next_hash;
      h->Free();

      h = next;
    }
  }
  delete list_;
}

void LRUHandleTable::Resize() {
  uint32_t new_length = length_ == 0 ? 16 : (length_ << 1);
  while (new_length < (elems_ << 1)) new_length = new_length << 1;

  LRUHandle **new_list = new LRUHandle *[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);

  // Re-hash
  for (uint32_t i = 0; i < length_; i++) {
    LRUHandle *h = list_[i];
    while(h != NULL) {
      LRUHandle *new_h = h;
      h = h->next_hash;
      
      uint32_t hash = new_h->hash;
      new_h->next_hash = new_list[hash & (new_length-1)];
      new_list[hash & (new_length-1)] = new_h;
    }
  }
  delete [] list_;
  list_ = new_list;
  length_ = new_length;
}

LRUHandle* LRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle *h = list_[hash & (length_ - 1)];
  while(h != NULL && (h->hash != hash || key != h->key())) {
    h = h->next_hash;
  }
  return h;
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return FindPointer(key, hash);
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
  LRUHandle* old = FindPointer(h->key(), h->hash);
  if(old != NULL) { // entry already exist (two threads insert the same key)
    return old;
  } 
  h->next_hash = list_[h->hash & (length_ - 1)];
  list_[h->hash & (length_ - 1)] = h;

  ++elems_;
  if (elems_ > length_) {
    // Since each cache entry is fairly large, we aim for a small
    // average linked list length (<= 1).
    Resize();
  }
  return h;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  uint32_t bucket_idx = hash & (length_ - 1);
  LRUHandle *h = list_[bucket_idx];
  LRUHandle *parent = NULL;
  while(h != NULL && (h->hash != hash || key != h->key())) {
    parent = h;
    h = h->next_hash;
  }
  if (h != NULL) {
    if (parent == NULL) list_[bucket_idx] = h->next_hash;
    else parent->next_hash = h->next_hash;
  }
  return h;
}


LRUCacheShard::LRUCacheShard(size_t capacity) : capacity_(0), usage_(0){
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  SetCapacity(capacity);
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  std::lock_guard<std::mutex> lck (mutex_);
  capacity_ = capacity;
}

Cache::Handle* LRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                      size_t charge, void (*deleter)(const Slice& key, void* value)) {

  LRUHandle *h = reinterpret_cast<LRUHandle*>(new char[sizeof(LRUHandle) - 1 + key.size()]);
  h->value = value;
  h->deleter = deleter;
  h->charge = charge;
  h->hash = hash;
  h->key_length = key.size();
  h->next = h->prev = NULL;
  h->refs = 0;
  h->inCache = 0;
  memcpy(h->key_data, key.data(), key.size());
  size_t total_charge = h->CalcTotalCharge();

  LRUHandle *old_h = NULL;
  h->next = h->prev = NULL;
  h->refs = 0;
  std::vector<LRUHandle*> lru_list;

{
  std::lock_guard<std::mutex> lck (mutex_);
  if ((usage_ + total_charge) > capacity_) {
    // cache full, evict LRU
    EvictFromLRU(total_charge, lru_list); // adjust usage_ in call
    
  } 
  // insert to LRU list
  if ((old_h = table_.Insert(h)) != h) {// entry already exist
    lru_list.push_back(h);
    h = old_h;
    h->Ref();
  }
  else {
    LRU_Insert(h);
    h->Ref();
    usage_ += total_charge;
  }
}
  
  // free evict LRU handles
  for (auto it = lru_list.begin(); it != lru_list.end(); ++it) {
    (*it)->Free();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

Cache::Handle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> lck (mutex_);
  LRUHandle* h = table_.Lookup(key, hash);
  if (h != NULL) {
    // move to MRU position
    LRU_Remove(h);
    LRU_Insert(h);
    h->Ref();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

void LRUCacheShard::Release(Cache::Handle* handle) {
  if (handle == NULL) return;
  bool last_reference = false;
  LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
  {
    std::lock_guard<std::mutex> lck (mutex_);
    last_reference = h->Unref();
    if (last_reference && (!h->InCache())) { // Erased from cache
      LRU_Remove(h);
      size_t charge = h->CalcTotalCharge();
      assert(usage_ >= charge);
      usage_ -= charge;
      h->Free();
    }
  }
}

bool LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* h;
  bool last_reference = false;
  {
    std::lock_guard<std::mutex> lck (mutex_);
    h = table_.Remove(key, hash);
    if (h != nullptr) {
      h->SetInCache (false);
      if (!h->HasRefs()) {
        // The entry is in LRU since it's in hash and has no external references
        LRU_Remove(h);
        size_t charge = h->CalcTotalCharge();
        assert(usage_ >= charge);
        usage_ -= charge;
        last_reference = true;
      }
    }
  }

  // Free the entry
  if (last_reference) h->Free();
  return h!=NULL ;
}

size_t LRUCacheShard::GetUsage() {
  std::lock_guard<std::mutex> lck (mutex_);
  return usage_;
}

void LRUCacheShard::LRU_Remove(LRUHandle* h) {
  // remove from LRU list
  h->prev->next = h->next;
  h->next->prev = h->prev;
  h->next = h->prev = NULL;
}

void LRUCacheShard::LRU_Insert(LRUHandle* h) {
  // add handle to LRU list in MRU position
  h->next = &lru_;
  h->prev = lru_.prev;
  lru_.prev->next = h;
  lru_.prev = h;
}

void LRUCacheShard::EvictFromLRU(size_t charge, std::vector<LRUHandle*>& deleted) {
  LRUHandle *h = lru_.next;
  while (h != &lru_ && (usage_ + charge) > capacity_) {
    if (!h->HasRefs()) {
      deleted.push_back(h);
      size_t evict_charge = h->CalcTotalCharge();
      LRUHandle *next_h = h->next;
      table_.Remove(h->key(), h->hash);
      LRU_Remove(h);
      h = next_h;
      usage_ -= evict_charge;
    }
    else {
      h = h->next;
    }
  }
}


LRUCache::LRUCache(size_t capacity, int num_shard_bits)
    : ShardedCache(capacity, num_shard_bits) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LRUCacheShard*>(
      malloc(sizeof(LRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LRUCacheShard(per_shard);
  }
}

LRUCache::~LRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~LRUCacheShard();
    }
    free(shards_);
  }
}

CacheShard* LRUCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LRUCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LRUCache::Value(Handle* handle) {
  return reinterpret_cast<const LRUHandle*>(handle)->value;
}

size_t LRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->charge;
}

uint32_t LRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->hash;
}

Cache* NewLRUCache(size_t capacity, int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return reinterpret_cast<Cache*>(new LRUCache(capacity, num_shard_bits));
}

} // end namespace wisckey
