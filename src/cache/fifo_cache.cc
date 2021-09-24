// Mian Qin
// 12/03/2019

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "fifo_cache.h"

namespace wisckey {

FIFOHandleTable::FIFOHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

FIFOHandleTable::~FIFOHandleTable() {
  for (uint32_t i = 0; i < length_; i++) {
    FIFOHandle *h = list_[i];
    while(h != NULL) {
      FIFOHandle *next = h->next_hash;
      h->Free();

      h = next;
    }
  }
  delete list_;
}

void FIFOHandleTable::Resize() {
  uint32_t new_length = length_ == 0 ? 16 : (length_ << 1);
  while (new_length < (elems_ << 1)) new_length = new_length << 1;

  FIFOHandle **new_list = new FIFOHandle *[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);

  // Re-hash
  for (uint32_t i = 0; i < length_; i++) {
    FIFOHandle *h = list_[i];
    while(h != NULL) {
      FIFOHandle *new_h = h;
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

FIFOHandle* FIFOHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  FIFOHandle *h = list_[hash & (length_ - 1)];
  while(h != NULL && (h->hash != hash || key != h->key())) {
    h = h->next_hash;
  }
  return h;
}

FIFOHandle* FIFOHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return FindPointer(key, hash);
}

FIFOHandle* FIFOHandleTable::Insert(FIFOHandle* h) {
  FIFOHandle* old = FindPointer(h->key(), h->hash);
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

FIFOHandle* FIFOHandleTable::Remove(const Slice& key, uint32_t hash) {
  uint32_t bucket_idx = hash & (length_ - 1);
  FIFOHandle *h = list_[bucket_idx];
  FIFOHandle *parent = NULL;
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


FIFOCacheShard::FIFOCacheShard(size_t capacity) : capacity_(0), usage_(0){
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  SetCapacity(capacity);
}

void FIFOCacheShard::SetCapacity(size_t capacity) {
  std::lock_guard<std::mutex> lck (mutex_);
  capacity_ = capacity;
}

Cache::Handle* FIFOCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                      size_t charge, void (*deleter)(const Slice& key, void* value)) {

  FIFOHandle *h = reinterpret_cast<FIFOHandle*>(new char[sizeof(FIFOHandle) - 1 + key.size()]);
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

  uint64_t ino = *(uint64_t *)key.data();

  FIFOHandle *old_h = NULL;
  h->next = h->prev = NULL;
  h->refs = 0;
  std::vector<FIFOHandle*> lru_list;

{
  std::lock_guard<std::mutex> lck (mutex_);
  if ((usage_ + total_charge) > capacity_) {
    // cache full, evict FIFO
    EvictFromFIFO(total_charge, lru_list); // adjust usage_ in call
  } 
  // insert to FIFO list
  if ((old_h = table_.Insert(h)) != h) {// entry already exist
    lru_list.push_back(h);
    h = old_h;
    h->Ref();
  }
  else {
    FIFO_Insert(h);
    h->Ref();
    usage_ += total_charge;
  }
}
  
  // free evict FIFO handles
  for (auto it = lru_list.begin(); it != lru_list.end(); ++it) {
    uint64_t ino = *(uint64_t *)(*it)->key_data;
    (*it)->Free();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

Cache::Handle* FIFOCacheShard::Lookup(const Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> lck (mutex_);
  FIFOHandle* h = table_.Lookup(key, hash);
  if (h != NULL) {
    h->Ref();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

void FIFOCacheShard::Release(Cache::Handle* handle) {
  if (handle == NULL) return;
  bool last_reference = false;
  FIFOHandle* h = reinterpret_cast<FIFOHandle*>(handle);
  {
    std::lock_guard<std::mutex> lck (mutex_);
    last_reference = h->Unref();
    if (last_reference && (!h->InCache())) { // Erased from cache
      FIFO_Remove(h);
      size_t charge = h->CalcTotalCharge();
      assert(usage_ >= charge);
      usage_ -= charge;
      h->Free();
    }
  }
}

bool FIFOCacheShard::Erase(const Slice& key, uint32_t hash) {
  FIFOHandle* h;
  bool last_reference = false;
  {
    std::lock_guard<std::mutex> lck (mutex_);
    h = table_.Remove(key, hash);
    if (h != nullptr) {
      h->SetInCache (false);
      if (!h->HasRefs()) {
        // The entry is in FIFO since it's in hash and has no external references
        FIFO_Remove(h);
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

size_t FIFOCacheShard::GetUsage() {
  std::lock_guard<std::mutex> lck (mutex_);
  return usage_;
}

void FIFOCacheShard::FIFO_Remove(FIFOHandle* h) {
  // remove from FIFO tail
  h->prev->next = h->next;
  h->next->prev = h->prev;
  h->next = h->prev = NULL;
}

void FIFOCacheShard::FIFO_Insert(FIFOHandle* h) {
  // add handle to FIFO list in head position
  h->next = &lru_;
  h->prev = lru_.prev;
  lru_.prev->next = h;
  lru_.prev = h;
}

void FIFOCacheShard::EvictFromFIFO(size_t charge, std::vector<FIFOHandle*>& deleted) {
  FIFOHandle *h = lru_.next;
  while (h != &lru_ && (usage_ + charge) > capacity_) {
    if (!h->HasRefs()) {
      deleted.push_back(h);
      size_t evict_charge = h->CalcTotalCharge();
      FIFOHandle *next_h = h->next;
      table_.Remove(h->key(), h->hash);
      FIFO_Remove(h);
      h = next_h;
      usage_ -= evict_charge;
    }
    else {
      h = h->next;
    }
  }
}


FIFOCache::FIFOCache(size_t capacity, int num_shard_bits)
    : ShardedCache(capacity, num_shard_bits) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<FIFOCacheShard*>(
      malloc(sizeof(FIFOCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        FIFOCacheShard(per_shard);
  }
}

FIFOCache::~FIFOCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~FIFOCacheShard();
    }
    free(shards_);
  }
}

CacheShard* FIFOCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* FIFOCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* FIFOCache::Value(Handle* handle) {
  return reinterpret_cast<const FIFOHandle*>(handle)->value;
}

size_t FIFOCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const FIFOHandle*>(handle)->charge;
}

uint32_t FIFOCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const FIFOHandle*>(handle)->hash;
}

Cache* NewFIFOCache(size_t capacity, int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return reinterpret_cast<Cache*>(new FIFOCache(capacity, num_shard_bits));
}

} // end namespace wisckey
