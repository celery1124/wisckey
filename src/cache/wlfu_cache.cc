// Mian Qin
// 04/13/2020

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "wlfu_cache.h"

namespace wisckey {

LFUHandleTable::LFUHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

LFUHandleTable::~LFUHandleTable() {
  for (uint32_t i = 0; i < length_; i++) {
    LFUHandle *h = list_[i];
    while(h != NULL) {
      LFUHandle *next = h->next_hash;
      h->Free();
      h = next;
    }
  }
  delete list_;
}

void LFUHandleTable::Resize() {
  uint32_t new_length = length_ == 0 ? 16 : (length_ << 1);
  while (new_length < (elems_ << 1)) new_length = new_length << 1;

  LFUHandle **new_list = new LFUHandle *[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);

  // Re-hash
  for (uint32_t i = 0; i < length_; i++) {
    LFUHandle *h = list_[i];
    while(h != NULL) {
      LFUHandle *new_h = h;
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

LFUHandle* LFUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LFUHandle *h = list_[hash & (length_ - 1)];
  while(h != NULL && (h->hash != hash || key != h->key())) {
    h = h->next_hash;
  }
  return h;
}

LFUHandle* LFUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return FindPointer(key, hash);
}

LFUHandle* LFUHandleTable::Insert(LFUHandle* h) {
  LFUHandle* old = FindPointer(h->key(), h->hash);
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

LFUHandle* LFUHandleTable::Remove(const Slice& key, uint32_t hash) {
  uint32_t bucket_idx = hash & (length_ - 1);
  LFUHandle *h = list_[bucket_idx];
  LFUHandle *parent = NULL;
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


LFUCacheShard::LFUCacheShard(size_t capacity) : capacity_(0), usage_(0){
  // Make empty circular linked list
  freqList_.next = &freqList_;
  freqList_.prev = &freqList_;
  SetCapacity(capacity);
}

LFUCacheShard::~LFUCacheShard() {
  FrequencyListHandle *h = freqList_.next;
  while (h != &freqList_) {
    LFUHandle *hh = h->head.next;
    int count = 0;
    while(hh != &(h->head)) {
	hh = hh->next;
	count++;
    }
    h = h->next;
  }
}

void LFUCacheShard::SetCapacity(size_t capacity) {
  std::lock_guard<std::mutex> lck (mutex_);
  capacity_ = capacity;
}

Cache::Handle* LFUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                      size_t charge, void (*deleter)(const Slice& key, void* value)) {

                        
  LFUHandle *h = reinterpret_cast<LFUHandle*>(new char[sizeof(LFUHandle) - 1 + key.size()]);
  h->value = value;
  h->deleter = deleter;
  h->charge = charge;
  h->hash = hash;
  h->parent = NULL;
  h->key_length = key.size();
  h->next = h->prev = NULL;
  h->refs = 0;
  h->inCache = 0;
  memcpy(h->key_data, key.data(), key.size());
  size_t total_charge = h->CalcTotalCharge();

  LFUHandle *old_h = NULL;
  std::vector<LFUHandle*> lru_list;

{
  std::lock_guard<std::mutex> lck (mutex_);
  if ((total_charge + charge) > capacity_) {
    // cache full, evict LRU
    EvictFromLFU(total_charge, lru_list); // adjust usage_ in call
    
  } 
  // insert to LFU list
  if ((old_h = table_.Insert(h)) != h) {// entry already exist
    lru_list.push_back(h);
    h = old_h;
    // update LFU list
    LFU_Update(h);
    h->Ref();
  }
  else {
    LFU_Insert(h);
    h->Ref();
    usage_ += total_charge;
  }
}
  
  // free evict LRU handles
  for (auto it = lru_list.begin(); it != lru_list.end(); ++it) {
    //printf("Evict key %s\n", std::string((*it)->key_data,(*it)->key_length).c_str());
    (*it)->Free();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

Cache::Handle* LFUCacheShard::Lookup(const Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> lck (mutex_);
  LFUHandle* h = table_.Lookup(key, hash);
  if (h != NULL) {
    // update LFU list
    LFU_Update(h);
    h->Ref();
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

void LFUCacheShard::Release(Cache::Handle* handle) {
  if (handle == NULL) return;
  bool last_reference = false;
  LFUHandle* h = reinterpret_cast<LFUHandle*>(handle);
  {
    std::lock_guard<std::mutex> lck (mutex_);
    last_reference = h->Unref();
    if (last_reference && (!h->InCache())) { // Erased from cache
      LFU_Remove(h);
      size_t charge = h->CalcTotalCharge();
      assert(usage_ >= charge);
      usage_ -= charge;
      h->Free();
    }
  }
}

bool LFUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LFUHandle* h;
  bool last_reference = false;
  {
    std::lock_guard<std::mutex> lck (mutex_);
    h = table_.Remove(key, hash);
    if (h != nullptr) {
      h->SetInCache (false);
      if (!h->HasRefs()) {
        // The entry is in LFU since it's in hash and has no external references
        LFU_Remove(h);
        size_t charge = h->CalcTotalCharge();
        assert(usage_ >= charge);
        usage_ -= charge;
        last_reference = true;
      }
    }
  }

  // Free the entry
  if (last_reference) h->Free();
  return h != NULL;
}

size_t LFUCacheShard::GetUsage() {
  std::lock_guard<std::mutex> lck (mutex_);
  return usage_;
}

void LFUCacheShard::LFU_Remove(LFUHandle* h) {
  // remove from double linked list
  h->prev->next = h->next;
  h->next->prev = h->prev;
  h->next = h->prev = NULL;
}

void LFUCacheShard::LFU_Insert(LFUHandle* h) {
  // add handle to LFU list
  FrequencyListHandle* lowestFreq = freqList_.next;
  if (lowestFreq->frequency != 1) {
    FrequencyListHandle* newLowestFreq = new FrequencyListHandle(1);
    newLowestFreq->next = freqList_.next;
    newLowestFreq->prev = &freqList_;
    freqList_.next->prev = newLowestFreq;
    freqList_.next = newLowestFreq;
    lowestFreq = newLowestFreq;
  }
  
  h->next = &(lowestFreq->head);
  h->prev = lowestFreq->head.prev;
  lowestFreq->head.prev->next = h;
  lowestFreq->head.prev = h;
  h->parent = lowestFreq;
}

void LFUCacheShard::LFU_Update(LFUHandle* h) {
  // insert updated handle to LFU list
  FrequencyListHandle* currFreq = h->parent;
  FrequencyListHandle* nextFreq;
  if (currFreq->next->frequency != currFreq->frequency+1) {
    nextFreq = new FrequencyListHandle(currFreq->frequency+1);
    nextFreq->prev = currFreq;
    nextFreq->next = currFreq->next;
    currFreq->next->prev = nextFreq;
    currFreq->next = nextFreq;
  }
  else nextFreq = currFreq->next;
  
  // remove old handle OR even freqList node
  LFU_Remove(h);
  if (currFreq->head.next == &(currFreq->head)) {
    currFreq->prev->next = currFreq->next;
    currFreq->next->prev = currFreq->prev;
    delete currFreq;
  }

  h->next = &(nextFreq->head);
  h->prev = nextFreq->head.prev;
  nextFreq->head.prev->next = h;
  nextFreq->head.prev = h;
  h->parent = nextFreq;
}

void LFUCacheShard::EvictFromLFU(size_t charge, std::vector<LFUHandle*>& deleted) {
  FrequencyListHandle* freq = freqList_.next;
  while (freq != &freqList_ && (usage_ + charge) > capacity_) {
    LFUHandle *h = freq->head.next;
    while (h->next != h && !h->HasRefs() && (usage_ + charge) > capacity_) {
      deleted.push_back(h);
      size_t evict_charge = h->CalcTotalCharge();
      LFUHandle *next_h = h->next;
      table_.Remove(h->key(), h->hash);

      LFU_Remove(h);
      h = next_h;
      usage_ -= evict_charge;
    }
    FrequencyListHandle* nextFreq = freq->next;
    if (freq->head.next == &(freq->head)) {
      freq->prev->next = freq->next;
      freq->next->prev = freq->prev;
      delete freq;
    }
    freq = nextFreq;
  }
}


LFUCache::LFUCache(size_t capacity, int num_shard_bits)
    : ShardedCache(capacity, num_shard_bits) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LFUCacheShard*>(
      malloc(sizeof(LFUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LFUCacheShard(per_shard);
  }
}

LFUCache::~LFUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~LFUCacheShard();
    }
    free(shards_);
  }
}

CacheShard* LFUCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LFUCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LFUCache::Value(Handle* handle) {
  return reinterpret_cast<const LFUHandle*>(handle)->value;
}

size_t LFUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const LFUHandle*>(handle)->charge;
}

uint32_t LFUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const LFUHandle*>(handle)->hash;
}

Cache* NewLFUCache(size_t capacity, int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return reinterpret_cast<Cache*>(new LFUCache(capacity, num_shard_bits));
}

} // end namespace wisckey
