// Mian Qin
// 12/02/2019

#pragma once

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <atomic>
#include <string>
#include <fstream>

#include "../hash.h"

namespace wisckey {

class Cache {
 public:
  Cache() {};

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache() {};

  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual bool Erase(const Slice& key) = 0;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  virtual void SetCapacity(size_t capacity) = 0;

    // returns the maximum configured capacity of the cache
  virtual size_t GetCapacity() const = 0;

  // returns the memory size for the entries residing in the cache.
  virtual size_t GetUsage() = 0;

  // returns the memory size for a specific entry in the cache.
  virtual size_t GetUsage(Handle* handle) = 0;

  // returns the charge for the specific entry in the cache.
  virtual size_t GetCharge(Handle* handle) const = 0;

 private:
};


extern Cache* NewLRUCache(size_t capacity, int num_shard_bits);
extern Cache* NewFIFOCache(size_t capacity, int num_shard_bits);
extern Cache* NewLFUCache(size_t capacity, int num_shard_bits);
extern Cache* NewPLFUCache(size_t capacity, int num_shard_bits);
extern Cache* NewBCache(size_t capacity, int num_shard_bits, std::ifstream &ifile);

} // end namespace wisckey
