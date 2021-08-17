/******* wisckey *******/
/* db_impl.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _db_impl_h_
#define _db_impl_h_

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <mutex>
#include <unordered_map>
#include "wisckey/db.h"
#include "cache/cache.h"

#include "hash.h"
#include "threadpool.h"

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#define MAX_THREAD_CNT 64
#define PAGE_SIZE 4096
#define PAGE_SHIFT 12
#define LOG_PARTITION 4

#define WAL_FLUSH_CNT 16

namespace wisckey {

class CacheEntry { 
public:
    char *val;
    int size;
    CacheEntry() : val(NULL), size(0){};
    CacheEntry(char *v, int s) : size(s) {
        val = (char *)malloc(s);
        memcpy(val, v, size);
    }
    ~CacheEntry() {if(val) free(val);}
};
template <class T>
static void DeleteEntry(const Slice& /*key*/, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

class DBImpl : public DB{
friend class DBIterator;
public:
  DBImpl(const Options& options, const std::string& dbname);
  ~DBImpl();

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  Status Delete(const WriteOptions&, const Slice& key);
  // Status Write(const WriteOptions& options, WriteBatch* updates);
  Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  Iterator* NewIterator(const ReadOptions&);
  void vLogGarbageCollect();

private:
  Options options_;
  std::string dbname_;
  // rocksdb for key-offset
  rocksdb::DB* rdb_;

  // write ahead log
  std::mutex walM_[LOG_PARTITION];
  FILE *walFD_[LOG_PARTITION];
  int wal_buf_offset_[LOG_PARTITION];
  int wal_flush_cnt_[LOG_PARTITION];

  // value log
  std::mutex logM_[LOG_PARTITION];
  int logFD_[LOG_PARTITION];
  uint64_t lba_[LOG_PARTITION];
  char* aligned_log_buf_[LOG_PARTITION];
  uint64_t log_buf_offset_[LOG_PARTITION];

  void flushVLog();
  void vLogGCWorker(std::vector<std::string> *ukey_list, std::vector<std::string> *vmeta_list, int idx, int size, int* oldLogFD, int* newLogFD);

  // thread pool
  threadpool_t *pool_;
  sem_t q_sem_;
  // I/O request conter (read only for now)
  std::atomic<int64_t> inflight_io_count_;

  // in-memory cache
  Cache *cache_;

  // in-memory cache interface
	Cache::Handle* read_cache(std::string& key, std::string* value) {
        if (cache_==NULL) return NULL;
        Cache::Handle *h = cache_->Lookup(key);
        if (h != NULL) {
            CacheEntry *rd_val = reinterpret_cast<CacheEntry*>(cache_->Value(h));
            value->append(rd_val->val, rd_val->size);
            RecordTick(options_.statistics.get(), CACHE_HIT);
        }
        else 
            RecordTick(options_.statistics.get(), CACHE_MISS);
        return h;
    };
    Cache::Handle* insert_cache(std::string& key, const Slice& value) {
        if (cache_==NULL) return NULL;
        CacheEntry *ins_val = new CacheEntry((char *)value.data(), value.size());
        size_t charge = sizeof(CacheEntry) + value.size();
        Cache::Handle *h = cache_->Insert(key, reinterpret_cast<void*>(ins_val), charge, DeleteEntry<CacheEntry>);
        RecordTick(options_.statistics.get(), CACHE_FILL);
        return h;
    };
    void erase_cache(std::string& key) {
        if (cache_==NULL) return ;
        bool evicted = cache_->Erase(key);

        if (evicted) RecordTick(options_.statistics.get(), CACHE_ERASE);
    };
    void release_cache(Cache::Handle* h) {
        if (cache_==NULL) return ;
        cache_->Release(h);
    };
};


}  // namespace wisckey



#endif
