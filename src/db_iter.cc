/******* wisckey *******/
/* db_iter.cc
* 08/06/2019
* by Mian Qin
*/
#include <atomic>
#include <mutex>
#include <iostream>
#include <condition_variable>
#include <unordered_map>
#include "wisckey/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

namespace wisckey {
inline uint64_t Hash0 (const Slice& key) {
  return NPHash64(key.data(), key.size());
}

inline uint64_t Hash0 (const rocksdb::Slice& key) {
  return NPHash64(key.data(), key.size());
}

inline uint64_t Hash0 (std::string& key) {
  return NPHash64(key.data(), key.size());
}

inline uint64_t roundUp(uint64_t in, uint64_t roundTo)
{
    assert(roundTo > 0);
    uint64_t remainder = in % roundTo;
    if (remainder == 0)
        return in;
    else
      return in + roundTo - remainder;
}

inline uint64_t roundDown(uint64_t in, uint64_t roundTo)
{
    assert(roundTo > 0);
    uint64_t remainder = in % roundTo;
    if (remainder == 0)
        return in;
    else
      return in - remainder;
}

typedef struct {
  int fd; 
	void* buf;
  size_t count;
  off_t offset;

  void (*callback)(void *);
  void *argument;
} dev_io_context;

static void io_task(void *arg) {
  dev_io_context* ctx = (dev_io_context *)arg;
  int ret;
  
  pread(ctx->fd, ctx->buf, ctx->count, ctx->offset);
  if (ctx->callback != NULL)
    ctx->callback(ctx->argument);
  delete ctx;
}

// Monitor for async I/O
class Monitor {
public:
  std::mutex mtx_;
  std::condition_variable cv_;
  bool ready_ ;
  Monitor() : ready_(false) {}
  ~Monitor(){}
  void reset() {ready_ = false;};
  void notify() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_one();
  }
  void notifyAll() {
    std::unique_lock<std::mutex> lck(mtx_);
    ready_ = true;
    cv_.notify_all();
  }
  void wait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!ready_) cv_.wait(lck);
  }
};

class Prefetch_context{
public:
  std::atomic<int> prefetch_cnt;
  int prefetch_num;
  Monitor *mon;
  Prefetch_context (int prefetch_num_, Monitor *mon_) : prefetch_cnt(0), prefetch_num(prefetch_num_), mon(mon_) {}
} ;

void on_prefetch_complete(void* args) {
  Prefetch_context *prefetch_ctx = (Prefetch_context *)args;
  if (prefetch_ctx->prefetch_cnt.fetch_add(1) == prefetch_ctx->prefetch_num-1)
    prefetch_ctx->mon->notify();

}

class DBIterator : public Iterator {
public:
  DBIterator(DBImpl *db, const ReadOptions &options);
  ~DBIterator();

  bool Valid() const {
    return valid_;
  }
  void SeekToFirst();
  void SeekToLast() { /* NOT IMPLEMENT */ }
  void Seek(const Slice& target);
  void Next();
  void Prev();
  Slice key() const;
  Slice value();
private:
  DBImpl *db_;
  const ReadOptions &options_;
  rocksdb::Slice s_upper_key_;
  rocksdb::Iterator *it_;
  std::string value_;
  bool valid_;

  // upper key hint
  Slice upper_key_;

  // for value prefetch
  bool prefetch_ena_;
  std::string *key_queue_;
  std::string *pkey_queue_;
  std::string *val_queue_;
  bool *valid_queue_;
  int prefetch_depth_;
  int queue_cur_;

  bool async_pread(int fd, void *buf, size_t count, off_t offset, void (*callback)(void *), void *args);
  void prefetch_value(std::vector<int> fd_list, std::vector<std::string>& pkey_list, 
                                std::vector<std::string>& val_list);

};

bool DBIterator::async_pread(int fd, void *buf, size_t count, off_t offset, void (*callback)(void *), void *args) {
    dev_io_context *dev_ctx = new dev_io_context{fd, buf, count, offset, callback, args};
    sem_wait(&(db_->q_sem_));
    if (threadpool_add(db_->pool_, &io_task, dev_ctx, 0) < 0) {
        printf("async_pread pool_add error, fd %d, offset %llu\n", fd, offset);
        exit(1);
    }
    return true;
}

void DBIterator::prefetch_value(std::vector<int> fd_list, std::vector<std::string>& pkey_list, 
                                std::vector<std::string>& val_list) {
  int prefetch_num = pkey_list.size();
  char **aligned_val_buf_list = new char*[prefetch_num];
  char **vbuf_list = new char*[prefetch_num];
  uint32_t *valSize_list = new uint32_t[prefetch_num];
  Monitor mon;
  Prefetch_context *ctx = new Prefetch_context (prefetch_num, &mon);

  db_->inflight_io_count_.fetch_add(prefetch_num, std::memory_order_relaxed);
  for (int i = 0 ; i < prefetch_num; i++) {
    char *p = (char *)&(pkey_list[i][0]);
    uint64_t logOffset= *((uint64_t *)p);
    p = p + 8;
    valSize_list[i] = *((uint32_t *)p);
    // calculate aligned read address
    uint64_t aligned_raddr = roundDown(logOffset, PAGE_SIZE);
    int rSize = roundUp(logOffset - aligned_raddr + valSize_list[i], PAGE_SIZE);
    aligned_val_buf_list[i] = (char *)aligned_alloc(PAGE_SIZE, rSize);
    vbuf_list[i] = aligned_val_buf_list[i] + logOffset - aligned_raddr;
    async_pread(fd_list[i], aligned_val_buf_list[i], rSize, aligned_raddr, on_prefetch_complete, (void*) ctx);
  }

  mon.wait();
  db_->inflight_io_count_.fetch_sub(prefetch_num, std::memory_order_relaxed);
  // save the vbuf
  for (int i = 0; i < prefetch_num; i++) {
    val_list.push_back(std::string(vbuf_list[i], valSize_list[i]));
  }
  
  // de-allocate resources
  for (int i = 0 ; i < prefetch_num; i++) {
    free(aligned_val_buf_list[i]);
  }
  delete [] aligned_val_buf_list;
  delete [] vbuf_list;
  delete [] valSize_list;
  delete ctx;
}

DBIterator::DBIterator(DBImpl *db, const ReadOptions &options) 
: db_(db), options_(options), valid_(false), queue_cur_(0){
  
  rocksdb::ReadOptions rdopts;
  if (options_.upper_key != NULL) {
    upper_key_ = *(options_.upper_key); 
    rocksdb::Slice r_upper_key(upper_key_.data(), upper_key_.size());
    s_upper_key_ = r_upper_key;
    rdopts.iterate_upper_bound = &s_upper_key_;
//printf("upper key: %s\n", std::string(s_upper_key_.data(), s_upper_key_.size()).c_str());
  }

  // whether to prefetch?
  prefetch_ena_ = db_->options_.prefetchEnabled && (db_->inflight_io_count_.load(std::memory_order_relaxed) < db_->options_.prefetchReqThres);
  prefetch_depth_ = 1;
  if (prefetch_ena_) {
    int prefetch_depth = db_->options_.prefetchDepth;
    key_queue_ = new std::string[prefetch_depth];
    pkey_queue_ = new std::string[prefetch_depth];
    val_queue_ = new std::string[prefetch_depth];
    valid_queue_ = new bool[prefetch_depth];
    for (int i = 0 ; i < prefetch_depth; i++) {
      valid_queue_[i] = false;
      val_queue_[i].clear();
    }
  }

  it_ = db_->rdb_->NewIterator(rdopts);
}

DBIterator::~DBIterator() { 
  delete it_; 

  if (prefetch_ena_) {
    delete [] key_queue_;
    delete [] pkey_queue_;
    
    delete [] val_queue_;
    delete [] valid_queue_;
  }
}


void DBIterator::SeekToFirst() { 
  it_->SeekToFirst();
  if (prefetch_ena_) {
    valid_ = valid_queue_[0] = it_->Valid();
    if (it_->Valid())
      key_queue_[0] = it_->key().ToString();
      pkey_queue_[0] = it_->value().ToString();
  }
  else {
    valid_ = it_->Valid();
  }
}


void DBIterator::Seek(const Slice& target) { 
  RecordTick(db_->options_.statistics.get(), REQ_SEEK);
  rocksdb::Slice rocks_target(target.data(), target.size());
//printf("seek key: %s\n", std::string(target.data(), target.size()).c_str());
  // none phase
  it_->Seek(rocks_target); 
  if (prefetch_ena_) {
    valid_ = valid_queue_[0] = it_->Valid();
    if (valid_) {
      key_queue_[0] = it_->key().ToString();
      pkey_queue_[0] = it_->value().ToString();
    }
    // implicit next for prefetch
    assert(queue_cur_ == 0);
    
    for (int i = 1; i < prefetch_depth_; i++) {
      if (it_->Valid()) {
        it_->Next();
        if(it_->Valid()) {
          key_queue_[i] = (it_->key()).ToString();
          pkey_queue_[i] = (it_->value()).ToString();
          valid_queue_[i] = true;
        }
        else {
          valid_queue_[i] = false;
          break;
        }
      }
    }
  }
  else {
    valid_ = it_->Valid();
  }
}

void DBIterator::Prev() { /* NOT FULLY IMPLEMENT, Suppose ONLY CALL BEFORE next */ 
  assert(valid_);
  // std::string curr_key = it_->key().ToString();

  // do {
  //   it_->Prev();
  // } while (it_->Valid() && db_->options_.comparator->Compare(it_->key(), curr_key) >= 0);
  // valid_ = it_->Valid();
}

void DBIterator::Next() {
  RecordTick(db_->options_.statistics.get(), REQ_NEXT);
  if (prefetch_ena_) {
    if (queue_cur_ == prefetch_depth_-1) {
      queue_cur_ = 0; //reset cursor
      
      // calculate prefetch depth 
      if (prefetch_depth_ < db_->options_.prefetchDepth) {
        prefetch_depth_ = prefetch_depth_ == 0 ? 1 : prefetch_depth_ << 1;
      }

      for (int i = 0; i < prefetch_depth_; i++) {
        it_->Next();
        valid_ = it_->Valid();
        if(valid_) {
          key_queue_[i] = (it_->key()).ToString();
          pkey_queue_[i] = (it_->value()).ToString();
          valid_queue_[i] = true;
        }
        else {
          valid_queue_[i] = false;
          break;
        }
      }
    }
    else
      queue_cur_++;
    
    valid_ = valid_queue_[queue_cur_];
  }
  else {
    it_->Next();
    valid_ = it_->Valid();
  }
}

Slice DBIterator::key() const {
  

  if (prefetch_ena_) {
    return Slice(key_queue_[queue_cur_]);
  }
  else {
    rocksdb::Slice rocks_key = it_->key();
    Slice it_key(rocks_key.data(), rocks_key.size());
    return it_key;
  }
}

Slice DBIterator::value() {
  assert(valid_);

  if (prefetch_ena_) {
    if (queue_cur_ == 0) {// do prefetch_value
      std::vector<int> fd_list;
      std::vector<std::string> pkey_list;
      std::vector<std::string> val_list;

      for (int i = 0; i < prefetch_depth_; i++) {
        if(valid_queue_[i]) {
          uint64_t hash = Hash0(key_queue_[i])%LOG_PARTITION;
          fd_list.push_back(db_->logFD_[hash]);
          pkey_list.push_back(pkey_queue_[i]);
        }
        else break;
      }
      prefetch_value(fd_list, pkey_list, val_list);
      for (int i = 0; i < val_list.size(); i++) {
        val_queue_[i] = val_list[i];
      }

    }
    return val_queue_[queue_cur_];
  }
  else {
    Slice curr_key = key();
    rocksdb::Slice rocks_key(curr_key.data(), curr_key.size());
    // hash
    uint64_t hash = Hash0(curr_key)%LOG_PARTITION;
    std::string rocks_val;
    rocksdb::Status s = db_->rdb_->Get(rocksdb::ReadOptions(), rocks_key, &rocks_val);
    
    assert(s.ok() && rocks_val.size() == 12);
    char *p = &rocks_val[0];
    uint64_t logOffset= *((uint64_t *)p);
    p = p + 8;
    uint32_t valSize = *((uint32_t *)p);
    // calculate aligned read address
    uint64_t aligned_raddr = roundDown(logOffset, PAGE_SIZE);
    int rSize = roundUp(logOffset - aligned_raddr + valSize, PAGE_SIZE);
    char *aligned_val_buf = (char *)aligned_alloc(PAGE_SIZE, rSize);
    size_t rret = pread(db_->logFD_[hash], aligned_val_buf, rSize, aligned_raddr);
    // printf("[pread] offset: %lu, size: %lu, read: %d, errno: %d\n", aligned_raddr, rSize, rret, errno);

    value_.append(aligned_val_buf + logOffset - aligned_raddr, valSize);
    free(aligned_val_buf);
    assert(rret >= 0);
    
    return Slice(value_);
  }
  

}

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options) {
  return new DBIterator(db, options);
}

} // end namespace wisckey
