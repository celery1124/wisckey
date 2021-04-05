/******* wisckey *******/
/* db_iter.cc
* 08/06/2019
* by Mian Qin
*/
#include <mutex>
#include <iostream>
#include <condition_variable>
#include <unordered_map>
#include "wisckey/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

namespace wisckey {
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

};

DBIterator::DBIterator(DBImpl *db, const ReadOptions &options) 
: db_(db), options_(options), valid_(false){
  
  rocksdb::ReadOptions rdopts;
  if (options_.upper_key != NULL) {
    upper_key_ = *(options_.upper_key); 
    rocksdb::Slice r_upper_key(upper_key_.data(), upper_key_.size());
    s_upper_key_ = r_upper_key;
    rdopts.iterate_upper_bound = &s_upper_key_;
//printf("upper key: %s\n", std::string(s_upper_key_.data(), s_upper_key_.size()).c_str());
  }

  it_ = db_->rdb_->NewIterator(rdopts);
}

DBIterator::~DBIterator() { 
  delete it_; 
}


void DBIterator::SeekToFirst() { 
  it_->SeekToFirst();

  valid_ = it_->Valid();
}


void DBIterator::Seek(const Slice& target) { 
  RecordTick(db_->options_.statistics.get(), REQ_SEEK);
  rocksdb::Slice rocks_target(target.data(), target.size());
//printf("seek key: %s\n", std::string(target.data(), target.size()).c_str());
  // none phase
  it_->Seek(rocks_target); 
  valid_ = it_->Valid();
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
  it_->Next();
  valid_ = it_->Valid();
}

Slice DBIterator::key() const {
  rocksdb::Slice rocks_key = it_->key();
  Slice ret(rocks_key.data(), rocks_key.size());
  return ret;
}

Slice DBIterator::value() {
  assert(valid_);
  
  value_.clear();

  rocksdb::Slice rocks_key = it_->key();
  // hash
  uint64_t hash = Hash0(rocks_key)%LOG_PARTITION;
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

Iterator* NewDBIterator(DBImpl *db, const ReadOptions &options) {
  return new DBIterator(db, options);
}

} // end namespace wisckey
