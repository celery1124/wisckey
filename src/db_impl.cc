/******* wisckey *******/
/* db_impl.cc
* 07/23/2019
* by Mian Qin
*/
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include "rocksdb/convenience.h"
#include "wisckey/db.h"
#include "wisckey/iterator.h"
#include "db_impl.h"
#include "db_iter.h"

namespace wisckey {

inline uint64_t Hash0 (const Slice& key) {
  return NPHash64(key.data(), key.size());
}

inline uint64_t Hash0 (std::string& key) {
  return NPHash64(key.data(), key.size());
}

DBImpl::DBImpl(const Options& options, const std::string& dbname) 
: options_(options), dbname_(dbname),
  inflight_io_count_(0), dbstats_(nullptr) {
  rocksdb::Options rocksOptions;
  rocksOptions.IncreaseParallelism();
  // rocksOptions.OptimizeLevelStyleCompaction();
  rocksOptions.create_if_missing = true;
  rocksOptions.max_open_files = options.maxOpenFiles;
  rocksOptions.compression = rocksdb::kNoCompression;
  rocksOptions.paranoid_checks = false;
  rocksOptions.allow_mmap_reads = false;
  rocksOptions.allow_mmap_writes = false;
  rocksOptions.use_direct_io_for_flush_and_compaction = true;
  rocksOptions.use_direct_reads = true;
  rocksOptions.write_buffer_size = 64 << 20;
  rocksOptions.target_file_size_base = 64 * 1048576;
  rocksOptions.max_bytes_for_level_base = 64 * 1048576;
  // dbstats_ = rocksdb::CreateDBStatistics();
  // rocksOptions.statistics = dbstats_;

  rocksdb::BlockBasedTableOptions table_options;
  if (options.filterType == Bloom) {
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(options.filterBitsPerKey, true));
    printf("Filter policy: Bloom %d bits per key\n", options.filterBitsPerKey);
  }
  // else if (options.filterType == Surf) {
  //   printf("Filter policy: Surf 2,4 %d bits per key\n", options.filterBitsPerKey);
  //   table_options.filter_policy.reset(rocksdb::NewSuRFPolicy(2, 4, true, options.filterBitsPerKey, true));
  // }
  table_options.block_size = options.indexBlockSize;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.cache_index_and_filter_blocks_with_high_priority = true;
  if (options.indexCacheSize > 0)
    table_options.block_cache = rocksdb::NewLRUCache((size_t)options.indexCacheSize * 1024 * 1024LL);
  else {
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    table_options.pin_l0_filter_and_index_blocks_in_cache = false;
    table_options.cache_index_and_filter_blocks_with_high_priority = false;
  } 
  rocksOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  // allocate write buffer 2 times larger than log buffer
  for (int i = 0; i < LOG_PARTITION; i++) {
    write_buffer_[i] = NewFIFOCache((size_t)options.logBufSize / LOG_PARTITION * 2 + 16384, 2);
  }
  if (options.dataCacheSize > 0) {
    cache_ = NewLRUCache((size_t)options.dataCacheSize << 20, -1);
  }
  else {
    cache_ = nullptr;
  }

  options.statistics.get()->setStatsDump(options.stats_dump_interval);

  // start thread pool for async i/o
  pool_ = threadpool_create(options.threadPoolThreadsNum, options.threadPoolQueueDepth, 0, &q_sem_);
  sem_init(&q_sem_, 0, options.threadPoolQueueDepth-1);

  // apply db options
  rocksdb::Status status = rocksdb::DB::Open(rocksOptions, dbname, &rdb_);
  if (status.ok()) printf("rocksdb open ok\n");
  else {
     std::string status_str=status.ToString();
     printf("rocksdb open error: %s\n", status_str.c_str());
     exit(-1);
  }
  // log buffer
  assert(options.logBufSize > 0);
  for (int i = 0; i < LOG_PARTITION; i++) {
    aligned_log_buf_[i] = NULL;
    log_buf_offset_[i] = 0;
    wal_buf_offset_[i] = 0;
    wal_flush_cnt_[i] = 0;
    // wal
    std::string dbWiskeyWAL = dbname+"/wiskeyWAL" + std::to_string(i) + ".log";
    walFD_[i] = fopen(dbWiskeyWAL.c_str(), "wb");

    if (options.logBufSize > 0) {
      aligned_log_buf_[i] = (char *)aligned_alloc(PAGE_SIZE, options.logBufSize/LOG_PARTITION);
    }

    std::string dbWiskeyLog = dbname+"/wiskey" + std::to_string(i) + ".log";
    if( access( dbWiskeyLog.c_str(), F_OK ) == 0 ) {
      logFD_[i] = open(dbWiskeyLog.c_str(), O_RDWR|O_APPEND|O_DIRECT|O_SYNC);
      size_t fileSize = lseek(logFD_[i], 0, SEEK_END);
      lba_[i] = fileSize;
      std::cout << "Open existing Wisckey log path:  " << dbWiskeyLog << " (" << logFD_[i] << ")" << std::endl;
    }
    else {
      logFD_[i] = open(dbWiskeyLog.c_str(), O_RDWR|O_CREAT|O_APPEND|O_DIRECT|O_SYNC, 0777);
      lba_[i] = 0;
      std::cout << "Create Wisckey log path:  " << dbWiskeyLog << " (" << logFD_[i] << ")" << std::endl;
    }
  }
}

DBImpl::~DBImpl() {
  // flush log buffer
  flushVLog();

  if (cache_) delete cache_;
  for (int i = 0; i < LOG_PARTITION; i++) {
    if (write_buffer_[i]) delete write_buffer_[i];
  }
  rocksdb::CancelAllBackgroundWork(rdb_, true);
  delete rdb_;

  // thread pool
  threadpool_destroy(pool_, 1);
  sem_destroy(&q_sem_);

  // if (log_buf_offset_) { // here log_buf_offset will not overflow (value size 4KB)
  //   printf("Flush log buffer (%lu, %d)\n", lba_, log_buf_offset_);
  //   pwrite(logFD_, aligned_log_buf_, log_buf_offset_, lba_);
  //   lba_ += log_buf_offset_;
  //   log_buf_offset_ = 0;
  // }

  for (int i = 0; i < LOG_PARTITION; i++) {
    fflush (walFD_[i]);
    fclose(walFD_[i]);

    if (aligned_log_buf_[i]) free(aligned_log_buf_[i]);
    fsync(logFD_[i]);
    fdatasync(logFD_[i]);
    close(logFD_[i]);
  }
  if (dbstats_) fprintf(stdout, "STATISTICS:\n%s\n", dbstats_->ToString().c_str());
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

Status DBImpl::Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) {

  RecordTick(options_.statistics.get(), REQ_PUT);
  // insert to in-memory cache
  std::string skey(key.data(), key.size());
  Cache::Handle* h = insert_cache(skey, value);
  release_cache_entry(h);

  // hash
  uint64_t hash = Hash0(key)%LOG_PARTITION;

  // WAL 
  // int key_len = key.size();
  // int val_len = value.size();
  // std::string wal_str;
  // wal_str.append((char*)&key_len, sizeof(int));
  // wal_str.append((char*)&val_len, sizeof(int));
  // wal_str.append(key.data(), key.size());
  // wal_str.append(value.data(), value.size());
  // {
  //   std::unique_lock<std::mutex> lock(walM_[hash]);
  //   fwrite(wal_str.data(), wal_str.size(), 1, walFD_[hash]);
  //   wal_buf_offset_[hash] += wal_str.size();
  //   if (wal_buf_offset_[hash] >= options_.walBufSize/LOG_PARTITION) {
  //     fflush (walFD_[hash]);
  //     wal_buf_offset_[hash] = 0;
  //     wal_flush_cnt_[hash]++;
  //   }
  //   if (wal_flush_cnt_[hash] >= WAL_FLUSH_CNT) {
  //     std::string dbWiskeyWAL = dbname_+"/wiskeyWAL" + std::to_string(hash) + ".log";
  //     fclose(walFD_[hash]);
  //     walFD_[hash] = fopen(dbWiskeyWAL.c_str(), "wb");
  //     wal_flush_cnt_[hash] = 0;
  //   }
  // }

  int wret;
  // int wSize = roundUp(value.size(), PAGE_SIZE);
  int wSize = value.size();
  uint64_t lba ;
  // write to write buffer first
  Cache::Handle* bh = insert_buffer(hash, skey, value);
  release_buffer_entry(hash, bh);
  // write value to log, make sure lseek and write atomic
  {
    std::unique_lock<std::mutex> lock(logM_[hash]);
    lba = lba_[hash] + log_buf_offset_[hash];
    memcpy(aligned_log_buf_[hash]+log_buf_offset_[hash], value.data(), wSize);
    log_buf_offset_[hash] += wSize;
    if (log_buf_offset_[hash] >= options_.logBufSize/LOG_PARTITION - PAGE_SIZE) { // here log_buf_offset will not overflow (value size 4KB)
      // pwrite needs to write PAGE_SIZE aligned buffer
      log_buf_offset_[hash] =  roundUp(log_buf_offset_[hash], PAGE_SIZE);
      wret = pwrite(logFD_[hash], aligned_log_buf_[hash], log_buf_offset_[hash], lba_[hash]);
      // fprintf(stderr, "pwrite fid: %d, size %lu, lba %lu, ret %d\n", logFD_[hash], log_buf_offset_[hash], lba_[hash], wret);
      // printf("pwrite size %lu, lba %lu, ret %d\n", log_buf_offset_[hash], lba_[hash], wret);
      lba_[hash] += log_buf_offset_[hash];
      log_buf_offset_[hash] = 0;
    }
  }

  // write key-offset record
  assert(lba >= 0);
  char metaVal[12];
  uint32_t valSize = value.size();
  memcpy(metaVal, &lba, sizeof(lba));
  memcpy(metaVal+sizeof(lba), &valSize, sizeof(valSize)); // not pretty!
  rocksdb::Slice rocks_key(key.data(), key.size());
  rocksdb::Slice rocks_val(metaVal, 12);
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = rdb_->Put(write_options, rocks_key, rocks_val);

  int wi_retry_cnt = 0;
  while (!s.ok()) {
    fprintf(stderr, "[rocks index put] err: %s\n", s.ToString().c_str());
    s = rdb_->Put(write_options, rocks_key, rocks_val);
    if (wi_retry_cnt++ >= 3) return Status().IOError(Slice());
  }
  assert(s.ok());
  
  // printf("[pwrite] offset: %lu, size: %lu, write: %d, errno: %d\n", lba, wSize, wret, errno);

  return Status();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  
  RecordTick(options_.statistics.get(), REQ_DEL);
  // simply update lsm-index (vlog GC manually)
  std::string skey(key.data(), key.size());
  erase_cache_entry(skey);
  
  uint64_t hash = Hash0(key)%LOG_PARTITION;
  erase_buffer_entry(hash, skey);

  rocksdb::WriteOptions write_options;
  rocksdb::Slice rocks_key(key.data(), key.size());
  rocksdb::Status s = rdb_->Delete(write_options, rocks_key);
  
  return Status();
}


Status DBImpl::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  
  RecordTick(options_.statistics.get(), REQ_GET);
  // read in-memory cache
  std::string skey(key.data(), key.size());
  Cache::Handle *h = read_cache(skey, value);
  if (h != NULL) { // hit in cache
      release_cache_entry(h);
      return Status();
  }
//printf("get key: %s\n", skey.c_str());
  // hash
  uint64_t hash = Hash0(key)%LOG_PARTITION;

  // read from device
  rocksdb::Slice rocks_key(key.data(), key.size());
  std::string rocks_val;
  rocksdb::Status s = rdb_->Get(rocksdb::ReadOptions(), rocks_key, &rocks_val);
  
  if (s.IsNotFound()) {
    RecordTick(options_.statistics.get(), REQ_GET_NOEXIST);
    return Status().NotFound(Slice());
  }
  int ri_retry_cnt = 0;
  while (!(s.ok())) { // read index retry, rare
    usleep(100);
    s = rdb_->Get(rocksdb::ReadOptions(), rocks_key, &rocks_val);
    uint64_t ino = *(uint64_t *)skey.data();
    fprintf(stderr, "[read index retry] key: %lx, %s, err: %s\n", ino, skey.data()+8, s.ToString().c_str());
    if (ri_retry_cnt++ >= 3) return Status().NotFound(Slice());
  }
  // check write buffer before reaching device if key exist
  Cache::Handle *bh = read_buffer(hash, skey, value);
  if (bh != NULL) { // hit in write buffer
      release_buffer_entry(hash, bh);
      return Status();
  }
  
  assert(s.ok() && rocks_val.size() == 12);
  char *p = &rocks_val[0];
  uint64_t logOffset= *((uint64_t *)p);
  p = p + 8;
  uint32_t valSize = *((uint32_t *)p);

  // calculate aligned read address
  uint64_t aligned_raddr = roundDown(logOffset, PAGE_SIZE);
  int rSize = roundUp(logOffset - aligned_raddr + valSize, PAGE_SIZE);
  char *aligned_val_buf = (char *)aligned_alloc(PAGE_SIZE, rSize);
  size_t rret = pread(logFD_[hash], aligned_val_buf, rSize, aligned_raddr);
  // fprintf(stderr, "[pread] fid: %d, offset: %lu, size: %lu, read: %d, errno: %d\n", logFD_[hash], aligned_raddr, rSize, rret, errno);
  // printf("[pread] offset: %lu, size: %lu, read: %d, errno: %d\n", aligned_raddr, rSize, rret, errno);

  int pr_retry_cnt = 0;
  while (rret <= 0) { // read retry (log buffer not flushed, something wrong with write buffer), rare
    usleep(100);
    rret = pread(logFD_[hash], aligned_val_buf, rSize, aligned_raddr);
    fprintf(stderr, "[pread retry] fid: %d, offset: %lu, size: %lu, read: %d, errno: %d\n", logFD_[hash], aligned_raddr, rSize, rret, errno);
    if (++pr_retry_cnt >= 3) return Status().NotFound(Slice());
  }
  assert(rret >= 0);
  value->append(aligned_val_buf + logOffset - aligned_raddr, valSize);
  free(aligned_val_buf);

  // insert to in-memory cache
  const Slice val(value->data(), value->size());
  h = insert_cache(skey, val);
  release_cache_entry(h);
  return Status();
}


void DBImpl::flushVLog() {
  int wret;
  for (int i = 0 ; i < LOG_PARTITION; i++)
  {
    std::unique_lock<std::mutex> lock(logM_[i]);
    if (log_buf_offset_[i]) {
      // pwrite needs to write PAGE_SIZE aligned buffer
      log_buf_offset_[i] =  roundUp(log_buf_offset_[i], PAGE_SIZE);
      wret = pwrite(logFD_[i], aligned_log_buf_[i], log_buf_offset_[i], lba_[i]);
      printf("Flush log buffer (%lu, %d)\n", lba_[i], log_buf_offset_[i]);
      lba_[i] += log_buf_offset_[i];
      log_buf_offset_[i] = 0;
    }
  }

  return;
}

void DBImpl::vLogGCWorker(int hash, std::vector<std::string> *ukey_list, std::vector<std::string> *vmeta_list, int idx, int size, int* oldLogFD, int* newLogFD) {

  for (int i = idx; i < idx+size; i++) {
    std::string ukey = (*ukey_list)[i];

    // read
    const char *p = (*vmeta_list)[i].data();
    uint64_t logOffset= *((uint64_t *)p);
    p = p + 8;
    uint32_t valSize = *((uint32_t *)p);
    // calculate aligned read address
    uint64_t aligned_raddr = roundDown(logOffset, PAGE_SIZE);
    int rSize = roundUp(logOffset - aligned_raddr + valSize, PAGE_SIZE);
    char *aligned_val_buf = (char *)aligned_alloc(PAGE_SIZE, rSize);
    size_t rret = pread(oldLogFD[hash], aligned_val_buf, rSize, aligned_raddr);
    assert(rret >= 0);
    char *validVal = aligned_val_buf + logOffset - aligned_raddr;

    // write to newlog
    int wret;
    uint64_t lba ;
    // write value to log, make sure lseek and write atomic
    {
      std::unique_lock<std::mutex> lock(logM_[hash]);
      lba = lba_[hash] + log_buf_offset_[hash];
      memcpy(aligned_log_buf_[hash]+log_buf_offset_[hash], validVal, valSize);
      log_buf_offset_[hash] += valSize;
      if (log_buf_offset_[hash] >= options_.logBufSize/LOG_PARTITION - PAGE_SIZE) { // here log_buf_offset will not overflow (value size 4KB)
        // pwrite needs to write PAGE_SIZE aligned buffer
        log_buf_offset_[hash] =  roundUp(log_buf_offset_[hash], PAGE_SIZE);
        wret = pwrite(newLogFD[hash], aligned_log_buf_[hash], log_buf_offset_[hash], lba_[hash]);
        lba_[hash] += log_buf_offset_[hash];
        log_buf_offset_[hash] = 0;
      }
    }

    // write key-offset record
    assert(lba >= 0);
    char metaVal[12];
    memcpy(metaVal, &lba, sizeof(lba));
    memcpy(metaVal+sizeof(lba), &valSize, sizeof(valSize)); // not pretty!
    rocksdb::Slice rocks_key(ukey.data(), ukey.size());
    rocksdb::Slice rocks_val(metaVal, 12);
    rocksdb::WriteOptions write_options;
    rocksdb::Status s = rdb_->Put(write_options, rocks_key, rocks_val);
    assert(s.ok());
    free(aligned_val_buf);
  }
}

// reclaim unused space from value log
// Just to be safe, read all live value metadata (offset, vsize) into memory through iterator, then update the lsm-tree
void DBImpl::vLogGarbageCollect() {

  auto t_start = std::chrono::high_resolution_clock::now();
  // flush vlog buffer first
  flushVLog();

  int newLogFD_[LOG_PARTITION];
  int oldLogFD_[LOG_PARTITION];
  for (int i = 0 ; i < LOG_PARTITION; i++) {
    close(logFD_[i]);
    // rename old vlog file
    std::string old_vlog = dbname_+ "/wiskey" + std::to_string(i) + ".log";
    std::string tmp_vlog = dbname_+ "/wiskey" + std::to_string(i) + ".tmp";
    int result= rename( old_vlog.c_str(), tmp_vlog.c_str() );
    if ( result == 0 )
      printf ( "File successfully renamed\n" );
    else {
      printf ( "Error renaming wisckey vlog from %s to %s\n", old_vlog.c_str(), tmp_vlog.c_str() );
      exit(-1);
    }

    oldLogFD_[i] = open(tmp_vlog.c_str(), O_RDWR|O_DIRECT|O_SYNC, 0777);
    // open new vlog
    newLogFD_[i] = open(old_vlog.c_str(), O_RDWR|O_CREAT|O_APPEND|O_DIRECT|O_SYNC, 0777);
    lba_[i] = 0;
    std::cout << "Create new Wisckey log path:  " << old_vlog << " (" << newLogFD_[i] << ")" << std::endl;
  }

  // read all valid keys
  const rocksdb::ReadOptions options;
  rocksdb::Iterator *it = rdb_->NewIterator(options);
  it->SeekToFirst();

  std::vector<std::string> ukey_list[LOG_PARTITION];
  std::vector<std::string> vmeta_list[LOG_PARTITION];
  while(it->Valid()) {
    rocksdb::Slice rocks_key = it->key();
    rocksdb::Slice rocks_val = it->value();
    // hash 
    std::string vkey = std::string(rocks_key.data(), rocks_key.size());
    uint64_t hash = Hash0(vkey) % LOG_PARTITION;

    assert(rocks_val.size() == 12);
    ukey_list[hash].push_back(std::string(rocks_key.data(), rocks_key.size()));
    vmeta_list[hash].push_back(std::string(rocks_val.data(), rocks_val.size()));
    it->Next();
  }
  delete it;
    
  // assign worker threads to move valid value to new vlog
  int gc_worker_num = options_.GCWorkerThreads; 
  std::thread **gc_worker_threads = new std::thread*[gc_worker_num]; 
  
  for (int p = 0; p < LOG_PARTITION; p++) {
    uint64_t remain_keys = ukey_list[p].size();
    int job_size = remain_keys / gc_worker_num;
    int job_start_idx = 0;
    for (int i = 0; i < gc_worker_num; i++) {
      int jSize = (i == (gc_worker_num - 1) ? remain_keys : job_size);
      gc_worker_threads[i] = new std::thread(&wisckey::DBImpl::vLogGCWorker, this, p, &ukey_list[p], &vmeta_list[p], job_start_idx, jSize, oldLogFD_, newLogFD_);
      job_start_idx += jSize;
      remain_keys -= jSize;
    }
    assert(remain_keys == 0);

    for (int i = 0; i < gc_worker_num; i++) {
      gc_worker_threads[i]->join();
      delete gc_worker_threads[i];
    }

    // clean up
    ukey_list[p].clear(); // reclaim memory
    vmeta_list[p].clear();

    fsync(oldLogFD_[p]);
    fdatasync(oldLogFD_[p]);
    close(oldLogFD_[p]);
    
    logFD_[p] = newLogFD_[p];
    std::string tmp_vlog = dbname_+ "/wiskey" + std::to_string(p) + ".tmp";
    if( remove( tmp_vlog.c_str() ) != 0 )
      printf( "Error deleting old value log %s\n", tmp_vlog.c_str() );
    else
      printf( "Old value log %s successfully deleted\n", tmp_vlog.c_str());

  }
  delete [] gc_worker_threads;
  
  flushVLog();

  auto t_end = std::chrono::high_resolution_clock::now();
  std::cout << std::fixed 
              << "GC Wall clock time passed: "
              << std::chrono::duration<double>(t_end-t_start).count()
              << " s\n";
  
  // rebuild cache after GC
  if(cache_) delete cache_;
  for (int i = 0; i < LOG_PARTITION; i++) {
    if (write_buffer_[i]) delete write_buffer_[i];
  }
  // allocate write buffer 2 times larger than log buffer
  for (int i = 0; i < LOG_PARTITION; i++) {
    write_buffer_[i] = NewFIFOCache((size_t)options_.logBufSize / LOG_PARTITION * 2 + 16384, 2);
  }
  if (options_.dataCacheSize > 0) {
    cache_ = NewLRUCache((size_t)options_.dataCacheSize << 20, -1);
  }
  else {
    cache_ = nullptr;
  }
};

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  return NewDBIterator(this, options);
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DB *db = new DBImpl(options, dbname);
  *dbptr = db;
  return Status(Status::OK());
}

}  // namespace wisckey

