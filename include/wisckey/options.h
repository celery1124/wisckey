/******* wisckey *******/
/* options.h
* 07/23/2019
* by Mian Qin
*/

#ifndef _options_h_
#define _options_h_


#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <string>

#include "wisckey/slice.h"
#include "wisckey/statistics.h"

namespace wisckey {

class Comparator;
class Slice;

enum FilterType {
  NoFilter,
  Bloom,
  Surf
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
  // -------------------
  // Parameters that affect behavior

  // Index cache size in MB (currently only support LSM)
  // Default: 128MB
  int indexCacheSize;
  
  // Whether enable value prefetch for iterators
  // Default: false
  bool prefetchEnabled;

  // Prefetch buffer size
  // Default: 64
  int prefetchDepth;

  // Prefetch total request threshold
  // Default: 128
  int prefetchReqThres;

  // Whether enable range filter for LSM index
  // Default: false
  bool rangefilterEnabled;

  // Helper record hint
  int helperHint;
  int helperTrainingThres;

  // Pack size for physical KV
  // Default: 4096
  int packSize;

  // Value size threshold for packing
  // Default: 4096
  int packThres;

  // Max number of KVs for packing
  // Default: 8
  int maxPackNum;

  // Timeout for packing thread dequeue in us
  // Default: 5000
  int packDequeueTimeout;

  // Depth of the packed KV queue
  // Default: 1024
  int packQueueDepth;

  // Number of threads to write packed KV
  // Default: 8
  int packThreadsNum;

  // Disable BG packing threads
  // Default: false
  bool packThreadsDisable;

  // Manual compaction
  // Default: false
  bool manualCompaction;

  // Background compaction
  // Default: false
  bool bgCompaction;

  // Background compaction interval (sec)
  // Default: 10
  bool bgCompactionInterval;

  // Background compaction scan length
  // Default: 100000
  bool bgCompactionScanLength;

  // Hot key training count
  // Default: 1000000
  int hotKeyTrainingNum;

  // Filter Type
  // Default: None
  FilterType filterType;

  // Bits per key for bloom filter
  // Default: 8
  int filterBitsPerKey;

  // In-memory data cache size
  // Default: 16 (MB)
  int dataCacheSize;

  // Log buffer size
  // Default: 16 (MB)
  size_t logBufSize;

  // WAL flush buffer size
  // Default: 1 (MB)
  size_t walBufSize;

  // GC worker threads
  // Default: 16
  int GCWorkerThreads;

  // Statistic (create to record count)
  // Default: NULL
  std::shared_ptr<Statistics> statistics;

  // Statistic dump interval in seconds
  // Default: -1 (no dump)
  int stats_dump_interval;

  Options() : indexCacheSize(128),
              prefetchEnabled(false),
              prefetchDepth(64),
              prefetchReqThres(128),
              rangefilterEnabled(false),
              helperHint(0),
              helperTrainingThres(10),
              packSize(4096),
              packThres(4096),
              maxPackNum(8),
              packDequeueTimeout(5000),
              packQueueDepth(1024),
              packThreadsNum(8),
              packThreadsDisable(false),
              manualCompaction(false),
              bgCompaction(false),
              bgCompactionInterval(10),
              bgCompactionScanLength(100000),
              hotKeyTrainingNum(1000000),
              filterType(NoFilter),
              filterBitsPerKey(8),
              dataCacheSize(16),
              logBufSize(16<<20),
              walBufSize(1<<20),
              GCWorkerThreads(16),
              statistics(nullptr),
              stats_dump_interval(-1) { }

  static std::shared_ptr<Statistics> CreateDBStatistics() {
    printf("Wisckey Statistics Created\n");
    return std::make_shared<Statistics>();
  } 
};

// Options that control read operations
struct ReadOptions {
  // Define the upper key (Non-Inclusive) for range query
  // Default: NULL
  Slice* upper_key;

  // Potential user hint for the length of a scan (how many next after seek?)
  // Default: 1 (adptively increase)
  int scan_length;

  // // Buffer size for base iterator in Bytes
  // // Default: 4MB
  // int base_iter_buffer_size;

  ReadOptions()
      : upper_key(NULL),
        scan_length(1) {
  }
};

// Options that control write operations
struct WriteOptions {
  // From LevelDB write options, currently we don't use this
  // Default: false
  bool sync;
  // Write Index in batch
  // Default: false
  bool batchIDXWrite;
  // Batch size for batch index write
  // Default: 8
  size_t batchIDXSize;

  WriteOptions()
      : sync(false),
        batchIDXWrite(false),
        batchIDXSize(8) {
  }
};

}  // namespace wisckey

#endif
