
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#include <stdlib.h>
#include <stdio.h>

int main () {
    rocksdb::DB* rdb_;

    rocksdb::Options rocksOptions;
    rocksOptions.IncreaseParallelism();
    // rocksOptions.OptimizeLevelStyleCompaction();
    rocksOptions.create_if_missing = true;
    rocksOptions.max_open_files = 1000;
    rocksOptions.compression = rocksdb::kNoCompression;
    rocksOptions.paranoid_checks = false;
    rocksOptions.allow_mmap_reads = false;
    rocksOptions.allow_mmap_writes = false;
    rocksOptions.use_direct_io_for_flush_and_compaction = true;
    rocksOptions.use_direct_reads = true;
    rocksOptions.write_buffer_size = 2 << 20;
    rocksOptions.target_file_size_base = 2 * 1048576;
    rocksOptions.max_bytes_for_level_base = 2 * 1048576;

    // apply db options
    rocksdb::Status status = rocksdb::DB::Open(rocksOptions, "test_rocksdb", &rdb_);
    if (status.ok()) printf("rocksdb open ok\n");
    else printf("rocksdb open error\n");

    // write some records
    for (int i = 0 ; i < 1000000; i++) {
        char key[16] = {0};
        char val[128] = {0};
        sprintf(key, "%0*ld", 16 - 1, i);
        sprintf(val, "value%ld", i);
        rocksdb::Slice rkey(key, 16);
        rocksdb::Slice rval(val, 128);

        rdb_->Put(rocksdb::WriteOptions(), rkey, rval);
    }
    printf("finished load records\n");

    // update in iterator
    const rocksdb::ReadOptions options;
    rocksdb::Iterator *it = rdb_->NewIterator(options);
    it->SeekToFirst();

    int newv = 1000000;
    while (it->Valid()) {
        rocksdb::Slice key = it->key();
        rocksdb::Slice val = it->value();
        
        char newval[128] = {0};
        sprintf(newval, "value%ld", newv++);
        rocksdb::Slice rval(newval, 128);
        rdb_->Put(rocksdb::WriteOptions(), key, rval);
    }
    printf("finished update records through iterator\n");
    delete it;

    // read back updated value
    it = rdb_->NewIterator(options);
    it->SeekToFirst();
    while (it->Valid()) {
        rocksdb::Slice key = it->key();
        rocksdb::Slice val = it->value();
        printf("key %s, value %s\n", key.data(), val.data());
    }

    return 0;

}