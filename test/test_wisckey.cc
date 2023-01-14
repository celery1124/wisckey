
#include "wisckey/db.h"
#include "wisckey/options.h"
#include "wisckey/slice.h"
#include "wisckey/status.h"
#include "wisckey/iterator.h"

#include <stdlib.h>
#include <stdio.h>

#define TOTAL_RECORDS 1000

int main () {
    wisckey::DB* db_;
    wisckey::Options options_;
    options_.statistics = wisckey::Options::CreateDBStatistics();

    // apply db options
    wisckey::Status status = wisckey::DB::Open(options_, "test_wisckey", &db_);
    if (status.ok()) printf("wisckey open ok\n");
    else printf("wisckey open error\n");

    // write some records
    for (int i = 0 ; i < TOTAL_RECORDS; i++) {
        char key[16] = {0};
        char val[128] = {0};
        sprintf(key, "%0*ld", 16 - 1, i);
        sprintf(val, "value%ld", i);
        wisckey::Slice rkey(key, 16);
        wisckey::Slice rval(val, 128);

        db_->Put(wisckey::WriteOptions(), rkey, rval);

        std::string gval;
        db_->Get(wisckey::ReadOptions(), rkey, &gval);
        printf("key %s, value %s\n", rkey.data(), gval.c_str());

    }
    printf("finished load records\n");

    // update in iterator
    const wisckey::ReadOptions options;
    wisckey::Iterator *it = db_->NewIterator(options);
    it->SeekToFirst();

    int newv = TOTAL_RECORDS;
    while (it->Valid()) {
        wisckey::Slice key = it->key();
        wisckey::Slice val = it->value();
        
        char newval[128] = {0};
        sprintf(newval, "value%ld", newv++);
        wisckey::Slice rval(newval, 128);
        db_->Put(wisckey::WriteOptions(), key, rval);
        it->Next();
    }
    printf("finished update records through iterator\n");
    delete it;

    db_->flushVLog();

    // read back updated value
    it = db_->NewIterator(options);
    it->SeekToFirst();
    while (it->Valid()) {
        wisckey::Slice key = it->key();
        wisckey::Slice val = it->value();
        printf("key %s, value %s\n", key.data(), val.data());
        it->Next();
    }

    return 0;

}