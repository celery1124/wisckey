# A wisckey implementation using RocksDB

The implementation use RocksDB to store the user key to value log offset mapping.  The implementation can set up multiple value log files for better concurrency.  This work serves for comparison with [KVRangeDB][KVRangeDB repo] work on KVSSD.

## Original wisckey paper

[Wisckey Fast'16][Wisckey]

[Wisckey]:https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu

[KVRangeDB repo]:https://github.com/celery1124/kvrangedb