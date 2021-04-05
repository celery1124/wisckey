# A wisckey implementation using RocksDB

The implementation use RocksDB to store the user key to value log offset mapping.  The implementation can set up multiple value log files for better concurrency.  This work serves for comparison with [KVRangeDB][KVRangeDB repo] work on KVSSD.

# YCSB binding

A simple java native interface (JNI) implementation with YCSB client is created for KVRangeDB. Please refer to the repo [ycsb-bindings][ycsb-bindings repo].

# Original wisckey paper

[Wisckey Fast'16][Wisckey]

[Wisckey]:https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu

[KVRangeDB repo]:https://github.com/celery1124/kvrangedb

[ycsb-bindings repo]:https://github.com/celery1124/ycsb-bindings
