# A wisckey implementation using RocksDB

The implementation use RocksDB to store the user key to value log offset mapping.  The implementation can set up multiple value log files for better concurrency.  This work serves for comparison with [KVRangeDB][KVRangeDB repo] work on KVSSD.

# Build and run
## Build wisckey library
### Build rocksdb (v5.8) shared library
```bash
  export PRJ_HOME=$(pwd)
	# download rocksdb source code
	wget https://github.com/facebook/rocksdb/archive/refs/tags/v5.8.tar.gz
  tar -xzvf v5.8.tar.gz
  cd rocksdb-5.8/
  make shared_lib

	# copy librocks.so to project libs
	mkdir -p $PRJ_HOME/libs
	cp librocks.so* $PRJ_HOME/libs/
```
### Build wisckey shared library
```bash
  make
```
### Build test bench
```bash
  cd $PRJ_HOME/test
  make
```
### Run test bench
```bash
  LD_LIBRARY_PATH=$PRJ_HOME/libs ./test_rocks
  LD_LIBRARY_PATH=$PRJ_HOME/libs ./test_wisc
```


# YCSB binding

A simple java native interface (JNI) implementation with YCSB client is created for KVRangeDB. Please refer to the repo [ycsb-bindings][ycsb-bindings repo].

# Original wisckey paper

[Wisckey Fast'16][Wisckey]

[Wisckey]:https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu

[KVRangeDB repo]:https://github.com/celery1124/kvrangedb

[ycsb-bindings repo]:https://github.com/celery1124/ycsb-bindings
