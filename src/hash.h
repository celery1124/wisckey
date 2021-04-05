// Mian Qin
// 12/02/2019

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "wisckey/slice.h"
using namespace wisckey;

#define BIG_CONSTANT(x) (x##LLU)
uint64_t MurmurHash64A ( const void * key, int len, uint64_t seed );

inline uint64_t NPHash64(const char* data, size_t n) {
  return MurmurHash64A(data, n, 0);
}
inline uint64_t GetSliceNPHash64(const Slice& s) {
  return NPHash64(s.data(), s.size());
}
inline uint64_t GetSliceNPHash64(const char* data, size_t n) {
  return NPHash64(data, n);
}

