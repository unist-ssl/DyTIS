/*
Copyright 2023, The DyTIS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#pragma once
#include <stdio.h>
#include <boost/pool/pool_alloc.hpp>
#define DO_NOTHING
#define INITIAL_RANGE_BITS 1
#define RANGE_BITS_LIMIT 17
typedef struct Directory Directory;

struct LineFriends {
  double gradient;
  double y_intercept;
};
boost::pool_allocator<Directory> seg_alloc;

#ifdef SEP
boost::pool_allocator<Pair> pair_alloc;
boost::pool<> chunk_alloc[20] = {
  boost::pool<>(sizeof(Pair)*kNumSlot),
  boost::pool<>(sizeof(Pair)*kNumSlot*2),
  boost::pool<>(sizeof(Pair)*kNumSlot*3),
  boost::pool<>(sizeof(Pair)*kNumSlot*4),
  boost::pool<>(sizeof(Pair)*kNumSlot*5),
  boost::pool<>(sizeof(Pair)*kNumSlot*6),
  boost::pool<>(sizeof(Pair)*kNumSlot*7),
  boost::pool<>(sizeof(Pair)*kNumSlot*8),
  boost::pool<>(sizeof(Pair)*kNumSlot*9),
  boost::pool<>(sizeof(Pair)*kNumSlot*10),
  boost::pool<>(sizeof(Pair)*kNumSlot*11),
  boost::pool<>(sizeof(Pair)*kNumSlot*12),
  boost::pool<>(sizeof(Pair)*kNumSlot*13),
  boost::pool<>(sizeof(Pair)*kNumSlot*14),
  boost::pool<>(sizeof(Pair)*kNumSlot*15),
  boost::pool<>(sizeof(Pair)*kNumSlot*16),
  boost::pool<>(sizeof(Pair)*kNumSlot*17),
  boost::pool<>(sizeof(Pair)*kNumSlot*18),
  boost::pool<>(sizeof(Pair)*kNumSlot*19),
  boost::pool<>(sizeof(Pair)*kNumSlot*20)
};
#else
boost::pool_allocator<Pair> pair_alloc;
boost::pool<> chunk_alloc[20] = {
  boost::pool<>(sizeof(Pair)*kNumSlot),
  boost::pool<>(sizeof(Pair)*kNumSlot*2),
  boost::pool<>(sizeof(Pair)*kNumSlot*3),
  boost::pool<>(sizeof(Pair)*kNumSlot*4),
  boost::pool<>(sizeof(Pair)*kNumSlot*5),
  boost::pool<>(sizeof(Pair)*kNumSlot*6),
  boost::pool<>(sizeof(Pair)*kNumSlot*7),
  boost::pool<>(sizeof(Pair)*kNumSlot*8),
  boost::pool<>(sizeof(Pair)*kNumSlot*9),
  boost::pool<>(sizeof(Pair)*kNumSlot*10),
  boost::pool<>(sizeof(Pair)*kNumSlot*11),
  boost::pool<>(sizeof(Pair)*kNumSlot*12),
  boost::pool<>(sizeof(Pair)*kNumSlot*13),
  boost::pool<>(sizeof(Pair)*kNumSlot*14),
  boost::pool<>(sizeof(Pair)*kNumSlot*15),
  boost::pool<>(sizeof(Pair)*kNumSlot*16),
  boost::pool<>(sizeof(Pair)*kNumSlot*17),
  boost::pool<>(sizeof(Pair)*kNumSlot*18),
  boost::pool<>(sizeof(Pair)*kNumSlot*19),
  boost::pool<>(sizeof(Pair)*kNumSlot*20)
};
#endif
int pool_num = 20;
boost::pool<> line_alloc[10] = {
  boost::pool<>(sizeof(double)*2*2), // 2 ranges
  boost::pool<>(sizeof(double)*4*2), // 4 ranges
  boost::pool<>(sizeof(double)*8*2),
  boost::pool<>(sizeof(double)*16*2),
  boost::pool<>(sizeof(double)*32*2),
  boost::pool<>(sizeof(double)*64*2),
  boost::pool<>(sizeof(double)*128*2),
  boost::pool<>(sizeof(double)*256*2),
  boost::pool<>(sizeof(double)*512*2),
  boost::pool<>(sizeof(double)*1024*2)
};
int line_pool_num = 10;


struct Directory {
  Directory(void) {
    seg_num = 1;
#ifdef SEP
    if (seg_num <= pool_num) {
      void* addr = chunk_alloc[seg_num-1].malloc();
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
    else {
      void* addr = malloc(sizeof(Key)*seg_num*kNumSlot*2);
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
#else
    if (seg_num <= pool_num) {
      slot = new(static_cast<Pair*>(chunk_alloc[seg_num-1].malloc())) \
             Pair[seg_num*kNumSlot];
    }
    else
      slot = new Pair[seg_num*kNumSlot];
#endif
    remap_available = 1;
    // for local cdf
    line = NULL;
    range_bits = 0;
    sibling = NULL;
  }

  Directory(size_t ld) {
    seg_num = 1;
#ifdef SEP
    if (seg_num <= pool_num) {
      void* addr = chunk_alloc[seg_num-1].malloc();
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
    else {
      void* addr = malloc(sizeof(Key)*seg_num*kNumSlot*2);
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
#else
    if (seg_num <= pool_num) {
      slot = new(static_cast<Pair*>(chunk_alloc[seg_num-1].malloc())) \
             Pair[seg_num*kNumSlot];
    }
    else
      slot = new Pair[seg_num*kNumSlot];
#endif
    remap_available = 1;
    // for local cdf
    line = NULL;
    range_bits = 0;
    sibling = NULL;
  }

  Directory(size_t ld, int _num) {
    seg_num = _num;
#ifdef SEP
    if (seg_num <= pool_num) {
      void* addr = chunk_alloc[seg_num-1].malloc();
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
    else {
      void* addr = malloc(sizeof(Key)*seg_num*kNumSlot*2);
      key_slot = new(static_cast<Key*>(addr)) \
             Key[seg_num*kNumSlot];
      void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
      val_slot = new(static_cast<Value*>(val_addr))\
                 Value[seg_num*kNumSlot];
    }
#else
    if (seg_num <= pool_num) {
      slot = new(static_cast<Pair*>(chunk_alloc[seg_num-1].malloc())) \
             Pair[seg_num*kNumSlot];
    }
    else
      slot = new Pair[seg_num*kNumSlot];
#endif
    remap_available = seg_num;
    // for local cdf
    line = NULL;
    range_bits = 0;
    sibling = NULL;
  }
  ~Directory(void) {
#ifdef SEP
    if (seg_num <= pool_num) {
      chunk_alloc[seg_num-1].free(key_slot);
    }
    else {
      free(key_slot);
    }
#else
    if (seg_num <= pool_num)
      chunk_alloc[seg_num-1].free(slot);
    else
      delete[] slot;
#endif
    if (line != NULL) {
      if (range_bits <= 10)
        line_alloc[range_bits-1].free(line);
      else
        delete[] line;
    }
  }

  inline int Insert(Key_t&, Value_t, size_t, size_t);
  inline int Delete(Key_t&, size_t, size_t, bool, int);
  inline Directory* LocalRemap(size_t, int);
  inline Directory** Split(size_t, int);
  inline int find_lower(Key_t&, size_t);
  inline int exponential_search(Key_t&, size_t);
  inline int binary_search_upper_bound(int, int, Key_t, size_t);
  inline Value_t Get(Key_t&, size_t);
  inline void Scan(Key_t&, int&, size_t, size_t, Value_t*);
  inline Value_t* Find(Key_t&, size_t);
  inline bool Expand(int, int);
#ifdef SEP
  Key* key_slot;
  Value* val_slot;
  size_t seg_num;
#else
  Pair* slot;
  size_t seg_num;
  size_t local_depth; // 4B
#endif
  double remap_available = 1;

  // for localcdf
  int reclaim_flag = 0;
  int range_bits = 0;
  Directory* sibling = NULL;
  uint64_t num_key = 0; // the number of keys stored
  LineFriends* line = NULL;

  size_t data_size(void) {
    size_t size = sizeof(Directory);
    size += sizeof(Pair) * seg_num*kNumSlot;
    int ranges = (1 << range_bits);
    size += sizeof(double) * ranges; // line
    return size;
  }

  // for local cdf
  inline void split_local_cdf(Directory** split, int local_depth);
  inline int get_local_cdf_range (size_t key, int local_depth);
  inline int get_bucket_increase (int range, int local_depth);
  // reclaim gradient as much as bucket delta and give it to the range
  inline bool tuning_local_cdf (int range, int& needed_bucket, \
      std::vector<int>& range_count, int local_depth);
  inline int tuning_local_cdf_by_range(int needed_bucket, int range, int local_depth);
  inline size_t lcdf(int local_depth, size_t key);
  inline int divide_ranges_if_needed (uint64_t, int);
  inline void init_lcdf (int local_depth);
  inline double get_segment_util();
  inline int find_over_range(int z, int local_depth, Key_t* over_bucket);
  inline int find_over_range(int z, int local_depth);
};

