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
#include "src/DyTIS.h"
#include "src/Directory_impl.h"
#include "src/ExtendibleHash_impl.h"


DyTIS::DyTIS(void)
{
  EH = new ExtendibleHash*[kCapacity];
  for (int i = 0; i < kCapacity; i++) {
    EH[i] = NULL;
  }
}


DyTIS::~DyTIS(void)
{
  delete[] EH;
}

inline void DyTIS::Insert(Key_t& key, Value_t value) {
  using namespace std;

  auto x = (key >> (8*sizeof(key) - kDepth));
  if (EH[x] == NULL) {
    int global_depth = 1;
    uint64_t capacity = (pow(2, global_depth));
    EH[x] = new ExtendibleHash(global_depth);
    for (int i = 0; i < capacity; ++i) {
      EH[x]->seg[i] = new(seg_alloc.allocate(1))Directory(global_depth);
      if (i > 0) {
        Directory* prev_seg = (Directory*)((uint64_t)EH[x]->seg[i-1] & ADDR_MASK);
        prev_seg->sibling = EH[x]->seg[i];
      }
      uint64_t hidden_ld = \
                (uint64_t) global_depth << LOCAL_DEPTH_SHIFT;
      EH[x]->seg[i] += hidden_ld;
    }
    uint64_t hidden_gd = (uint64_t) global_depth << ADDR_BITS;
    EH[x] = (ExtendibleHash*)((uint64_t)EH[x] + hidden_gd);
  }

  auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
  auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
  auto seg = target_EH->seg;
  int ret_global_depth = target_EH->Insert(key, value, global_depth);
  if (global_depth != ret_global_depth) {
    uint64_t hidden_gd = (uint64_t) (ret_global_depth - global_depth) << ADDR_BITS;
    EH[x] = (ExtendibleHash*)((uint64_t)EH[x] + hidden_gd);
    global_depth = ret_global_depth;
  }

  if (UNIFORM_TEST == false && global_depth >= (REMAP_THRE+2)) {
    UNIFORM_TEST = true;
    int seg_num=0;
    int expand=0;
    for (int i = 0; i < kCapacity; i++) {
      if (EH[i] == NULL)
        continue;
      auto temp_EH = (ExtendibleHash*)((uint64_t)EH[i] & ADDR_MASK);
      auto global_depth = (uint64_t)EH[i] >> ADDR_BITS;
      uint64_t capacity = (pow(2, global_depth));
      int count = 0;
      while (count < capacity) {
        auto target = (Directory*)((uint64_t)temp_EH->seg[count] & ADDR_MASK);
        uint64_t ld = (uint64_t)temp_EH->seg[count] >> (64 - LOCAL_DEPTH_BITS);
        int chunk_size = pow(2, global_depth - ld);
        count += chunk_size;
        seg_num++;
        if (target->line == NULL && target->seg_num > 1)
          expand++;
      }
    }
    if ((double)expand/seg_num > 0.1) {
      DEFAULT_MAX_BITS = UNIFORM_MAX_BITS;
    }
  }
}


inline bool DyTIS::Delete(Key_t& key) {
  auto x = (key >> (8*sizeof(key) - kDepth));
  if (EH[x] == NULL) return true;

  auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
  auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
  return target_EH->Delete(key, global_depth);
}

Value_t DyTIS::Get(Key_t& key) {
  auto x = (key >> (8*sizeof(key) - kDepth));
  if (EH[x] == NULL) return NONE;

  auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
  auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
  return target_EH->Get(key, global_depth);
}


inline Value_t* DyTIS::Scan(Key_t& key, size_t n) {

  Value_t* result = new Value_t[n];
  auto x = (key >> (8*sizeof(key) - kDepth));
  int count = 0;
  if (EH[x] != NULL) {
    auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
    auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
    target_EH->Scan(key, count, n, result, global_depth);
    if (count == n) {
      return result;
    }
  }
  Key_t k = 0; // only first EH need to compare key
  while (x < kCapacity) {
    if (EH[x] == NULL) {
      x++;
      continue;
    }
    auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
    auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
    target_EH->Scan(k, count, n, result, global_depth);
    if (count == n) {
      return result;
    }
    x++;
  }
  return result;
}

inline Value_t* DyTIS::Find(Key_t& key) {

  auto x = (key >> (8*sizeof(key) - kDepth));
  if (EH[x] == NULL) return NULL;

  auto target_EH = (ExtendibleHash*)((uint64_t)EH[x] & ADDR_MASK);
  auto global_depth = (uint64_t)EH[x] >> ADDR_BITS;
  return target_EH->Find(key, global_depth);

}


inline bool DyTIS::Update(Key_t& key, Value_t value) {
  Value_t* val = Find(key);
  if (val) {
    *val = value;
    return true;
  }
  return false;
}
