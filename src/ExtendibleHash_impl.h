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
#include "src/ExtendibleHash.h"
inline int ExtendibleHash::Insert(Key_t& key, Value_t value, short global_depth) {

RETRY:
  size_t key_hash = key & y_mask;
  size_t y;
  if (global_depth == 0) y = 0;
  else y = (key_hash >> (8*sizeof(key_hash) -kDepth- global_depth));

  uint64_t local_depth = (uint64_t)seg[y] >> (64 - LOCAL_DEPTH_BITS);
  auto target = (Directory*)((uint64_t)seg[y] & ADDR_MASK);

  size_t local_mask;
  if (local_depth == 0)
    local_mask = 0;
  else
    local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;
  size_t masked_key_hash = key_hash & local_mask;
  size_t local_key_hash = target->lcdf(local_depth, masked_key_hash);
  auto z = (local_key_hash >> (64 - kDepth - local_depth));
  auto ret = target->Insert(key, value, key_hash, z);

  if (ret == -1) {
    // when LD < GD
    if (local_depth < global_depth && local_depth >= REMAP_THRE) {
      int PRACTICAL_MAX_SEG_NUM = max_bucket_num(local_depth);
      double seg_util = target->get_segment_util();
      if (seg_util < BUC_THRE) {
        if (target->line == NULL)
          target->init_lcdf(local_depth);
        if (target->remap_available > 0 && target->seg_num <= PRACTICAL_MAX_SEG_NUM) { // skewed in target segment
          target->divide_ranges_if_needed(masked_key_hash, local_depth);
          target->LocalRemap(masked_key_hash, local_depth);

          if (target->remap_available != -1) {
            goto RETRY;
          }

        }
      }
    }
    if (local_depth >= global_depth && global_depth >= REMAP_THRE) {
      int PRACTICAL_MAX_SEG_NUM = max_bucket_num(local_depth);

      double seg_util = target->get_segment_util();
      if (seg_util >= BUC_THRE) { // uniformly distributed in target segment
        bool expansion = target->Expand(local_depth, target->range_bits);
        if (expansion) { // expansion success
          goto RETRY;
        }
      } // high segment util condition done
      else  {// buc_util < BUC_THRE, meaning skewed in target segment and EH x
        if (target->line == NULL) {
          target->init_lcdf(local_depth);
        }
        if (target->remap_available > 0) {
          target->divide_ranges_if_needed(masked_key_hash, local_depth);
          target->LocalRemap(masked_key_hash, local_depth);
          if (target->remap_available != -1) {
            goto RETRY;
          }
        }
      }
    }


    Directory** s = target->Split(z, local_depth);
    s[1]->sibling = target->sibling;
    s[0]->sibling = s[1];
    int chunk_size = pow(2, global_depth - local_depth);
    int prev_y = y - (y % chunk_size) - 1;
    if (prev_y >= 0) {
      auto prev_seg = (Directory*)((uint64_t)seg[prev_y] & ADDR_MASK);
      prev_seg->sibling = s[0];
    }

    local_depth++;
    int PRACTICAL_MAX_SEG_NUM = max_bucket_num(local_depth);

    { // CRITICAL SECTION - directory update
      if (local_depth-1 < global_depth) {  // normal split
        unsigned depth_diff = global_depth - local_depth;
        uint64_t hidden_ld = local_depth << LOCAL_DEPTH_SHIFT;
        if (depth_diff == 0) {
          if (y%2 == 0) {
            seg[y+1] = hidden_ld + s[1];
            seg[y] = hidden_ld + s[0];
          } else {
            seg[y] = hidden_ld + s[1];
            seg[y-1] = hidden_ld + s[0];
          }
        } else {
          int chunk_size = pow(2, global_depth - (local_depth - 1));
          y = y - (y % chunk_size);
          for (unsigned i = 0; i < chunk_size/2; ++i) {
            seg[y+chunk_size/2+i] = hidden_ld + s[1];
          }
          for (unsigned i = 0; i < chunk_size/2; ++i) {
            seg[y+i] = hidden_ld + s[0];
          }
        }
      } else {  // directory doubling

        auto d = seg;
        uint64_t capacity = (pow(2, global_depth));
        Directory** _seg = new Directory*[capacity*2];

        for (unsigned i = 0; i < capacity; ++i) {
          if (i == y) {
            uint64_t hidden_ld = (uint64_t)local_depth << LOCAL_DEPTH_SHIFT;
            _seg[2*i] = hidden_ld + s[0];
            _seg[2*i+1] = hidden_ld + s[1];
          } else {
            _seg[2*i] = d[i];
            _seg[2*i+1] = d[i];
          }
        }
        delete[] seg;
        // update EH metadata after doubling
        seg = _seg;
        global_depth++;
      }
      seg_alloc.destroy(target);
      delete[] s;
     }  // End of critical section
    goto RETRY;
  }
  return global_depth;
}


inline bool ExtendibleHash::Delete(Key_t& key, short global_depth) {
RETRY_D:
  auto key_hash = key & y_mask;

  size_t y = (key_hash >> (8*sizeof(key_hash) - kDepth - global_depth));

  auto target = (Directory*)((uint64_t)seg[y] & ADDR_MASK);
  uint64_t local_depth = (uint64_t)seg[y] >> (64 - LOCAL_DEPTH_BITS);
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t) - kDepth - local_depth)) - 1;
  size_t local_key_hash = key_hash & local_mask;
  local_key_hash = target->lcdf(local_depth, local_key_hash);

  auto z = (local_key_hash >> (64 - kDepth - local_depth \
                               ));
  auto ret = target->Delete(key, key_hash, z, false, local_depth);
  if (ret == -1) {
    DO_NOTHING;
  } else if (ret == -2) {
    goto RETRY_D;
  } else {
    return true;
  }
}

inline Value_t ExtendibleHash::Get(Key_t& key, short global_depth) {
  auto key_hash = key & y_mask;
  size_t y = (key_hash >> (8*sizeof(key_hash) - kDepth - global_depth));
  auto target = (Directory*)((uint64_t)seg[y] & ADDR_MASK);
  uint64_t local_depth = (uint64_t)seg[y] >> (64 - LOCAL_DEPTH_BITS);
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t) - kDepth - local_depth)) - 1;
  size_t local_key_hash = key_hash & local_mask;
  local_key_hash = target->lcdf(local_depth, local_key_hash);
  auto z = (local_key_hash >> (64 - kDepth - local_depth \
                               ));
  return target->Get(key, z);
}

inline void ExtendibleHash::Scan(Key_t& key, int& count, size_t n, Value_t* result, short global_depth) {
  if (key != 0) {
    auto key_hash = key & y_mask;
    size_t y = (key_hash >> (8*sizeof(key_hash) - kDepth - global_depth));
    auto target = (Directory*)((uint64_t)seg[y] & ADDR_MASK);
    uint64_t local_depth = (uint64_t)seg[y] >> (64 - LOCAL_DEPTH_BITS);
    size_t local_mask = ((size_t)1 << (8*sizeof(Key_t) - kDepth - local_depth)) - 1;
    size_t local_key_hash = key_hash & local_mask;
    local_key_hash = target->lcdf(local_depth, local_key_hash);
    auto z = (local_key_hash >> (64 - kDepth - local_depth \
                                 ));
    target->Scan(key, count, n, z, result);
  }
  else { // Scan from first segment of the EH
    auto target = (Directory*)((uint64_t)seg[0] & ADDR_MASK);
    target->Scan(key, count, n, 0, result);
  }
}

inline Value_t* ExtendibleHash::Find(Key_t& key, short global_depth) {
  auto key_hash = key & y_mask;
  size_t y = (key_hash >> (8*sizeof(key_hash) - kDepth - global_depth));
  auto target = (Directory*)((uint64_t)seg[y] & ADDR_MASK);
  uint64_t local_depth = (uint64_t)seg[y] >> (64 - LOCAL_DEPTH_BITS);
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t) - kDepth - local_depth)) - 1;
  size_t local_key_hash = key_hash & local_mask;
  local_key_hash = target->lcdf(local_depth, local_key_hash);
  auto z = (local_key_hash >> (64 - kDepth - local_depth \
                               ));
  return target->Find(key, z);

}
