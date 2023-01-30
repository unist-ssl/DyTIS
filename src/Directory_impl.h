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
#include "src/Directory.h"

inline Directory* Directory::LocalRemap(size_t key, int local_depth) {
  if (remap_available == -1) {
    return NULL;
  }
  assert(line != NULL);
  int ranges = (1 << range_bits);
  bool fixed = true;
  if (ranges >= seg_num && get_segment_util() > 0.1) {
    remap_available = 0;
    fixed = false;
  }
  static std::vector<double> old_line;
  for (int i = 0; i < ranges; i++) {
    old_line.push_back(line[i].gradient);
    old_line.push_back(line[i].y_intercept);
  }
  int available = remap_available;
  auto local_key_hash = lcdf(local_depth, key);
  auto z = (local_key_hash >> (64 - kDepth - local_depth));
  int snum = seg_num;
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;
  uint64_t over_key = key & local_mask; // may be key is already masked
  uint64_t first_over_key = over_key;
  auto seg = kNumSlot;
  int over_range;
  int over_buc = z;
  uint64_t one_bucket = (
      (uint64_t)1 << (64 - kDepth - local_depth));
  int buc_idx = 0;
  int buc_num = 0;
  static std::vector<int> range_count;
  if (fixed)
    range_count.assign(reclaim_flag, block+1);

  int buffer = 0;

  uint64_t limit_stride = ((size_t)1 << (64 - kDepth -local_depth));
  uint64_t  one_range = limit_stride / ranges;
  over_range = find_over_range(z, local_depth);
  if (fixed == true) { // it means this local remap includes reclaim process
    uint64_t first_key = lcdf(local_depth, (reclaim_flag*one_range));
    int iter_range = reclaim_flag; // start range
    int iter_bucket = (first_key >> (64 - kDepth -local_depth));
    int iter_index = 0;
    int range_num = 0;
    while (iter_bucket < seg_num) {
#ifdef SEP
      if (key_slot[iter_bucket*block+iter_index].item == INVALID) {
#else
      if (slot[iter_bucket*block+iter_index].key == INVALID) {
#endif
        iter_bucket++;
        iter_index = 0;
        continue;
#ifdef SEP
      }
      uint64_t key_hash = key_slot[iter_bucket*block+iter_index].item & local_mask;
#else
      }
      uint64_t key_hash = slot[iter_bucket*block+iter_index].key & local_mask;
#endif
      int range = get_local_cdf_range(key_hash, local_depth);
      if (range == iter_range)
        range_num++;
      if (range > iter_range) {
        for (int i = iter_range; i < range; i++) {
          range_count.push_back(range_num);
          range_num = 0;
        }
        iter_range = range;
        range_num++;
      }
      iter_index++;
      if (iter_index == block) {
        iter_bucket++;
        iter_index = 0;
      }
    }
    for (int i = iter_range; i < ranges; i++) {
      range_count.push_back(range_num);
      range_num = 0;
    }
  } // End of fixed

  // while bucket need to split (remap)
  do {
    size_t key_hash;
    int pre_seg_index = 0;
    int pre_buc_index = 0;

    // check if cdf is over
    if (remap_available > 0 || (remap_available == 0 && !fixed)) {
      uint64_t min_over_range = over_range*one_range;
      first_over_key = min_over_range;
      uint64_t tlocal_key_hash = lcdf(local_depth, first_over_key);
      auto temp_z = (tlocal_key_hash >> \
          (64 - kDepth - local_depth));
      over_buc = temp_z;
      int needed_bucket = get_bucket_increase(over_range, local_depth);
      if (fixed) { // fixed && reclaim
        bool remap_done = tuning_local_cdf(over_range,
                                        needed_bucket, range_count, local_depth);
        available = remap_available;
        if (remap_done == false) {
          fixed = false;
        }
      }
      if (!fixed) { // !fixed && !reclaim
        available = tuning_local_cdf_by_range(needed_bucket, over_range, local_depth);
      }

      if (available <= 0) { //local remap fail during tuning
        remap_available = available;
        for (int i = 0; i < ranges; i++) {
          line[i].gradient = old_line[2*i];
          line[i].y_intercept = old_line[2*i+1];
        }
        old_line.clear();
        range_count.clear();
        return NULL;
      }
    }
    else { //local remap fail (already)
      for (int i = 0; i < ranges; i++) {
        line[i].gradient = old_line[2*i];
        line[i].y_intercept = old_line[2*i+1];
      }
      old_line.clear();
      range_count.clear();
      return NULL;
    }

    if (snum != available) { // snum changed
      snum = available;
    }
    Key_t temp_bucket[block];
    buc_idx = 0;
    buc_num = 0;
    //int s_bucket = min (over_buc, reclaim_flag);
    for (int k = 0; k < seg_num; k++) { // for each bucket
      for (unsigned i = block*k; i < block*(k+1); ++i) {
#ifdef SEP
        if (key_slot[i].item == INVALID)
          break;
        key_hash = key_slot[i].item & y_mask;
#else
        if (slot[i].key == INVALID)
          break;
        key_hash = slot[i].key & y_mask;
#endif

        key_hash = key_hash & local_mask;
        local_key_hash = lcdf(local_depth, key_hash);
        z = (local_key_hash >> (64 - kDepth - local_depth));
        int range = get_local_cdf_range(key_hash, local_depth);
        if (buc_idx != z) {
          buc_idx = z;
          buc_num = 0;
          first_over_key = key_hash;
        }
#ifdef SEP
        temp_bucket[buc_num++] = key_slot[i].item;
#else
        temp_bucket[buc_num++] = slot[i].key;
#endif

        if (buc_num >= (block-buffer)) {
          over_key = key_hash;
          uint64_t tlocal_key_hash = lcdf(local_depth, first_over_key);
          auto temp_z = (
              tlocal_key_hash >> (64 - kDepth - local_depth));
          over_buc = temp_z;
          int prev_range;
          int prev_count = 0;
          int count = 0;
          for (int k = 0; k < (block-buffer); k++) {
            uint64_t tkey_hash = temp_bucket[k] & y_mask;
            tkey_hash = tkey_hash & local_mask;
            int range = get_local_cdf_range(tkey_hash, local_depth);
            if ((k == 0) || (k == (block-buffer-1)) || (range != prev_range)) {
              if ((k == 0) || (prev_count < count)) {
                prev_count = count;
                over_range = prev_range;
              }
              prev_range = range;
              count = 0;
            }
            count++;
          }
          break;
        }
      }
      if (buc_num >= (block-buffer)) {
        break;
      }
    } // End of for loop for each bucket
  } // End of while bucket need to split (remap)
  while (buc_num >= (block-buffer));
#ifdef SEP
  Key* temp_key_slot;
  Value* temp_val_slot;
  if (snum <= pool_num) {
    void* addr = chunk_alloc[snum-1].malloc();
    temp_key_slot = new(static_cast<Key*>(addr)) \
           Key[snum*kNumSlot];
    void* val_addr = addr + sizeof(Key) * snum * kNumSlot;
    temp_val_slot = new(static_cast<Value*>(val_addr))\
               Value[snum*kNumSlot];
  }
  else {
    void* addr = malloc(sizeof(Key) * snum * kNumSlot * 2);
    temp_key_slot = new(static_cast<Key*>(addr)) \
           Key[snum*kNumSlot];
    void* val_addr = addr + sizeof(Key) * snum * kNumSlot;
    temp_val_slot = new(static_cast<Value*>(val_addr))\
               Value[snum*kNumSlot];
  }
#else
  Pair* temp_slot;
  if (snum <= pool_num) {
    temp_slot = new(static_cast<Pair*>(chunk_alloc[snum-1].malloc())) \
           Pair[snum*kNumSlot];
  }
  else
    temp_slot = new Pair[snum*kNumSlot];
#endif
  buc_idx = 0;
  buc_num = 0;
#ifdef SEP
  for (uint32_t k = 0; k < seg_num; k++) { // for each bucket
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (key_slot[i].item == INVALID)
        break;
      uint64_t key_hash = key_slot[i].item & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = lcdf(local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - local_depth));
      if (buc_idx != z) {
        buc_idx = z;
        buc_num = 0;
      }
      temp_val_slot[z*block + buc_num].item = val_slot[i].item;
      temp_key_slot[z*block + buc_num++].item = key_slot[i].item;
    }
  }

  // copy remapped data
  remap_available = available;
  if (seg_num <= pool_num) {
    chunk_alloc[seg_num-1].free(key_slot);
  }
  else {
    free(key_slot);
  }
  seg_num = snum;
  key_slot = temp_key_slot;
  val_slot = temp_val_slot;
#else
  for (uint32_t k = 0; k < seg_num; k++) { // for each bucket
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (slot[i].key == INVALID)
        break;
      uint64_t key_hash = slot[i].key & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = lcdf(local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - local_depth));
      if (buc_idx != z) {
        buc_idx = z;
        buc_num = 0;
      }
      temp_slot[z*block + buc_num].value = slot[i].value;
      temp_slot[z*block + buc_num++].key = slot[i].key;
    }
  }

  // copy remapped data
  remap_available = available;
  if (seg_num <= pool_num)
    chunk_alloc[seg_num-1].free(slot);
  else
    delete[] slot;
  //pair_alloc.deallocate(slot, seg_num*kNumSlot);
  seg_num = snum;
  slot = temp_slot;
#endif
  old_line.clear();
  range_count.clear();
  return this;

}

inline int Directory::Insert(Key_t& key, Value_t value, size_t key_hash, size_t y) {

  int ret = 1;

  auto bucket = block*y; // which number of block
#ifdef SEP
  for (int i = 0; i < block; i++) {
    if (key > key_slot[bucket+i].item) {
      continue;
    }
    if (key == key_slot[bucket+i].item) {
      val_slot[bucket+i].item = value;
      return i;
    }
    if (key_slot[bucket+i].item == INVALID) {
      key_slot[bucket+i].item = key;
      val_slot[bucket+i].item = value;
      num_key++;
      return i;
    }
    // insert new key and copy larger key to behind
    if (key < key_slot[bucket+i].item) {
      // this segment is already full so no enough space
      if (key_slot[bucket+block-1].item != INVALID)
        return -1;
      int count = 0;
      for (int j = i; j < block; j++) {
        if (key_slot[bucket+j].item == INVALID)
          break;
        count++;
      }

      if (count != 0) {
        memmove(key_slot+bucket+i+1, key_slot+bucket+i, sizeof(Key)*count);
        memmove(val_slot+bucket+i+1, val_slot+bucket+i, sizeof(Value)*count);

      }

      key_slot[bucket+i].item = key;
      val_slot[bucket+i].item = value;
      num_key++;
      return i;
    }
  }
#else
  for (int i = 0; i < block; i++) {
    if (key > slot[bucket+i].key) {
      continue;
    }
    if (key == slot[bucket+i].key) {
      slot[bucket+i].value = value;
      return i;
    }
    if (slot[bucket+i].key == INVALID) {
      slot[bucket+i].key = key;
      slot[bucket+i].value = value;
      num_key++;
      return i;
    }
    // insert new key and copy larger key to behind
    if (key < slot[bucket+i].key) {
      // this segment is already full so no enough space
      if (slot[bucket+block-1].key != INVALID)
        return -1;
      int count = 0;
      for (int j = i; j < block; j++) {
        if (slot[bucket+j].key == INVALID)
          break;
        count++;
      }

      if (count != 0) {
        memmove(slot+bucket+i+1, slot+bucket+i, sizeof(Pair)*count);
      }

      slot[bucket+i].key = key;
      slot[bucket+i].value = value;
      num_key++;
      return i;
    }
  }
#endif
  return -1;
}

inline int Directory::Delete(Key_t& key, size_t key_hash, size_t y, bool islock, int local_depth) {

  int ret = 1;

  auto bucket = block*y;

  bool shift = false;
#ifdef SEP
  for (int i = 0; i < block; i++) {
    if (key_slot[bucket+i].item == INVALID)
      break;
    if (!shift && key_slot[bucket+i].item == key) {
      val_slot[bucket+i].item = (i == 0) ? NULL : val_slot[bucket+i-1].item;
      shift = true;
      num_key--;
    }
    if (shift) {
      key_slot[bucket+i].item = key_slot[bucket+i+1].item;
      val_slot[bucket+i].item = val_slot[bucket+i+1].item;
    }
  }
  if (key_slot[bucket].item == 0) {
    return -1;
  }
#else
  for (int i = 0; i < block; i++) {
    if (slot[bucket+i].key == INVALID)
      break;
    if (!shift && slot[bucket+i].key == key) {
      slot[bucket+i].value = (i == 0) ? NULL : slot[bucket+i-1].value;
      shift = true;
      num_key--;
    }
    if (shift) {
      slot[bucket+i].key = slot[bucket+i+1].key;
      slot[bucket+i].value = slot[bucket+i+1].value;
    }
  }
  if (slot[bucket].key == 0) {
    return -1;
  }
#endif
  return 0;
}

inline Directory** Directory::Split(size_t y, int local_depth) {

  int sum = 0, sum1 = 0;
  auto seg = kNumSlot;
  Directory** split = new Directory*[2];
  split_local_cdf(split, local_depth);

  int buc_index[2] = {0, 0};
  int buc_num[2] = {0, 0};
  uint64_t half = ((uint64_t)1 << (8*sizeof(Key_t)-kDepth-local_depth-1));
  uint64_t bound_hash = lcdf(local_depth, half); //remap global remapped key as local remapped
  auto bound_buc = (bound_hash >> (64 - kDepth - local_depth));
  int next_local_depth = local_depth + 1;
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-next_local_depth))-1;
  size_t original_local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;
  uint64_t key_hash, local_key_hash;
  int z;
  int bound1 = 0;
#ifdef SEP
  for (unsigned i = block*bound_buc; i < block*(bound_buc+1); ++i) {
    if (key_slot[i].item == INVALID)
      break;
    auto key_hash = key_slot[i].item & y_mask;
    uint64_t split_test = key_hash >> (8*sizeof(Key_t)-kDepth-local_depth-1);
    split_test = split_test & 1;
    size_t local_key_hash = key_hash & local_mask;
    local_key_hash = split[split_test]->lcdf(next_local_depth, local_key_hash);
    auto z = local_key_hash >> (64 - kDepth - next_local_depth);
    if (split_test == 1) {
      assert(z*block+ buc_num[1] < split[1]->seg_num*kNumSlot);
      if (z != buc_index[1]) {
        buc_index[1] = z;
        buc_num[1] = 0;
      }
      split[1]->val_slot[z*block + buc_num[1]].item = val_slot[i].item;
      split[1]->key_slot[z*block + (buc_num[1]++)].item = key_slot[i].item;
      bound1++;
      split[1]->num_key++;
    }
  }

  for (unsigned k = bound_buc+1; k < seg_num; k++) {
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (key_slot[i].item == INVALID)
        break;
      key_hash = key_slot[i].item & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = split[1]->lcdf(next_local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[1]) {
        buc_index[1] = z;
        buc_num[1] = 0;
      }
      assert(buc_num[1] < block);
      assert(z*block+ buc_num[1] < split[1]->seg_num*kNumSlot);

      split[1]->val_slot[z*block+ buc_num[1]].item = val_slot[i].item;
      split[1]->key_slot[z*block + (buc_num[1]++)].item = key_slot[i].item;
      split[1]->num_key++;
    }
  }


  for (unsigned k = 0; k < bound_buc; k++) {
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (key_slot[i].item == INVALID)
        break;
      key_hash = key_slot[i].item & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = split[0]->lcdf(next_local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[0]) {
        buc_index[0] = z;
        buc_num[0] = 0;
      }
      assert(buc_num[0] < block);
      assert(z*block+ buc_num[0] < split[0]->seg_num*kNumSlot);
      split[0]->val_slot[z*block + buc_num[0]].item = val_slot[i].item;
      split[0]->key_slot[z*block + (buc_num[0]++)].item = key_slot[i].item;
      split[0]->num_key++;
    }
  }
    int bound0 = 0;
    for (unsigned i = block*bound_buc; i < block*(bound_buc+1); ++i) {
      if (key_slot[i].item == INVALID)
        break;
      auto key_hash = key_slot[i].item & y_mask;
      uint64_t split_test = key_hash >> (8*sizeof(Key_t)-kDepth-local_depth-1);
      split_test = split_test & 1;
      size_t local_key_hash = key_hash & local_mask;
      local_key_hash = split[split_test]->lcdf(next_local_depth, local_key_hash);
      auto z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[0]) {
        buc_index[0] = z;
        buc_num[0] = 0;
      }
      if (split_test == 0) {
        assert(buc_num[0] < block);
        assert(z*block+ buc_num[0] < split[0]->seg_num*kNumSlot);
        split[0]->val_slot[z*block + buc_num[0]].item = val_slot[i].item;
        split[0]->key_slot[z*block + (buc_num[0]++)].item = key_slot[i].item;
        bound0++;
        split[0]->num_key++;
      } // End of condition split_test is equal to zero.
    }



#else
  for (unsigned i = block*bound_buc; i < block*(bound_buc+1); ++i) {
    if (slot[i].key == INVALID)
      break;
    auto key_hash = slot[i].key & y_mask;
    uint64_t split_test = key_hash >> (8*sizeof(Key_t)-kDepth-local_depth-1);
    split_test = split_test & 1;
    size_t local_key_hash = key_hash & local_mask;
    local_key_hash = split[split_test]->lcdf(next_local_depth, local_key_hash);
    auto z = local_key_hash >> (64 - kDepth - next_local_depth);
    if (split_test == 1) {
      assert(z*block+ buc_num[1] < split[1]->seg_num*kNumSlot);
      if (z != buc_index[1]) {
        buc_index[1] = z;
        buc_num[1] = 0;
      }
      split[1]->slot[z*block + buc_num[1]].value = slot[i].value;
      split[1]->slot[z*block + (buc_num[1]++)].key = slot[i].key;
      split[1]->num_key++;
      bound1++;
    }
  }

  for (unsigned k = bound_buc+1; k < seg_num; k++) {
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (slot[i].key == INVALID)
        break;
      key_hash = slot[i].key & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = split[1]->lcdf(next_local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[1]) {
        buc_index[1] = z;
        buc_num[1] = 0;
      }

      assert(buc_num[1] < block);
      assert(z*block+ buc_num[1] < split[1]->seg_num*kNumSlot);

      split[1]->slot[z*block+ buc_num[1]].value = slot[i].value;
      split[1]->slot[z*block + (buc_num[1]++)].key = slot[i].key;
      split[1]->num_key++;
    }
  }


  for (unsigned k = 0; k < bound_buc; k++) {
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (slot[i].key == INVALID)
        break;
      key_hash = slot[i].key & y_mask;
      key_hash = key_hash & local_mask;
      local_key_hash = split[0]->lcdf(next_local_depth, key_hash);
      z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[0]) {
        buc_index[0] = z;
        buc_num[0] = 0;
      }
      assert(buc_num[0] < block);
      assert(z*block+ buc_num[0] < split[0]->seg_num*kNumSlot);
      split[0]->slot[z*block + buc_num[0]].value = slot[i].value;
      split[0]->slot[z*block + (buc_num[0]++)].key = slot[i].key;
      split[0]->num_key++;
    }
  }
    int bound0 = 0;
    for (unsigned i = block*bound_buc; i < block*(bound_buc+1); ++i) {
      if (slot[i].key == INVALID)
        break;
      auto key_hash = slot[i].key & y_mask;
      uint64_t split_test = (key_hash >> (8*sizeof(Key_t)-kDepth-local_depth-1));
      split_test = split_test & 1;
      size_t local_key_hash = key_hash & local_mask;
      local_key_hash = split[split_test]->lcdf(next_local_depth, local_key_hash);
      auto z = (local_key_hash >> (64 - kDepth - next_local_depth));
      if (z != buc_index[0]) {
        buc_index[0] = z;
        buc_num[0] = 0;
      }
      if (split_test == 0) {
        assert(buc_num[0] < block);
        assert(z*block+ buc_num[0] < split[0]->seg_num*kNumSlot);
        split[0]->slot[z*block + buc_num[0]].value = slot[i].value;
        split[0]->slot[z*block + (buc_num[0]++)].key = slot[i].key;
        bound0++;
        split[0]->num_key++;
      } // End of condition split_test is equal to zero.
    }


#endif
  return split;
}


#ifdef SEP
inline int Directory::exponential_search(Key_t& key, size_t bucket) {
  int bound = 1;
  int l,r;
  int m =  block * 0.4; // heuristic value
  if (key < key_slot[bucket + m].item) {
    int size = m;
    while (bound < size && key_slot[bucket + m - bound].item > key) {
      bound *= 2;
    }
    l = m - std::min<int>(bound, m);
    r = m - bound / 2;
  }
  else {
    int size = block - m;
    while (bound < size && key_slot[bucket + m + bound].item <= key) {
      bound *= 2;
    }
    l = m+bound/2;
    r = m+std::min<int>(bound, size);
  }
  return binary_search_upper_bound(l, r, key, bucket);
}
inline int Directory::binary_search_upper_bound(int l, int r, Key_t key, size_t bucket) {
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (key_slot[bucket + mid].item <= key) {
      l = mid + 1;
    }
    else {
      r = mid;
    }
  }
  return l;
}
#else
inline int Directory::exponential_search(Key_t& key, size_t bucket) {
  int bound = 1;
  int l,r;
  int m =  block * 0.4; // heuristic value
  if (key < slot[bucket + m].key) {
    int size = m;
    while (bound < size && slot[bucket + m - bound].key > key) {
      bound *= 2;
    }
    l = m - std::min<int>(bound, m);
    r = m - bound / 2;
  }
  else {
    int size = block - m;
    while (bound < size && slot[bucket + m + bound].key <= key) {
      bound *= 2;
    }
    l = m+bound/2;
    r = m+std::min<int>(bound, size);
  }
  return binary_search_upper_bound(l, r, key, bucket);
}
inline int Directory::binary_search_upper_bound(int l, int r, Key_t key, size_t bucket) {
  while (l < r) {
    int mid = l + (r - l) / 2;
    if (slot[bucket + mid].key <= key) {
      l = mid + 1;
    }
    else {
      r = mid;
    }
  }
  return l;
}
#endif

inline Value_t Directory::Get(Key_t& key, size_t y) {
  auto bucket = block*y;
  size_t result_exp = exponential_search(key, bucket) - 1;
#ifdef SEP
  if (key_slot[bucket + result_exp].item == key)
    return val_slot[bucket + result_exp].item;
  else {
    return NONE;
  }
#else
  if (slot[bucket + result_exp].key == key)
    return slot[bucket + result_exp].value;
  else
    return NONE;
#endif
}

inline void Directory::Scan(Key_t& min, int& count, size_t n, size_t z, Value_t* result) {
  Key_t k = min;
  Directory* current = this;

  auto bucket = block*z;
  size_t result_exp = 0;
  if (min != 0)
    result_exp = exponential_search(min, bucket);
  size_t index = result_exp > 0 ? bucket + result_exp -1 : bucket + result_exp;

#ifdef SEP
  Value* cur_val_slot = current->val_slot;
  size_t cur_seg_num = current->seg_num;
  while (count < n) {
    if (cur_val_slot[index].item != INVALID) {
      result[count++] = cur_val_slot[index].item;
      index++;
      if (index == cur_seg_num * block) {
        current = current->sibling;
        if (current == NULL)
          return;
        cur_val_slot = current->val_slot;
        cur_seg_num = current->seg_num;
        index = 0;
      }
    }
    else {
      index += (block - index % block);
      if (index >= cur_seg_num * block) {
        current = current->sibling;
        if (current == NULL)
          return;
        cur_val_slot = current->val_slot;
        cur_seg_num = current->seg_num;
        index = 0;
      }
    }
  }

#else
  Pair* cur_slot = current->slot;
  size_t cur_seg_num = current->seg_num;
  while (count < n) {
    if (cur_slot[index].value != INVALID) {
      result[count++] = cur_slot[index].value;
      index++;
      if (index == cur_seg_num * block) {
        current = current->sibling;
        if (current == NULL)
          return;
        index = 0;
        cur_slot = current->slot;
        cur_seg_num = current->seg_num;
      }
    }
    else {
      index += (block - index % block);
      if (index >= cur_seg_num * block) {
        current = current->sibling;
        if (current == NULL)
          return;
        index = 0;
        cur_slot = current->slot;
        cur_seg_num = current->seg_num;
      }
    }
  }
#endif

}

inline Value_t* Directory::Find(Key_t& key, size_t y) {

  auto bucket = block*y;
  size_t result_exp = exponential_search(key, bucket) - 1;
#ifdef SEP
  if (key_slot[bucket + result_exp].item == key)
    return &val_slot[bucket + result_exp].item;
  else
    return NULL;
#else
  if (slot[bucket + result_exp].key == key)
    return &slot[bucket + result_exp].value;
  else
    return NULL;
#endif
}

inline bool Directory::Expand(int local_depth, int rbits) {
  int max_seg_size = max_bucket_num(local_depth);
  if (seg_num*2 > max_seg_size)
    return false;
  if (line != NULL) {
    int ranges = (1 << rbits);
    uint64_t limit_stride = ((size_t)1 << (64-kDepth-local_depth));
    uint64_t one_range = limit_stride >> rbits;
    line[0].gradient *= 2;
    for (int i = 1; i < ranges; i++) {
      size_t left = one_range*i;
      size_t left_y = left*line[i-1].gradient+line[i-1].y_intercept;
      line[i].gradient *= 2;
      line[i].y_intercept = (double)left_y - line[i].gradient*(double)left;
    }
  }

  int prev_seg_num = seg_num;
  seg_num *= 2;
#ifdef SEP
  Key* temp_key_slot;
  Value* temp_val_slot;
  if (seg_num <= pool_num) {
    void* addr = chunk_alloc[seg_num-1].malloc();
    temp_key_slot = new(static_cast<Key*>(addr)) \
           Key[seg_num*kNumSlot];
    void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
    temp_val_slot = new(static_cast<Value*>(val_addr))\
               Value[seg_num*kNumSlot];
  }
  else {
    void* addr = malloc(sizeof(Key)*seg_num*kNumSlot*2);
    temp_key_slot = new(static_cast<Key*>(addr)) \
           Key[seg_num*kNumSlot];
    void* val_addr = addr + sizeof(Key) * seg_num * kNumSlot;
    temp_val_slot = new(static_cast<Value*>(val_addr))\
               Value[seg_num*kNumSlot];
  }
#else
  Pair* temp_slot;
  if (seg_num <= pool_num) {
    temp_slot = new(static_cast<Pair*>(chunk_alloc[seg_num-1].malloc())) \
           Pair[seg_num*kNumSlot];
  }
  else
    temp_slot = new Pair[seg_num*kNumSlot];
#endif
  int buc_idx = 0;
  int buc_num = 0;
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;

#ifdef SEP
  for (uint32_t k = 0; k < prev_seg_num; k++) { // for each bucket
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (key_slot[i].item == INVALID)
        break;
      uint64_t key_hash = key_slot[i].item & y_mask;
      key_hash = key_hash & local_mask;
      size_t local_key_hash = lcdf(local_depth, key_hash);
      auto z = (local_key_hash >> (64 - kDepth - local_depth));
      if (buc_idx != z) {
        buc_idx = z;
        buc_num = 0;
      }
      temp_val_slot[z*block + buc_num].item = val_slot[i].item;
      temp_key_slot[z*block + buc_num++].item = key_slot[i].item;
    }
  }
  if (prev_seg_num <= pool_num) {
    chunk_alloc[prev_seg_num-1].free(key_slot);
  }
  else {
    free(key_slot);
  }
  key_slot = temp_key_slot;
  val_slot = temp_val_slot;
#else
  for (uint32_t k = 0; k < prev_seg_num; k++) { // for each bucket
    for (unsigned i = block*k; i < block*(k+1); ++i) {
      if (slot[i].key == INVALID)
        break;
      uint64_t key_hash = slot[i].key & y_mask;
      key_hash = key_hash & local_mask;
      size_t local_key_hash = lcdf(local_depth, key_hash);
      auto z = (local_key_hash >> (64 - kDepth - local_depth));
      if (buc_idx != z) {
        buc_idx = z;
        buc_num = 0;
      }
      temp_slot[z*block + buc_num].value = slot[i].value;
      temp_slot[z*block + buc_num++].key = slot[i].key;
    }
  }
  if (prev_seg_num <= pool_num)
    chunk_alloc[prev_seg_num-1].free(slot);
  else
    delete[] slot;

  slot = temp_slot;
#endif
  remap_available = seg_num;
  return true;


}

// for local cdf
inline void Directory::split_local_cdf(Directory** split, int local_depth) {

  if (line == NULL) { // uniform segment
    split[0] = new(seg_alloc.allocate(1))Directory(local_depth+1, seg_num);
    split[1] = new(seg_alloc.allocate(1))Directory(local_depth+1, seg_num);
    return;
  }

  uint64_t limit = ((uint64_t)1 << (64 - kDepth - local_depth));
  uint64_t one_range = limit >> range_bits;
  int before_range = (1 << range_bits);
  uint64_t limit_y = limit * line[before_range-1].gradient + line[before_range-1].y_intercept;
  uint64_t next_limit = ((uint64_t)1 << (64 - kDepth - (local_depth + 1)));
  uint32_t PRACTICAL_MAX_SEG_NUM = max_bucket_num(local_depth);
  uint64_t last_y[2];
  last_y[1] = limit_y;
  uint64_t half_last = limit / 2; // half of local cdf range
  last_y[0] = half_last * line[before_range/2-1].gradient + line[before_range/2-1].y_intercept;
  last_y[1] -= (last_y[0] - last_y[0] % limit);
  int snum[2] = {0,};
  for (int32_t i = PRACTICAL_MAX_SEG_NUM*2-1; i >= 0; i--) {
    if (last_y[0] > next_limit*i) {
      snum[0] = (i+1);
      break;
    }
  }
  for (int32_t i = PRACTICAL_MAX_SEG_NUM*2-1; i >= 0; i--) {
    if (last_y[1] > next_limit*i) {
      snum[1] = (i+1);
      break;
    }
  }
  split[0] = new(seg_alloc.allocate(1))Directory(local_depth+1, snum[0]);
  split[1] = new(seg_alloc.allocate(1))Directory(local_depth+1, snum[1]);

  if (range_bits == 1) { // mininum # of ranges
    // [TODO] : if snum is 2^n, lcdf is not essential
    split[0]->range_bits = range_bits;
    split[1]->range_bits = range_bits;
    split[0]->line = static_cast<LineFriends*>(line_alloc[0].malloc());
    split[1]->line = static_cast<LineFriends*>(line_alloc[0].malloc());
    for (int i = 0; i < 2; i++) { // minimum % of ranges (2)
      split[0]->line[i].gradient = line[0].gradient;
      split[0]->line[i].y_intercept = 0;
      split[1]->line[i].gradient = line[1].gradient;
      split[1]->line[i].y_intercept = 0;
    }
  }
  else {
    split[0]->range_bits = range_bits-1;
    split[1]->range_bits = range_bits-1;
    if ((range_bits-1) <= 10) {
      split[0]->line = static_cast<LineFriends*>(line_alloc[range_bits-2].malloc());
      split[1]->line = static_cast<LineFriends*>(line_alloc[range_bits-2].malloc());
    }
    else {
      split[0]->line = new LineFriends[before_range/2]; // 2 * (before_range/2)
      split[1]->line = new LineFriends[before_range/2];
    }
    memcpy(split[0]->line, line, sizeof(double) * before_range);
    memcpy(split[1]->line, line + before_range/2, sizeof(double) * before_range);
    uint64_t left_y = last_y[0] - last_y[0] % limit;
    uint64_t right = one_range * (1 + before_range/2);
    uint64_t right_y = split[1]->line[0].gradient * right + split[1]->line[0].y_intercept;
    double gradient = (double)(right_y-left_y) / (double)one_range;
    split[1]->line[0].gradient = gradient;
    split[1]->line[0].y_intercept = 0;
    for (int i = 1; i < before_range/2; i++) {
      uint64_t left_y = split[1]->line[i-1].gradient * one_range * i + split[1]->line[i-1].y_intercept;
      split[1]->line[i].y_intercept = (double)left_y - split[1]->line[i].gradient*one_range*i;
    }
  }
}

inline int Directory::get_local_cdf_range (size_t key, int local_depth) {
  int target_range = key >> (64-kDepth-local_depth-range_bits);
  return target_range;
}

inline int Directory::get_bucket_increase (int range, int local_depth) {
  uint64_t limit_stride = ((uint64_t)1 << (64 - kDepth - local_depth));
  uint64_t  one_range = limit_stride >> range_bits;
  double one_bucket_gradient = (double)limit_stride / one_range;
  double how_many_buckets = std::ceil(line[range].gradient/one_bucket_gradient);
  return how_many_buckets < 1 ? 1 : how_many_buckets;
}

// reclaim gradient as much as bucket delta and give it to the range
inline bool Directory::tuning_local_cdf (int range, int& needed_bucket,
                                         std::vector<int>& range_count, int local_depth) {
  uint64_t limit_stride = ((uint64_t)1 << (64 - kDepth - local_depth));
  uint64_t one_range = limit_stride >> range_bits;
  double one_bucket_gradient = (double)limit_stride / one_range;
  double one_bucket_shifting = (double)limit_stride;
  double delta_gradient = one_bucket_gradient * needed_bucket;
  int ranges = (1 << range_bits);
  int new_reclaim_flag = -1;
  for (int i = reclaim_flag; i < ranges; i++) {
    double how_many_buckets = line[i].gradient/one_bucket_gradient;
    // if util is >= RECLAIM_THRE then it means this range has util enough util
    double util = range_count[i]/(how_many_buckets*block);
    if (util >= RECLAIM_THRE || i == range) {
      continue;
    }
    else {
      int taken_bucket = ceil((1-util) * how_many_buckets);
      bool enough = false;
      if (taken_bucket >= how_many_buckets)
        taken_bucket--;
      if (taken_bucket > needed_bucket) {
        enough = true;
        taken_bucket = needed_bucket;
      }
      if (taken_bucket == 0)
        continue;
      if (new_reclaim_flag == -1)
        new_reclaim_flag = i;
      double new_gradient = line[i].gradient - taken_bucket*one_bucket_gradient;
      assert(taken_bucket > 0 && taken_bucket < how_many_buckets);
      size_t left = one_range*i;
      size_t left_y = left*line[i].gradient+line[i].y_intercept;
      size_t right = one_range*(i+1);
      double right_y = right*line[i].gradient+line[i].y_intercept;
      line[i].gradient = new_gradient;
      line[i].y_intercept = (double)left_y - line[i].gradient*(double)left;
      double after_right_y = right*line[i].gradient+line[i].y_intercept;
      double shifting = after_right_y - right_y;
      assert((uint64_t)shifting%limit_stride == 0);
      for (int j = (i+1); j < ranges; j++) {
        line[j].y_intercept += shifting;
      }
      needed_bucket -= taken_bucket;
      if (needed_bucket == 0) {
        // give the gradient collected to the target range
        size_t left = one_range*range;
        size_t left_y = left*line[range].gradient+line[range].y_intercept;
        size_t right = one_range*(range+1);
        double right_y = right*line[range].gradient+line[range].y_intercept;
        line[range].gradient += delta_gradient;
        line[range].y_intercept = (double)left_y - line[range].gradient*(double)left;
        double after_right_y = right*line[range].gradient+line[range].y_intercept;
        range_count[range] *= 2;
        double shifting = after_right_y - right_y;
        assert(shifting > 0);
        assert((uint64_t)shifting%limit_stride == 0);
        for (int j = (range+1); j < ranges; j++) {
          line[j].y_intercept += shifting;
        }
        // update raclaim_flag
        reclaim_flag = new_reclaim_flag;
        return true;
      }
    }
  }

  if (delta_gradient != one_bucket_gradient*needed_bucket) {
    size_t left = one_range*range;
    size_t left_y = left*line[range].gradient+line[range].y_intercept;
    size_t right = one_range*(range+1);
    double right_y = right*line[range].gradient+line[range].y_intercept;
    line[range].gradient += (delta_gradient - one_bucket_gradient*needed_bucket);
    line[range].y_intercept = (double)left_y - line[range].gradient*(double)left;
    double after_right_y = right*line[range].gradient+line[range].y_intercept;
    double shifting = after_right_y - right_y;
    assert(shifting > 0);
    assert((uint64_t)shifting%limit_stride == 0);
    for (int j = (range+1); j < ranges; j++) {
      line[j].y_intercept += shifting;
    }
    // update raclaim_flag
    reclaim_flag = new_reclaim_flag;
    size_t last = one_range*ranges;
    size_t last_y = last*line[ranges-1].gradient+line[ranges-1].y_intercept;
    remap_available = last_y/limit_stride;
    if (last_y % limit_stride != 0)
      remap_available += 1;
  }
  return false;
}

// always when fixed is false, give gradient for needed buckets to the range
inline int Directory::tuning_local_cdf_by_range(int needed_bucket, int range, int local_depth) {
  int over_range = range;
  int ranges = (1 << range_bits);
  assert(range < ranges);
  uint64_t limit_stride = ((uint64_t)1 << (64 - kDepth - local_depth));
  uint64_t one_range = limit_stride / ranges;
  size_t left = one_range*over_range;
  size_t right = one_range*(1+over_range);
  size_t left_y = left*line[over_range].gradient+line[over_range].y_intercept;
  size_t last = one_range*ranges;
  size_t last_y = last*line[ranges-1].gradient+line[ranges-1].y_intercept;
  uint32_t PRACTICAL_MAX_SEG_NUM = max_bucket_num(local_depth);
  uint64_t max = PRACTICAL_MAX_SEG_NUM*limit_stride;
  double bucket_gradient = (double) needed_bucket * limit_stride / one_range;

  if (last_y >= max) {
    return -1;
  }

  if (last_y+limit_stride * needed_bucket >= max) {
    return -1;
  }

  double gradient, delta;
  uint64_t increasing;
  gradient = line[over_range].gradient + bucket_gradient;
  double y_intercept = (double)left_y - gradient*(double)left;

  size_t right_y = right*line[over_range].gradient+line[over_range].y_intercept;
  size_t after_right_y = right*gradient+y_intercept;
  increasing = after_right_y - right_y;
  line[over_range].gradient = gradient;
  line[over_range].y_intercept = y_intercept;
  for (int i =over_range+1; i < ranges; i++)
    line[i].y_intercept += increasing;

  last_y = last*line[ranges-1].gradient+line[ranges-1].y_intercept;
  if (seg_num < PRACTICAL_MAX_SEG_NUM) {
    int new_seg_num = last_y/limit_stride;
    if (last_y % limit_stride != 0)
      new_seg_num += 1;
    return new_seg_num;
  }

}


inline size_t Directory::lcdf(int local_depth, size_t key) {
  if (line == NULL) {
    return seg_num * key;
  }
  int target_range = key >> (64-kDepth-local_depth-range_bits);
  return line[target_range].gradient*key + line[target_range].y_intercept;
}


inline int Directory::divide_ranges_if_needed (uint64_t masked_key_hash, int local_depth) {
  double util = 0;
  int changed = 0;
  int ranges = (1 << range_bits);
  if (range_bits < INITIAL_RANGE_BITS) {
    changed = INITIAL_RANGE_BITS - range_bits;
    int stride = (1 << changed);
    LineFriends* new_line;
    int new_ranges = (1 << INITIAL_RANGE_BITS);
    if (INITIAL_RANGE_BITS <= 10) {
      new_line = static_cast<LineFriends*>(line_alloc[INITIAL_RANGE_BITS-1].malloc());
    }
    else
      new_line = new LineFriends[new_ranges];
    for (int i = ranges-1; i >= 0; i--) {
      for (int j = 0; j < stride; j++) {
        new_line[stride*i+j].gradient = line[i].gradient;
        new_line[stride*i+j].y_intercept = line[i].y_intercept;
      }
    }
    delete[] line;
    line = new_line;
    range_bits = INITIAL_RANGE_BITS;
    reclaim_flag *= stride;
  }
  do {
    int ranges = (1 << range_bits);
    uint64_t limit_stride = ((size_t)1 << (64-kDepth-local_depth));
    uint64_t  one_range = limit_stride / ranges;
    int range = get_local_cdf_range(masked_key_hash, local_depth);
    uint64_t first_key = lcdf(local_depth, (range*one_range));
    int first_bucket = first_key >> (64 - kDepth - local_depth);
    uint64_t last_key = lcdf(local_depth, ((range+1)*one_range-1));
    int last_bucket  = last_key >> (64 - kDepth - local_depth);
    if (last_bucket == seg_num)
     last_bucket--;
    int count = 0;
    if (first_bucket == last_bucket) {
      return changed;
    }
    for (int i = first_bucket; i <= last_bucket; i++) {
      for (int j = 0; j < block; j++) {
#ifdef SEP
        if (key_slot[i*block+j].item == INVALID)
#else
        if (slot[i*block+j].key == INVALID)
#endif
          break;
        count++;
      }
    }
    util = (double)count / ((last_bucket - first_bucket + 1) * block);
    if (util < RECLAIM_THRE) {
      if (range_bits == RANGE_BITS_LIMIT) {// [TODO] what is best value??
        break;
      }
      changed++;
      LineFriends* new_line;
      if ((range_bits+1) <= 10) {
        new_line = static_cast<LineFriends*>(line_alloc[range_bits].malloc());
      }
      else
        new_line = new LineFriends[2*ranges]; // 2 * line

      for (int i = ranges-1; i >= 0; i--) {
        new_line[2*i].gradient = line[i].gradient;
        new_line[2*i].y_intercept = line[i].y_intercept;
        new_line[2*i+1].gradient = line[i].gradient;
        new_line[2*i+1].y_intercept = line[i].y_intercept;
      }
      if (range_bits <= 10)
        line_alloc[range_bits-1].free(line);
      else
        delete[] line;
      line = new_line;
      range_bits++;
      reclaim_flag *= 2;

    }
  } while (util < RECLAIM_THRE);
  //if (changed > 0 && row_util)
    //merge_local_cdf(local_depth);
  return changed;
}


inline void Directory::init_lcdf (int local_depth) {
  assert (line == NULL);
  remap_available = seg_num;
  range_bits = 1;
  int ranges = (1 << range_bits);
  double gradient = seg_num;
  line = static_cast<LineFriends*>(line_alloc[0].malloc());
  for (int i = 0; i < ranges; i++) {
    line[i].gradient = gradient;
    line[i].y_intercept = 0;
  }
}


inline double Directory::get_segment_util() {

  return num_key/(double)(seg_num*kNumSlot);
}

// find over range from over bucket
inline int Directory::find_over_range(int z, int local_depth, Key_t* over_bucket) {
  int prev_range;
  int prev_count = 0;
  int count = 0;
  int over_range;
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;
  for (int k = z*block; k < (z+1)*block; k++) {
#ifdef SEP
    uint64_t tkey_hash = key_slot[k].item & y_mask;
#else
    uint64_t tkey_hash = slot[k].key & y_mask;
#endif
    tkey_hash = tkey_hash & local_mask;
    over_bucket[k%block] = tkey_hash;
    int range = get_local_cdf_range(tkey_hash, local_depth);
    if ((k == z*block) || ((k+1) % block == 0) || (range != prev_range)) {
      if ((k == 0) || (prev_count < count)) {
        prev_count = count;
        over_range = prev_range;
      }
      prev_range = range;
      count = 0;
    }
    count++;
  }
  return over_range;
}
// find over range from over bucket
inline int Directory::find_over_range(int z, int local_depth) {
  int prev_range;
  int prev_count = 0;
  int count = 0;
  int over_range;
  size_t local_mask = ((size_t)1 << (8*sizeof(Key_t)-kDepth-local_depth))-1;
  for (int k = z*block; k < (z+1)*block; k++) {
#ifdef SEP
    uint64_t tkey_hash = key_slot[k].item & y_mask;
#else
    uint64_t tkey_hash = slot[k].key & y_mask;
#endif
    tkey_hash = tkey_hash & local_mask;
    int range = get_local_cdf_range(tkey_hash, local_depth);
    if ((k == z*block) || ((k+1) % block == 0) || (range != prev_range)) {
      if ((k == 0) || (prev_count < count)) {
        prev_count = count;
        over_range = prev_range;
      }
      prev_range = range;
      count = 0;
    }
    count++;
  }
  return over_range;
}
