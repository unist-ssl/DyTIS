#pragma once

#define REMAP_THRE 6
#define BUC_THRE 0.6
#define RECLAIM_THRE 0.6
#define SKEWED_MAX_BITS 1
#define UNIFORM_MAX_BITS 7

#define ADDR_BITS 48
#define DIRECTORY_BITS 6
#define LOCAL_DEPTH_BITS 5
static const int LOCAL_DEPTH_SHIFT = (64 - DIRECTORY_BITS - LOCAL_DEPTH_BITS);
static const uint64_t ADDR_MASK = ((uint64_t) 1 << ADDR_BITS) - 1;

static size_t kDepth = 9;
const size_t kBucSize = 128 * 16; // segment size, 512 = 16B * 32 pairs, 1024 = 64 pairs, 4096 = 256 pairs
const size_t kNumSlot = kBucSize/sizeof(Pair); // # key value pair slot of blocks in segment
const uint16_t block = kNumSlot;
size_t y_mask = ((size_t)1 << (8*sizeof(Key_t) - kDepth)) - 1;
static bool UNIFORM_TEST = false;
static int DEFAULT_MAX_BITS = SKEWED_MAX_BITS;
uint64_t max_bucket_num (size_t local_depth) {
  // [TODO] : return 1, not 2..
  if (local_depth >= REMAP_THRE)
    // If workload is skewed, DEFAULT_MAX_BITS is SKEWED_MAX_BITS
    // Else if workload is uniform, DEFAULT_MAX_BITS is UNIFORM_MAX_BITS
    return ((uint64_t)1 << (DEFAULT_MAX_BITS+local_depth-REMAP_THRE));
  else
    return 1;

}

