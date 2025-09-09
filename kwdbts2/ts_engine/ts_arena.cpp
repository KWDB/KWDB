// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_arena.h"
#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#endif

namespace kwdbts {

int GetCPUID() {
#if defined(__x86_64__) &&                                                     \
    (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
  return sched_getcpu();
#elif defined(__x86_64__) || defined(__i386__)
  unsigned eax, ebx = 0, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    return -1;
  }
  return ebx >> 24;
#else
  return -1;
#endif
}

static size_t FixedBlockSize(size_t size) {
  size = std::max(MIN_BLOCK_SIZE, size);
  size = std::min(MAX_BLOCK_SIZE, size);
  if ((size & (ALIGNED_SIZE - 1)) != 0) {
    size &= ~(ALIGNED_SIZE - 1);
    size += ALIGNED_SIZE;
  }
  return size;
}

static constexpr size_t MAX_SHARD_BLOCK_SIZE = 1 << 17;
__thread size_t ConcurrentAllocator::tls_cpuid = 0;

ConcurrentAllocator::ConcurrentAllocator(size_t size) // NOLINT
    : shard_block_size_(std::min(MAX_SHARD_BLOCK_SIZE, size / 8)),
      block_size_(FixedBlockSize(size)) {
  assert(block_size_ >= MIN_BLOCK_SIZE && block_size_ <= MAX_BLOCK_SIZE &&
         (block_size_ & (ALIGNED_SIZE - 1)) == 0);
  alloc_bytes_remaining_ = sizeof(internal_storage_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = internal_storage_;
  unaligned_alloc_ptr_ = internal_storage_ + alloc_bytes_remaining_;
  Update();
}

ConcurrentAllocator::~ConcurrentAllocator() {
  for (const auto &block : blocks_) {
    delete[] block;
  }
}

char *ConcurrentAllocator::AllocateAlignedUnsafe(size_t size) {
  size_t mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (ALIGNED_SIZE - 1);
  size_t skip = mod == 0 ? 0 : ALIGNED_SIZE - mod;
  size_t needed = size + skip;
  char *result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + skip;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    result = AllocateInternal(size, true);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (ALIGNED_SIZE - 1)) == 0);
  return result;
}

char *ConcurrentAllocator::AllocateInternal(size_t size, bool aligned) {
  if (size > block_size_ / 4) {
    ++irregular_block_num_;
    return AllocateBlock(size);
  }

  auto block_head = AllocateBlock(block_size_);
  alloc_bytes_remaining_ = block_size_ - size;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + size;
    unaligned_alloc_ptr_ = block_head + block_size_;
    return block_head;
  }
  aligned_alloc_ptr_ = block_head;
  unaligned_alloc_ptr_ = block_head + block_size_ - size;
  return unaligned_alloc_ptr_;
}

ConcurrentAllocator::Shard *ConcurrentAllocator::Repick() {
  auto shard_info = shards_.AccessElementAndIndex();
  tls_cpuid = shard_info.second | shards_.Size();
  return shard_info.first;
}

}  //  namespace kwdbts
