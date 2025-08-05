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

static constexpr size_t ALIGNED_SIZE = alignof(max_align_t);
static_assert((ALIGNED_SIZE & (ALIGNED_SIZE - 1)) == 0, "Aligned size must be pow of 2");

int GetCPUID() {
#if defined(__x86_64__) && (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
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
  size = std::max(Allocator::MIN_BLOCK_SIZE, size);
  size = std::min(Allocator::MAX_BLOCK_SIZE, size);
  if ((size & (ALIGNED_SIZE - 1)) != 0) {
    size &= ~(ALIGNED_SIZE - 1);
    size += ALIGNED_SIZE;
  }
  return size;
}

Allocator::Allocator(size_t size) : block_size_(FixedBlockSize(size)) {
  assert(block_size_ >= MIN_BLOCK_SIZE && block_size_ <= MAX_BLOCK_SIZE && (block_size_ & (ALIGNED_SIZE - 1)) == 0);
  alloc_bytes_remaining_ = sizeof(internal_storage_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = internal_storage_;
  unaligned_alloc_ptr_ = internal_storage_ + alloc_bytes_remaining_;
}

Allocator::~Allocator() {
  for (const auto &block : blocks_) {
    delete[] block;
  }
}

char *Allocator::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > block_size_ / 4) {
    ++irregular_block_num_;
    return AllocateNewBlock(bytes);
  }

  auto block_head = AllocateNewBlock(block_size_);
  alloc_bytes_remaining_ = block_size_ - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + block_size_;
    return block_head;
  }
  aligned_alloc_ptr_ = block_head;
  unaligned_alloc_ptr_ = block_head + block_size_ - bytes;
  return unaligned_alloc_ptr_;
}

char *Allocator::AllocateAligned(size_t bytes) {
  size_t mod = reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (ALIGNED_SIZE - 1);
  size_t skip = mod == 0 ? 0 : ALIGNED_SIZE - mod;
  size_t needed = bytes + skip;
  char *result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + skip;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    result = AllocateFallback(bytes, true);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (ALIGNED_SIZE - 1)) == 0);
  return result;
}

char *Allocator::AllocateNewBlock(size_t block_bytes) {
  blocks_.emplace_back(nullptr);
  auto block = new char[block_bytes];
  blocks_memory_ += block_bytes;
  blocks_.back() = block;
  return block;
}

static constexpr size_t MAX_SHARD_BLOCK_SIZE = 1 << 17;
thread_local size_t ConcurrentAllocator::tls_cpuid = 0;

ConcurrentAllocator::ConcurrentAllocator(size_t size) // NOLINT
    : shard_block_size_(std::min(MAX_SHARD_BLOCK_SIZE, size / 8)), alloc_(size) {
  Update();
}

ConcurrentAllocator::Shard *ConcurrentAllocator::Repick() {
  auto shard_info = shards_.AccessElementAndIndex();
  tls_cpuid = shard_info.second | shards_.Size();
  return shard_info.first;
}

} //  namespace kwdbts
