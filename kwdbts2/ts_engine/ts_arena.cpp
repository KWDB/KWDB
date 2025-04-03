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

namespace kwdbts {


// MSVC complains that it is already defined since it is static in the header.
#ifndef _MSC_VER
const size_t Arena::kInlineSize;
#endif

const size_t Arena::kMinBlockSize = 4096;
const size_t Arena::kMaxBlockSize = 2u << 30;
static const int kAlignUnit = alignof(max_align_t);

int PhysicalCoreID() {
#if defined(ROCKSDB_SCHED_GETCPU_PRESENT) && defined(__x86_64__) && \
    (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
  // sched_getcpu uses VDSO getcpu() syscall since 2.22. I believe Linux offers VDSO
  // support only on x86_64. This is the fastest/preferred method if available.
  int cpuno = sched_getcpu();
  if (cpuno < 0) {
    return -1;
  }
  return cpuno;
#elif defined(__x86_64__) || defined(__i386__)
  // clang/gcc both provide cpuid.h, which defines __get_cpuid(), for x86_64 and i386.
  unsigned eax, ebx = 0, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    return -1;
  }
  return ebx >> 24;
#else
  // give up, the caller can generate a random number or something.
  return -1;
#endif
}

size_t OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

Arena::Arena(size_t block_size, size_t huge_page_size)
    : kBlockSize(OptimizeBlockSize(block_size)) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
#ifdef MAP_HUGETLB
  hugetlb_size_ = huge_page_size;
  if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
    hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
  }
#else
  (void)huge_page_size;
#endif
}

Arena::~Arena() {
  for (const auto& block : blocks_) {
    delete[] block;
  }

#ifdef MAP_HUGETLB
  for (const auto& mmap_info : huge_blocks_) {
    if (mmap_info.addr_ == nullptr) {
      continue;
    }
    auto ret = munmap(mmap_info.addr_, mmap_info.length_);
    if (ret != 0) {
      // TODO(sdong): Better handling
    }
  }
#endif
}

char* Arena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
#ifdef MAP_HUGETLB
  if (hugetlb_size_) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
#endif
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateNewBlock(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes) {
#ifdef MAP_HUGETLB
  if (hugetlb_size_ == 0) {
    return nullptr;
  }
  // Reserve space in `huge_blocks_` before calling `mmap`.
  // Use `emplace_back()` instead of `reserve()` to let std::vector manage its
  // own memory and do fewer reallocations.
  //
  // - If `emplace_back` throws, no memory leaks because we haven't called
  //   `mmap` yet.
  // - If `mmap` throws, no memory leaks because the vector will be cleaned up
  //   via RAII.
  huge_blocks_.emplace_back(nullptr /* addr */, 0 /* length */);

  void* addr = mmap(nullptr, bytes, (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0);

  if (addr == MAP_FAILED) {
    return nullptr;
  }
  huge_blocks_.back() = MmapInfo(addr, bytes);
  blocks_memory_ += bytes;
  if (tracker_ != nullptr) {
    tracker_->Allocate(bytes);
  }
  return reinterpret_cast<char*>(addr);
#else
  (void)bytes;
  return nullptr;
#endif
}

char* Arena::AllocateAligned(size_t bytes, size_t huge_page_size,
                             Logger* logger) {
  assert((kAlignUnit & (kAlignUnit - 1)) ==
         0);  // Pointer size should be a power of 2

#ifdef MAP_HUGETLB
  if (huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TBL table.
    assert(logger != nullptr);  // logger need to be passed in.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     strerror(errno));
      // fail back to malloc
    } else {
      return addr;
    }
  }
#else
  (void)huge_page_size;
  (void)logger;
#endif

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  // Reserve space in `blocks_` before allocating memory via new.
  // Use `emplace_back()` instead of `reserve()` to let std::vector manage its
  // own memory and do fewer reallocations.
  //
  // - If `emplace_back` throws, no memory leaks because we haven't called `new`
  //   yet.
  // - If `new` throws, no memory leaks because the vector will be cleaned up
  //   via RAII.
  blocks_.emplace_back(nullptr);

  char* block = new char[block_bytes];
  size_t allocated_size;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  allocated_size = malloc_usable_size(block);
#ifndef NDEBUG
  // It's hard to predict what malloc_usable_size() returns.
  // A callback can allow users to change the costed size.
  std::pair<size_t*, size_t*> pair(&allocated_size, &block_bytes);
  TEST_SYNC_POINT_CALLBACK("Arena::AllocateNewBlock:0", &pair);
#endif  // NDEBUG
#else
  allocated_size = block_bytes;
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  blocks_memory_ += allocated_size;
  blocks_.back() = block;
  return block;
}

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread size_t ConcurrentArena::tls_cpuid = 0;
#endif

namespace {
// If the shard block size is too large, in the worst case, every core
// allocates a block without populate it. If the shared block size is
// 1MB, 64 cores will quickly allocate 64MB, and may quickly trigger a
// flush. Cap the size instead.
const size_t kMaxShardBlockSize = size_t{128 * 1024};
}  // namespace

ConcurrentArena::ConcurrentArena(size_t block_size, size_t huge_page_size)
    : shard_block_size_(std::min(kMaxShardBlockSize, block_size / 8)),
      shards_(),
      arena_(block_size, huge_page_size) {
  Fixup();
}

ConcurrentArena::Shard* ConcurrentArena::Repick() {
  auto shard_and_index = shards_.AccessElementAndIndex();
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
  // even if we are cpu 0, use a non-zero tls_cpuid so we can tell we
  // have repicked
  tls_cpuid = shard_and_index.second | shards_.Size();
#endif
  return shard_and_index.first;
}

}  //  namespace kwdbts

