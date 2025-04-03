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

#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#endif
#include "ts_common.h"

namespace kwdbts {

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

// Only generate field unused warning for padding array, or build under
// GCC 4.8.1 will fail.
#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
#else
#define ROCKSDB_FIELD_UNUSED
#endif  // __clang__


static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("wfe");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

class Random {
 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };
  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  // This is the largest value that can be returned from Next()
  enum : uint32_t { kMaxNext = M };

  explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

  void Reset(uint32_t s) { seed_ = GoodSeed(s); }

  uint32_t Next() {
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  // Returns a Random instance for use by the current thread without
  // additional locking
  static Random* GetTLSInstance() {
    static Random* tls_instance;
    static std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

    auto rv = tls_instance;
    if (UNLIKELY(rv == nullptr)) {
      size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
      rv = new (&tls_instance_bytes) Random((uint32_t)seed);
      tls_instance = rv;
    }
    return rv;
  }
};

class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                Logger* logger = nullptr) = 0;

  virtual size_t BlockSize() const = 0;
};


class Arena : public Allocator {
 public:
  // No copying allowed
  Arena(const Arena&) = delete;
  void operator=(const Arena&) = delete;

  static const size_t kInlineSize = 2048;
  static const size_t kMinBlockSize;
  static const size_t kMaxBlockSize;

  // huge_page_size: if 0, don't use huge page TLB. If > 0 (should set to the
  // supported hugepage size of the system), block allocation will try huge
  // page TLB first. If allocation fails, will fall back to normal case.
  explicit Arena(size_t block_size = kMinBlockSize, size_t huge_page_size = 0);
  ~Arena();

  char* Allocate(size_t bytes) override;

  // huge_page_size: if >0, will try to allocate from huage page TLB.
  // The argument will be the size of the page size for huge page TLB. Bytes
  // will be rounded up to multiple of the page size to allocate through mmap
  // anonymous option with huge page on. The extra  space allocated will be
  // wasted. If allocation fails, will fall back to normal case. To enable it,
  // need to reserve huge pages for it to be allocated, like:
  //     sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt for details.
  // huge page allocation can fail. In this case it will fail back to
  // normal cases. The messages will be logged to logger. So when calling with
  // huge_page_tlb_size > 0, we highly recommend a logger is passed in.
  // Otherwise, the error message will be printed out to stderr directly.
  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (exclude the space allocated but not yet used for future
  // allocations).
  size_t ApproximateMemoryUsage() const {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*) -
           alloc_bytes_remaining_;
  }

  size_t MemoryAllocatedBytes() const { return blocks_memory_; }

  size_t AllocatedAndUnused() const { return alloc_bytes_remaining_; }

  // If an allocation is too big, we'll allocate an irregular block with the
  // same size of that allocation.
  size_t IrregularBlockNum() const { return irregular_block_num; }

  size_t BlockSize() const override { return kBlockSize; }

  bool IsInInlineBlock() const {
    return blocks_.empty();
  }

 private:
  char inline_block_[kInlineSize] __attribute__((__aligned__(alignof(max_align_t))));
  // Number of bytes allocated in one block
  const size_t kBlockSize;
  // Array of new[] allocated memory blocks
  typedef std::vector<char*> Blocks;
  Blocks blocks_;

  struct MmapInfo {
    void* addr_;
    size_t length_;

    MmapInfo(void* addr, size_t length) : addr_(addr), length_(length) {}
  };
  std::vector<MmapInfo> huge_blocks_;
  size_t irregular_block_num = 0;

  // Stats for current active block.
  // For each block, we allocate aligned memory chucks from one end and
  // allocate unaligned memory chucks from the other end. Otherwise the
  // memory waste for alignment will be higher if we allocate both types of
  // memory from one direction.
  char* unaligned_alloc_ptr_ = nullptr;
  char* aligned_alloc_ptr_ = nullptr;
  // How many bytes left in currently active block?
  size_t alloc_bytes_remaining_ = 0;

#ifdef MAP_HUGETLB
  size_t hugetlb_size_ = 0;
#endif  // MAP_HUGETLB
  char* AllocateFromHugePage(size_t bytes);
  char* AllocateFallback(size_t bytes, bool aligned);
  char* AllocateNewBlock(size_t block_bytes);

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_ = 0;
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    unaligned_alloc_ptr_ -= bytes;
    alloc_bytes_remaining_ -= bytes;
    return unaligned_alloc_ptr_;
  }
  return AllocateFallback(bytes, false /* unaligned */);
}

// check and adjust the block_size so that the return value is
//  1. in the range of [kMinBlockSize, kMaxBlockSize].
//  2. the multiple of align unit.
extern size_t OptimizeBlockSize(size_t block_size);

//
// SpinMutex has very low overhead for low-contention cases.  Method names
// are chosen so you can use std::unique_lock or std::lock_guard with it.
//
class SpinMutex {
 public:
  SpinMutex() : locked_(false) {}

  bool try_lock() {
    auto currently_locked = locked_.load(std::memory_order_relaxed);
    return !currently_locked &&
           locked_.compare_exchange_weak(currently_locked, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  void lock() {
    for (size_t tries = 0;; ++tries) {
      if (try_lock()) {
        // success
        break;
      }
      AsmVolatilePause();
      if (tries > 100) {
        std::this_thread::yield();
      }
    }
  }

  void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> locked_;
};

// An array of core-local values. Ideally the value type, T, is cache aligned to
// prevent false sharing.
template <typename T>
class CoreLocalArray {
 public:
  CoreLocalArray();

  size_t Size() const;
  // returns pointer to the element corresponding to the core that the thread
  // currently runs on.
  T* Access() const;
  // same as above, but also returns the core index, which the client can cache
  // to reduce how often core ID needs to be retrieved. Only do this if some
  // inaccuracy is tolerable, as the thread may migrate to a different core.
  std::pair<T*, size_t> AccessElementAndIndex() const;
  // returns pointer to element for the specified core index. This can be used,
  // e.g., for aggregation, or if the client caches core index.
  T* AccessAtCore(size_t core_idx) const;

 private:
  std::unique_ptr<T[]> data_;
  int size_shift_;
};

template <typename T>
CoreLocalArray<T>::CoreLocalArray() {
  int num_cpus = static_cast<int>(std::thread::hardware_concurrency());
  // find a power of two >= num_cpus and >= 8
  size_shift_ = 3;
  while (1 << size_shift_ < num_cpus) {
    ++size_shift_;
  }
  data_.reset(new T[static_cast<size_t>(1) << size_shift_]);
}

template <typename T>
size_t CoreLocalArray<T>::Size() const {
  return static_cast<size_t>(1) << size_shift_;
}

template <typename T>
T* CoreLocalArray<T>::Access() const {
  return AccessElementAndIndex().first;
}

int PhysicalCoreID();

template <typename T>
std::pair<T*, size_t> CoreLocalArray<T>::AccessElementAndIndex() const {
  int cpuid = PhysicalCoreID();
  size_t core_idx;
  if (UNLIKELY(cpuid < 0)) {
    // cpu id unavailable, just pick randomly
    core_idx = Random::GetTLSInstance()->Uniform(1 << size_shift_);
  } else {
    core_idx = static_cast<size_t>(cpuid & ((1 << size_shift_) - 1));
  }
  return {AccessAtCore(core_idx), core_idx};
}

template <typename T>
T* CoreLocalArray<T>::AccessAtCore(size_t core_idx) const {
  assert(core_idx < static_cast<size_t>(1) << size_shift_);
  return &data_[core_idx];
}

// ConcurrentArena wraps an Arena.  It makes it thread safe using a fast
// inlined spinlock, and adds small per-core allocation caches to avoid
// contention for small allocations.  To avoid any memory waste from the
// per-core shards, they are kept small, they are lazily instantiated
// only if ConcurrentArena actually notices concurrent use, and they
// adjust their size so that there is no fragmentation waste when the
// shard blocks are allocated from the underlying main arena.
class ConcurrentArena : public Allocator {
 public:
  // block_size and huge_page_size are the same as for Arena (and are
  // in fact just passed to the constructor of arena_.  The core-local
  // shards compute their shard_block_size as a fraction of block_size
  // that varies according to the hardware concurrency level.
  explicit ConcurrentArena(size_t block_size = Arena::kMinBlockSize,
                           size_t huge_page_size = 0);

  char* Allocate(size_t bytes) override {
    return AllocateImpl(bytes, false /*force_arena*/,
                        [=]() { return arena_.Allocate(bytes); });
  }

  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override {
    size_t rounded_up = ((bytes - 1) | (sizeof(void*) - 1)) + 1;
    assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void*) &&
           (rounded_up % sizeof(void*)) == 0);

    return AllocateImpl(rounded_up, huge_page_size != 0 /*force_arena*/, [=]() {
      return arena_.AllocateAligned(rounded_up, huge_page_size, logger);
    });
  }

  size_t ApproximateMemoryUsage() const {
    std::unique_lock<SpinMutex> lock(arena_mutex_, std::defer_lock);
    lock.lock();
    return arena_.ApproximateMemoryUsage() - ShardAllocatedAndUnused();
  }

  size_t MemoryAllocatedBytes() const {
    return memory_allocated_bytes_.load(std::memory_order_relaxed);
  }

  size_t AllocatedAndUnused() const {
    return arena_allocated_and_unused_.load(std::memory_order_relaxed) +
           ShardAllocatedAndUnused();
  }

  size_t IrregularBlockNum() const {
    return irregular_block_num_.load(std::memory_order_relaxed);
  }

  size_t BlockSize() const override { return arena_.BlockSize(); }

 private:
  struct Shard {
    char padding[40] ROCKSDB_FIELD_UNUSED;
    mutable SpinMutex mutex;
    char* free_begin_;
    std::atomic<size_t> allocated_and_unused_;

    Shard() : free_begin_(nullptr), allocated_and_unused_(0) {}
  };

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
  static __thread size_t tls_cpuid;
#else
  enum ZeroFirstEnum : size_t { tls_cpuid = 0 };
#endif

  char padding0[56] ROCKSDB_FIELD_UNUSED;

  size_t shard_block_size_;

  CoreLocalArray<Shard> shards_;

  Arena arena_;
  mutable SpinMutex arena_mutex_;
  std::atomic<size_t> arena_allocated_and_unused_;
  std::atomic<size_t> memory_allocated_bytes_;
  std::atomic<size_t> irregular_block_num_;

  char padding1[56] ROCKSDB_FIELD_UNUSED;

  Shard* Repick();

  size_t ShardAllocatedAndUnused() const {
    size_t total = 0;
    for (size_t i = 0; i < shards_.Size(); ++i) {
      total += shards_.AccessAtCore(i)->allocated_and_unused_.load(
          std::memory_order_relaxed);
    }
    return total;
  }

  template <typename Func>
  char* AllocateImpl(size_t bytes, bool force_arena, const Func& func) {
    size_t cpu;

    // Go directly to the arena if the allocation is too large, or if
    // we've never needed to Repick() and the arena mutex is available
    // with no waiting.  This keeps the fragmentation penalty of
    // concurrency zero unless it might actually confer an advantage.
    std::unique_lock<SpinMutex> arena_lock(arena_mutex_, std::defer_lock);
    if (bytes > shard_block_size_ / 4 || force_arena ||
        ((cpu = tls_cpuid) == 0 &&
         !shards_.AccessAtCore(0)->allocated_and_unused_.load(
             std::memory_order_relaxed) &&
         arena_lock.try_lock())) {
      if (!arena_lock.owns_lock()) {
        arena_lock.lock();
      }
      auto rv = func();
      Fixup();
      return rv;
    }

    // pick a shard from which to allocate
    Shard* s = shards_.AccessAtCore(cpu & (shards_.Size() - 1));
    if (!s->mutex.try_lock()) {
      s = Repick();
      s->mutex.lock();
    }
    std::unique_lock<SpinMutex> lock(s->mutex, std::adopt_lock);

    size_t avail = s->allocated_and_unused_.load(std::memory_order_relaxed);
    if (avail < bytes) {
      // reload
      std::lock_guard<SpinMutex> reload_lock(arena_mutex_);

      // If the arena's current block is within a factor of 2 of the right
      // size, we adjust our request to avoid arena waste.
      auto exact = arena_allocated_and_unused_.load(std::memory_order_relaxed);
      assert(exact == arena_.AllocatedAndUnused());

      if (exact >= bytes && arena_.IsInInlineBlock()) {
        // If we haven't exhausted arena's inline block yet, allocate from arena
        // directly. This ensures that we'll do the first few small allocations
        // without allocating any blocks.
        // In particular this prevents empty memtables from using
        // disproportionately large amount of memory: a memtable allocates on
        // the order of 1 KB of memory when created; we wouldn't want to
        // allocate a full arena block (typically a few megabytes) for that,
        // especially if there are thousands of empty memtables.
        auto rv = func();
        Fixup();
        return rv;
      }

      avail = exact >= shard_block_size_ / 2 && exact < shard_block_size_ * 2
                  ? exact
                  : shard_block_size_;
      s->free_begin_ = arena_.AllocateAligned(avail);
      Fixup();
    }
    s->allocated_and_unused_.store(avail - bytes, std::memory_order_relaxed);

    char* rv;
    if ((bytes % sizeof(void*)) == 0) {
      // aligned allocation from the beginning
      rv = s->free_begin_;
      s->free_begin_ += bytes;
    } else {
      // unaligned from the end
      rv = s->free_begin_ + avail - bytes;
    }
    return rv;
  }

  void Fixup() {
    arena_allocated_and_unused_.store(arena_.AllocatedAndUnused(),
                                      std::memory_order_relaxed);
    memory_allocated_bytes_.store(arena_.MemoryAllocatedBytes(),
                                  std::memory_order_relaxed);
    irregular_block_num_.store(arena_.IrregularBlockNum(),
                               std::memory_order_relaxed);
  }

  ConcurrentArena(const ConcurrentArena&) = delete;
  ConcurrentArena& operator=(const ConcurrentArena&) = delete;
};

}  // namespace kwdbts
