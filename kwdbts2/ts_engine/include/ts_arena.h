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
#include <memory>
#include <utility>
#include <vector>
#include "ts_common.h"

namespace kwdbts {

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

static void AsmVolatilePause() {
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
  static constexpr uint32_t M = 2147483647L; // 2^31 - 1
  static constexpr uint32_t A = 16807;       // bits 14, 8, 7, 5, 2, 1, 0

  uint32_t seed_;

public:
  explicit Random(uint32_t s) : seed_((s & M) != 0 ? s & M : 1) {}
  void Reset(uint32_t s) { seed_ = (s & M) != 0 ? s & M : 1; }

  static constexpr auto MAX_NEXT = M;

  auto Next() {
    auto x = static_cast<uint64_t>(seed_) * A;
    seed_ = static_cast<uint32_t>((x >> 31) + (x & M));
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint32_t Uniform(int n) { return Next() % n; }
  bool OneIn(int n) { return Uniform(n) == 0; }
  uint32_t Skewed(int n) { return Uniform(1 << Uniform(n + 1)); }

  static Random *GetInstance() {
    thread_local Random *instance;
    thread_local std::aligned_storage_t<sizeof(Random)> storage;

    auto ret = instance;
    if (UNLIKELY(ret == nullptr)) {
      const auto seed =
          std::hash<std::thread::id>()(std::this_thread::get_id());
      ret = new (&storage) Random(static_cast<uint32_t>(seed));
      instance = ret;
    }
    return ret;
  }
};

class SpinMutex {
public:
  SpinMutex() : locked_(false) {}

  bool try_lock() {
    auto locked = locked_.load(std::memory_order_relaxed);
    return !locked && locked_.compare_exchange_weak(locked, true,
                                                    std::memory_order_acquire,
                                                    std::memory_order_relaxed);
  }

  void lock() {
    for (size_t count = 0;; ++count) {
      if (try_lock()) {
        break;
      }
      AsmVolatilePause();
      if (count > 100) {
        std::this_thread::yield();
      }
    }
  }

  void unlock() { locked_.store(false, std::memory_order_release); }

private:
  std::atomic<bool> locked_;
};

template <typename T> class TLSArray {
public:
  TLSArray();

  auto Size() const { return 1ul << size_shift_; }     // NOLINT
  auto Access() const { return AccessElementAndIndex().first; } // NOLINT
  auto AccessAtCore(size_t core_idx) const {                    // NOLINT
    assert(core_idx < (1ul << size_shift_));
    return &data_[core_idx];
  }

  std::pair<T *, size_t> AccessElementAndIndex() const; // NOLINT

private:
  std::unique_ptr<T[]> data_;
  int size_shift_;
};

template <typename T> TLSArray<T>::TLSArray() {
  const int num_cpus = static_cast<int>(std::thread::hardware_concurrency());
  size_shift_ = 3;
  while ((1 << size_shift_) < num_cpus) { // NOLINT
    ++size_shift_;
  }
  data_.reset(new T[static_cast<size_t>(1) << size_shift_]);
}

int GetCPUID();

template <typename T>
std::pair<T *, size_t> TLSArray<T>::AccessElementAndIndex() const {
  auto cpuid = GetCPUID();
  size_t idx;
  if (UNLIKELY(cpuid < 0)) {
    idx = Random::GetInstance()->Uniform(1 << size_shift_);
  } else {
    idx = static_cast<size_t>(cpuid & ((1 << size_shift_) - 1));
  }
  return {AccessAtCore(idx), idx};
}

static constexpr size_t INTERNAL_SIZE = 1u << 11;
static constexpr size_t MIN_BLOCK_SIZE = 1u << 17;
static constexpr size_t MAX_BLOCK_SIZE = 1u << 31;
static constexpr size_t ALIGNED_SIZE = alignof(max_align_t);
static_assert((ALIGNED_SIZE & (ALIGNED_SIZE - 1)) == 0,
              "Aligned size must be pow of 2");

class ConcurrentAllocator final {
public:
  explicit ConcurrentAllocator(size_t size = MIN_BLOCK_SIZE);
  ~ConcurrentAllocator();

  // NOCOPY
  ConcurrentAllocator(const ConcurrentAllocator &) = delete;
  ConcurrentAllocator &operator=(const ConcurrentAllocator &) = delete;

  char *Allocate(size_t bytes) {
    return AllocateImpl(bytes, false, [=] {
      assert(bytes > 0);
      if (bytes <= alloc_bytes_remaining_) {
        unaligned_alloc_ptr_ -= bytes;
        alloc_bytes_remaining_ -= bytes;
        return unaligned_alloc_ptr_;
      }
      return AllocateInternal(bytes, false);
    });
  }

  char *AllocateAligned(size_t bytes) {
    size_t rounded_up = ((bytes - 1) | (sizeof(void *) - 1)) + 1;
    assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void *) &&
           rounded_up % sizeof(void *) == 0);

    return AllocateImpl(rounded_up, false,
                        [=] { return AllocateAlignedUnsafe(rounded_up); });
  }

  size_t ApproximateMemoryUsage() const {
    std::unique_lock lock(alloc_mutex_, std::defer_lock);
    lock.lock();
    return blocks_memory_ + blocks_.capacity() * sizeof(char *) -
           alloc_bytes_remaining_ - ShardAllocatedAndUnused();
  }

  size_t MemoryAllocatedBytes() const {
    return memory_allocated_.load(std::memory_order_relaxed);
  }

  size_t AllocatedAndUnused() const {
    return alloc_allocated_and_unused_.load(std::memory_order_relaxed) +
           ShardAllocatedAndUnused();
  }

  size_t IrregularBlockNum() const {
    return irregular_block_count_.load(std::memory_order_relaxed);
  }

  size_t BlockSize() const { return block_size_; }

  bool IsInInlineBlock() const { return blocks_.empty(); }

private:
  struct Shard {
    char padding[40];
    mutable SpinMutex mutex;
    char *free_begin_;
    std::atomic<size_t> allocated_and_unused_;

    Shard() : free_begin_(nullptr), allocated_and_unused_(0) {} // NOLINT
  };

  static __thread size_t tls_cpuid;

  size_t shard_block_size_;
  TLSArray<Shard> shards_;

  // unsafe alloc
  const size_t block_size_;
  std::vector<char *> blocks_;
  size_t irregular_block_num_ = 0;
  char internal_storage_[INTERNAL_SIZE]
      __attribute__((__aligned__(alignof(max_align_t)))){};

  char *unaligned_alloc_ptr_ = nullptr;
  char *aligned_alloc_ptr_ = nullptr;
  size_t alloc_bytes_remaining_ = 0;
  size_t blocks_memory_ = 0;

  // concurrent
  mutable SpinMutex alloc_mutex_;
  std::atomic<size_t> alloc_allocated_and_unused_;
  std::atomic<size_t> memory_allocated_; // bytes
  std::atomic<size_t> irregular_block_count_;

  // unsafe
  char *AllocateAlignedUnsafe(size_t size);
  char *AllocateInternal(size_t size, bool aligned);
  char *AllocateBlock(size_t block_size);

  Shard *Repick();

  size_t ShardAllocatedAndUnused() const {
    size_t total = 0;
    for (size_t i = 0; i < shards_.Size(); ++i) {
      total += shards_.AccessAtCore(i)->allocated_and_unused_.load(
          std::memory_order_relaxed);
    }
    return total;
  }

  template <typename Func>
  char *AllocateImpl(size_t bytes, bool force_arena, const Func &func) {
    size_t cpu;
    std::unique_lock alloc_lock(alloc_mutex_, std::defer_lock);
    if (bytes > shard_block_size_ / 4 || force_arena ||
        ((cpu = tls_cpuid) == 0 &&
         !shards_.AccessAtCore(0)->allocated_and_unused_.load(
             std::memory_order_relaxed) &&
         alloc_lock.try_lock())) {
      if (!alloc_lock.owns_lock()) {
        alloc_lock.lock();
      }
      auto rv = func();
      Update();
      return rv;
    }

    // pick a shard from which to allocate
    auto s = shards_.AccessAtCore(cpu & (shards_.Size() - 1));
    if (!s->mutex.try_lock()) {
      s = Repick();
      s->mutex.lock();
    }
    std::unique_lock lock(s->mutex, std::adopt_lock);

    size_t avail = s->allocated_and_unused_.load(std::memory_order_relaxed);
    if (avail < bytes) {
      std::lock_guard reload_lock(alloc_mutex_);
      auto exact = alloc_allocated_and_unused_.load(std::memory_order_relaxed);
      assert(exact == alloc_.AllocatedAndUnused());

      if (exact >= bytes && IsInInlineBlock()) {
        auto rv = func();
        Update();
        return rv;
      }

      avail = exact >= shard_block_size_ / 2 && exact < shard_block_size_ * 2
                  ? exact
                  : shard_block_size_;
      s->free_begin_ = AllocateAlignedUnsafe(avail);
      Update();
    }
    s->allocated_and_unused_.store(avail - bytes, std::memory_order_relaxed);

    char *rv;
    if (bytes % sizeof(void *) == 0) {
      rv = s->free_begin_;
      s->free_begin_ += bytes;
    } else {
      rv = s->free_begin_ + avail - bytes;
    }
    return rv;
  }

  void Update() {
    alloc_allocated_and_unused_.store(alloc_bytes_remaining_,
                                      std::memory_order_relaxed);
    memory_allocated_.store(blocks_memory_, std::memory_order_relaxed);
    irregular_block_count_.store(irregular_block_num_,
                                 std::memory_order_relaxed);
  }
};

inline char *ConcurrentAllocator::AllocateBlock(size_t block_size) {
  blocks_.emplace_back(nullptr);
  auto block = new char[block_size];
  blocks_memory_ += block_size;
  blocks_.back() = block;
  return block;
}

} // namespace kwdbts
