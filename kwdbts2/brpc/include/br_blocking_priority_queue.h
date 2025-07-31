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

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <vector>

#include "kwdb_type.h"

namespace kwdbts {

// Fixed capacity FIFO queue, where both BlockingGet and BlockingPut
// operations block if the queue is empty or full, respectively.
template <typename T>
class BlockingPriorityQueue {
 public:
  explicit BlockingPriorityQueue(k_size_t max_elements) : max_element_(max_elements) {}
  ~BlockingPriorityQueue() { Shutdown(); }

  // Return false iff has been shutdown.
  k_bool BlockingGet(T* out) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    get_cv_.wait(unique_lock, [this]() { return !heap_.empty() || shutdown_; });
    if (!heap_.empty()) {
      AdjustPriorityIfNeeded();
      std::pop_heap(heap_.begin(), heap_.end());
      if constexpr (std::is_move_assignable_v<T>) {
        *out = std::move(heap_.back());
      } else {
        *out = heap_.back();
      }
      heap_.pop_back();
      ++upgrade_counter_;
      unique_lock.unlock();
      put_cv_.notify_one();
      return true;
    }
    return false;
  }

  // Return false iff has been shutdown or empty.
  k_bool NonBlockingGet(T* out) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    if (!heap_.empty()) {
      AdjustPriorityIfNeeded();
      std::pop_heap(heap_.begin(), heap_.end());
      if constexpr (std::is_move_assignable_v<T>) {
        *out = std::move(heap_.back());
      } else {
        *out = heap_.back();
      }
      heap_.pop_back();
      ++upgrade_counter_;
      unique_lock.unlock();
      put_cv_.notify_one();
      return true;
    }
    return false;
  }

  // Return false iff has been shutdown.
  k_bool BlockingPut(const T& val) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    put_cv_.wait(unique_lock, [this]() { return heap_.size() < max_element_ || shutdown_; });
    if (!shutdown_) {
      assert(heap_.size() < max_element_);
      heap_.emplace_back(val);
      std::push_heap(heap_.begin(), heap_.end());
      unique_lock.unlock();
      get_cv_.notify_one();
      return true;
    }
    return false;
  }

  // force push val
  void ForcePut(T&& val) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    if (!shutdown_) {
      heap_.emplace_back(std::move(val));
      std::push_heap(heap_.begin(), heap_.end());
      unique_lock.unlock();
      get_cv_.notify_one();
    }
  }

  // Return false iff has been shutdown.
  k_bool BlockingPut(T&& val) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    put_cv_.wait(unique_lock, [this]() { return heap_.size() < max_element_ || shutdown_; });
    if (!shutdown_) {
      assert(heap_.size() < max_element_);
      heap_.emplace_back(std::move(val));
      std::push_heap(heap_.begin(), heap_.end());
      unique_lock.unlock();
      get_cv_.notify_one();
      return true;
    }
    return false;
  }

  // Return false if queue full or has been shutdown.
  k_bool TryPut(const T& val) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    if (heap_.size() < max_element_ && !shutdown_) {
      heap_.emplace_back(val);
      std::push_heap(heap_.begin(), heap_.end());
      unique_lock.unlock();
      get_cv_.notify_one();
      return true;
    }
    return false;
  }

  // Return false if queue full or has been shutdown.
  k_bool TryPut(T&& val) {
    std::unique_lock<std::mutex> unique_lock(lock_);
    if (heap_.size() < max_element_ && !shutdown_) {
      heap_.emplace_back(std::move(val));
      std::push_heap(heap_.begin(), heap_.end());
      unique_lock.unlock();
      get_cv_.notify_one();
      return true;
    }
    return false;
  }

  // Shut down the queue. Wakes up all threads waiting on blocking_get or
  // blocking_put.
  void Shutdown() {
    {
      std::lock_guard<std::mutex> guard(lock_);
      if (shutdown_) return;
      shutdown_ = true;
    }

    get_cv_.notify_all();
    put_cv_.notify_all();
  }

  k_size_t GetCapacity() const { return max_element_; }
  k_uint32 GetSize() const {
    std::unique_lock<std::mutex> l(lock_);
    return heap_.size();
  }

 private:
  // REQUIRES: lock_ has been acquired.
  __attribute__((always_inline)) void AdjustPriorityIfNeeded() {
    if (upgrade_counter_ <= 512) {
      return;
    }
    const k_size_t n = heap_.size();
    for (k_size_t i = 0; i < n; i++) {
      ++heap_[i];
    }
    std::make_heap(heap_.begin(), heap_.end());
    upgrade_counter_ = 0;
  }

  const k_int32 max_element_;
  mutable std::mutex lock_;
  std::condition_variable get_cv_;  // 'get' callers wait on this
  std::condition_variable put_cv_;  // 'put' callers wait on this
  std::vector<T> heap_;
  k_int32 upgrade_counter_ = 0;
  k_bool shutdown_ = false;
};

}  // namespace kwdbts
