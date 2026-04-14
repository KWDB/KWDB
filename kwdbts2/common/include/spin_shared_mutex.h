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
#include <atomic>
#include <thread>

namespace kwdbts {

class spin_shared_mutex {
 private:
  // alignas(64) is used to ensure that the lock is aligned to 64 bytes, which is the cache line size on most modern
  // CPUs. This helps to avoid false sharing and improve performance.
  alignas(64) std::atomic<bool> w_lock{false};
  alignas(64) std::atomic<int> r_count{0};

  void IncAndSleep(uint64_t &spin_count) {
    if (spin_count < 4096) {
      ++spin_count;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
  }

 public:
  void lock() noexcept {
    uint64_t spin_count = 0;
    bool e = false;
    while (!w_lock.compare_exchange_strong(e, true, std::memory_order_acq_rel)) {
      e = false;
      IncAndSleep(spin_count);
    }
    while (r_count.load(std::memory_order_acquire) != 0) {
      IncAndSleep(spin_count);
    }
  }
  bool try_lock() noexcept {
    if (w_lock.load(std::memory_order_acquire) || r_count.load(std::memory_order_acquire) != 0) {
      return false;
    }
    bool e = false;
    if (!w_lock.compare_exchange_strong(e, true, std::memory_order_acquire, std::memory_order_relaxed)) {
      return false;
    }
    if (r_count.load(std::memory_order_acquire) != 0) {
      w_lock.store(false, std::memory_order_relaxed);
      return false;
    }
    return true;
  }
  void unlock() noexcept { w_lock.store(false, std::memory_order_release); }

  void lock_shared() noexcept {
    uint64_t spin_count = 0;
    while (true) {
      if (w_lock.load(std::memory_order_relaxed)) {
        IncAndSleep(spin_count);
        continue;
      }
      r_count.fetch_add(1, std::memory_order_acq_rel);
      if (w_lock.load(std::memory_order_acquire)) {
        r_count.fetch_sub(1, std::memory_order_relaxed);
        IncAndSleep(spin_count);
        continue;
      }
      break;
    }
  }
  bool try_lock_shared() noexcept {
    if (w_lock.load(std::memory_order_relaxed)) {
      return false;
    }
    r_count.fetch_add(1, std::memory_order_acq_rel);
    if (w_lock.load(std::memory_order_acquire)) {
      r_count.fetch_sub(1, std::memory_order_relaxed);
      return false;
    }
    return true;
  }
  void unlock_shared() noexcept { r_count.fetch_sub(1, std::memory_order_acq_rel); }
};

}  // namespace kwdbts
