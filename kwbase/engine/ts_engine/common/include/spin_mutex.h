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
class spin_mutex {
 private:
  std::atomic<bool> flag = false;
  void Wait(uint64_t &spin_count) {
    if (spin_count < 4096) {
      ++spin_count;
      // asm_volatile_pause();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
  }

 public:
  bool try_lock() noexcept {
    if (flag.load(std::memory_order_relaxed)) {
      return false;
    }
    return !flag.exchange(true, std::memory_order_acquire);
  }
  void lock() noexcept {
    uint64_t spin_count = 0;
    while (!try_lock()) {
      Wait(spin_count);
    }
  }

  void unlock() noexcept { flag.store(false, std::memory_order_release); }
};
