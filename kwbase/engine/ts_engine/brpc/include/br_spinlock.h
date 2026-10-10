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

#include "butil/logging.h"
#include "kwdb_type.h"

namespace kwdbts {

class SpinLock {
 private:
  static const k_int32 NUM_SPIN_CYCLES = 70;
  k_bool locked_{false};

  void slow_acquire() {
    while (true) {
      if (try_lock()) {
        return;
      }
      for (k_int32 i = 0; i < NUM_SPIN_CYCLES; ++i) {
#if (defined(__i386) || defined(__x86_64__))
        asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
        asm volatile("isb\n" ::: "memory");
#endif
      }
      if (try_lock()) {
        return;
      }
      sched_yield();
    }
  }

 public:
  SpinLock() {}

  inline k_bool try_lock() { return __sync_bool_compare_and_swap(&locked_, false, true); }
  void dcheck_locked() { DCHECK(locked_); }

  void unlock() {
    __sync_synchronize();
    DCHECK(locked_);
    locked_ = false;
  }
  void lock() {
    if (!try_lock()) {
      slow_acquire();
    }
  }
};

}  // namespace kwdbts
