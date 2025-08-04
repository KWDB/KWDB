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

// Lightweight spinlock.
class SpinLock {
 public:
  SpinLock() {}

  // Acquires the lock, spins until the lock becomes available
  void lock() {
    if (!try_lock()) {
      slow_acquire();
    }
  }

  void unlock() {
    // Memory barrier here. All updates before the unlock need to be made
    // visible.
    __sync_synchronize();
    DCHECK(locked_);
    locked_ = false;
  }

  // Tries to acquire the lock
  inline k_bool try_lock() { return __sync_bool_compare_and_swap(&locked_, false, true); }

  void dcheck_locked() { DCHECK(locked_); }

 private:
  // Out-of-line definition of the actual spin loop. The primary goal is to have
  // the actual lock method as short as possible to avoid polluting the i-cache
  // with unnecessary instructions in the non-contested case.
  void slow_acquire() {
    while (true) {
      if (try_lock()) {
        return;
      }
      for (k_int32 i = 0; i < NUM_SPIN_CYCLES; ++i) {
#if (defined(__i386) || defined(__x86_64__))
        asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
        // A "yield" instruction in aarch64 is essentially a nop, and does
        // not cause enough delay to help backoff. "isb" is a barrier that,
        // especially inside a loop, creates a small delay without consuming
        // ALU resources.  Experiments shown that adding the isb instruction
        // improves stability and reduces result jitter. Adding more delay
        // to the UT_RELAX_CPU than a single isb reduces performance.
        asm volatile("isb\n" ::: "memory");
#endif
      }
      if (try_lock()) {
        return;
      }
      sched_yield();
    }
  }

  // In typical spin lock implements, we want to spin (and keep the core fully
  // busy), for some number of cycles before yielding. Consider these three
  // cases:
  //  1) lock is un-contended - spinning doesn't kick in and has no effect.
  //  2) lock is taken by another thread and that thread finishes quickly.
  //  3) lock is taken by another thread and that thread is slow (e.g. scheduled
  //  away).
  //
  // In case 3), we'd want to yield so another thread can do work. This thread
  // won't be able to do anything useful until the thread with the lock runs
  // again. In case 2), we don't want to yield (and give up our scheduling time
  // slice) since we will get to run soon after. To try to get the best of
  // everything, we will busy spin for a while before yielding to another
  // thread.
  static const k_int32 NUM_SPIN_CYCLES = 70;
  k_bool locked_{false};
};

}  // end namespace kwdbts
