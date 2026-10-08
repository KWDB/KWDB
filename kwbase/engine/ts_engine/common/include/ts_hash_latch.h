// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

#include <vector>
#include "lt_latch.h"
#include "lt_rw_latch.h"

class TsHashLatch {
 private:
  size_t bucket_num_;
  KLatch** latches_;

 public:
  explicit TsHashLatch(size_t bucket_num, latch_id_t latch_id) : bucket_num_(bucket_num) {
    latches_ = new KLatch*[bucket_num];
    for (int i = 0; i < bucket_num; ++i) {
      latches_[i] = new KLatch(latch_id);
    }
  }
  ~TsHashLatch() {
    for (int i = 0; i < bucket_num_; ++i) {
      delete latches_[i];
    }
    delete[] latches_;
  }

  inline void Lock(uint64_t id) {
    MUTEX_LOCK(latches_[id % bucket_num_]);
  }

  inline void Unlock(uint64_t id) {
    MUTEX_UNLOCK(latches_[id % bucket_num_]);
  }
};

class TsHashRWLatch {
 private:
  size_t bucket_num_;
  KRWLatch** rw_latches_;

 public:
  explicit TsHashRWLatch(size_t bucket_num, rwlatch_id_t rw_latch_id) : bucket_num_(bucket_num) {
    rw_latches_ = new KRWLatch*[bucket_num];
    for (int i = 0; i < bucket_num; ++i) {
      rw_latches_[i] = new KRWLatch(rw_latch_id);
    }
  }
  ~TsHashRWLatch() {
    for (int i = 0; i < bucket_num_; ++i) {
      delete rw_latches_[i];
    }
    delete[] rw_latches_;
  }

  inline void RdLock(uint64_t id) {
    RW_LATCH_S_LOCK(rw_latches_[id % bucket_num_]);
  }

  inline void WrLock(uint64_t id) {
    RW_LATCH_X_LOCK(rw_latches_[id % bucket_num_]);
  }

  inline void Unlock(uint64_t id) {
    RW_LATCH_UNLOCK(rw_latches_[id % bucket_num_]);
  }
};
