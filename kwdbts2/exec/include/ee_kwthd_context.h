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
#include <pthread.h>
#include <list>
#include <utility>

#include "ee_row_batch.h"
#include "ee_parallel_group.h"
#include "kwdb_type.h"

namespace kwdbts {
class KWThdContext {
 private:
  RowBatchPtr row_batch_{nullptr};
  ParallelGroup* parallel_group_{nullptr};
  k_uint64 thd_id_{0};
  IChunk* data_chunk_{nullptr};

 public:
  KWThdContext() { thd_id_ = pthread_self(); }
  ~KWThdContext() { Reset(); }

  void SetRowBatch(RowBatchPtr ptr) {
    row_batch_ = ptr;
  }

  void SetParallelGroup(ParallelGroup* ptr) {
    parallel_group_ = ptr;
  }

  void Reset() {
    if (row_batch_) {
      row_batch_.reset();
    }
  }
  RowBatchPtr& GetRowBatch() { return row_batch_; }

  /**
   * Only used to read the data in row_batch_ structure.
   * The caller should ensure the row_batch_ shared_ptr is valid during the read process.
   * @return the original pointer for row_batch_ structure.
   */
  RowBatch* GetRowBatchOriginalPtr() { return row_batch_.get(); }
  ParallelGroup* GetParallelGroup() { return parallel_group_; }
  k_uint32 GetDegree() {
    if (parallel_group_) {
      return parallel_group_->GetDegree();
    }
    return 1;
  }
  k_uint64 GetThdID() { return thd_id_; }
  void SetDataChunk(IChunk *ptr) { data_chunk_ = ptr; }
  IChunk* GetDataChunk() { return data_chunk_; }

 public:
  static thread_local KWThdContext *thd_;
};

#define current_thd KWThdContext::thd_

};  // namespace kwdbts