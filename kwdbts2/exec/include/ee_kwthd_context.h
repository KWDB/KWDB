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
  RowBatch* row_batch_{nullptr};
  ParallelGroup* parallel_group_{nullptr};
  k_uint64 thd_id_{0};
  IChunk* data_chunk_{nullptr};
  TsNextRetState next_state_;
  k_int64 *command_limit_{nullptr};
  std::atomic<k_int64> *count_for_limit_{nullptr};

 public:
  KWThdContext() { thd_id_ = pthread_self(); }
  ~KWThdContext() { Reset(); }

  void SetRowBatch(RowBatch* ptr) {
    row_batch_ = ptr;
  }

  void SetParallelGroup(ParallelGroup* ptr) {
    parallel_group_ = ptr;
  }

  void Reset() {}
  RowBatch* GetRowBatch() { return row_batch_; }
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
  void SetPgEncode(TsNextRetState nextState) { next_state_ = nextState; }
  TsNextRetState GetPgEncode() { return next_state_; }
  k_int64* GetCommandLimit() { return command_limit_; }
  void SetCommandLimit(k_int64* limit) { command_limit_ = limit; }
  std::atomic<k_int64>* GetCountForLimit() { return count_for_limit_; }
  void SetCountForLimit(std::atomic<k_int64>* count_for_limit) {
    count_for_limit_ = count_for_limit;
  }

  void Copy(KWThdContext *thd) {
    SetPgEncode(thd->GetPgEncode());
    SetCommandLimit(thd->GetCommandLimit());
    SetCountForLimit(thd->GetCountForLimit());
    wtyp_ = thd->wtyp_;
    window_field_ = thd->window_field_;
  }

 public:
  static thread_local KWThdContext *thd_;
  bool auto_quit_{false};
  WindowGroupType wtyp_{WindowGroupType::EE_WGT_UNKNOWN};
  Field* window_field_{nullptr};
};

#define current_thd KWThdContext::thd_

};  // namespace kwdbts
