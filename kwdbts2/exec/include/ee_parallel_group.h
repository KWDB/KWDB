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
// #include <memory>

// #include "ee_base_op.h"
// #include "ee_data_chunk.h"
// #include "ee_global.h"
// #include "ee_task.h"

// namespace kwdbts {
// class KWThdContext;
// class SynchronizerOperator;
// class ParallelGroup;
// typedef std::shared_ptr<ParallelGroup> ParallelGroupPtr;

// enum ParallelState {
//   PS_TASK_INIT = 0,
//   PS_TASK_RUN,
//   PS_TASK_PAUSE,
//   PS_TASK_CLOSE
// };

// class ParallelGroup : public ExecTask {
//  private:
//   BaseOperator *iterator_{nullptr};
//   SynchronizerOperator *sparent_{nullptr};
//   TABLE *table_{nullptr};
//   void *ts_engine_{nullptr};
//   void *fetcher_{nullptr};
//   k_bool is_parallel_pg_{KFALSE};
//   bool is_stop_{false};
//   k_uint64 relation_ctx_{0};
//   k_int32 degree_{1};
//   ParallelState ps_{PS_TASK_INIT};
//   DataChunkPtr  chunk_;
//   KWThdContext *thd_{nullptr};
//   k_int32 repeat_{0};
//   k_int32 index_{0};
//   k_int8 timezone_;

//  public:
//   ParallelGroup() {}
//   ~ParallelGroup();
//   SynchronizerOperator *GetParent() { return sparent_; }
//   k_bool GetParallel() { return is_parallel_pg_; }
//   k_int32 GetDegree() { return degree_; }
//   void SetParallel(k_bool parallel) { is_parallel_pg_ = parallel; }
//   void SetIterator(BaseOperator *it) { iterator_ = it; }
//   void SetParent(SynchronizerOperator *parent) { sparent_ = parent; }
//   void SetTable(TABLE *table) { table_ = table; }
//   void SetDegree(k_int32 degree) { degree_ = degree; }
//   KStatus Init(kwdbContext_p ctx);
//   void Run(kwdbContext_p ctx);
//   void *GetTsEngine() { return ts_engine_; }
//   void Stop() { is_stop_ = true; }
//   KStatus TimeRun() override;
//   void SetIndex(k_int32 index) { index_ = index; }

//  private:
//   inline void Close(kwdbContext_p ctx, const EEIteratorErrCode &code);
//   inline void Pause();
// };
// };  // namespace kwdbts
