// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//

// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

#include <mutex>

#include <memory>
#include <vector>

#include "ee_global.h"
#include "ee_task.h"

namespace kwdbts {

class PipelineGroup;
class BaseOperator;
class TABLE;
class KWThdContext;
class Processors;
class DmlExec;

enum PipelineTaskState {
  PS_NOT_RUNNING = 0,
  PS_STARTED,
  PS_HAS_OUTPUT,
  PS_BLOCKED,
  PS_NEXT,
  PS_FINISHED,
};

class PipelineTask : public ExecTask {
 public:
  explicit PipelineTask(PipelineGroup *pipeline);
  ~PipelineTask();

  void SetStop();

  void Wait();

  KStatus Init(kwdbContext_p ctx);

  void SetOperator(BaseOperator *oper);

  void AddDependency(std::shared_ptr<PipelineTask> task);

  bool is_can_schedule() override;

  void Cancel();

  k_int32 GetDegree();

  bool isRunning() { return is_running_; }

  void SetInExecPool(bool in_exec_pool) { in_exec_pool_ = in_exec_pool; }

  bool isInExecPool() { return in_exec_pool_; }

  bool HasDependency() { return total_dependencies_ > 0; }

  void SetTable(TABLE *table);

  KStatus Clone(kwdbContext_p ctx, k_int32 num,
                std::vector<std::shared_ptr<PipelineTask>> &tasks,
                std::vector<BaseOperator *> &new_operators);

  void Close(kwdbContext_p ctx, const EEIteratorErrCode &code);

  void Blocked(kwdbContext_p ctx);
  void Finish(kwdbContext_p ctx);

  void UpdateStartTime();

  int64_t DurationTimes();

  void Run(kwdbContext_p ctx);

 private:
  BaseOperator *operator_{nullptr};
  PipelineGroup *pipeline_group_{nullptr};
  Processors *processors_{nullptr};
  std::vector<PipelineTask *> parents_;
  std::vector<PipelineTask *> dependencies_;
  k_int32 total_dependencies_{0};
  std::atomic_int32_t finish_dependencies_{0};
  TABLE *table_{nullptr};
  void *ts_engine_{nullptr};
  void *fetcher_{nullptr};
  k_bool is_parallel_pg_{KFALSE};
  std::atomic_bool is_stop_{false};
  std::atomic_bool is_running_{false};
  std::atomic_bool is_cancel_{false};
  std::atomic_bool in_exec_pool_{false};
  k_uint64 relation_ctx_{0};
  k_int8 timezone_;
  char timezone_name_[TIMEZONE_MAX_LEN]{0};  // IANA timezone name for DST support
  k_bool use_dst_{false};      // DST flag for timezone handling
  KWThdContext *thd_{nullptr};
  bool is_clone_{false};
  std::mutex mutex_;
  std::mutex block_mutex_;
  PipelineTaskState state_{PS_NOT_RUNNING};
  static constexpr int64_t YIELD_MAX_TIME_SPENT_NS = 100'000'000L;  // 100ms
  std::chrono::_V2::system_clock::time_point start_time_;
  DmlExec* dml_exec_handle_{nullptr};
};

}  // namespace kwdbts
