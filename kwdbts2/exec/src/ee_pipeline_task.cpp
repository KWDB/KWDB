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

#include "ee_pipeline_task.h"
#include "ee_pipeline_group.h"
#include "ee_processors.h"
#include "ee_base_op.h"
#include "ee_kwthd_context.h"
#include "ee_outbound_op.h"
#include "ee_cancel_checker.h"
#include "ee_exec_pool.h"

namespace kwdbts {

PipelineTask::PipelineTask(PipelineGroup *pipeline)
  : pipeline_group_(pipeline), processors_(pipeline->processors_) { }
PipelineTask::~PipelineTask() {
  SafeDeletePointer(thd_);
}

KStatus PipelineTask::Init(kwdbContext_p ctx) {
  EnterFunc();
  ts_engine_ = ctx->ts_engine;
  fetcher_ = ctx->fetcher;
  is_parallel_pg_ = false;
  is_stop_ = false;
  relation_ctx_ = ctx->relation_ctx;
  timezone_ = ctx->timezone;
  KWThdContext *main_thd = current_thd;
  thd_ = KNEW KWThdContext();
  if (!thd_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("New KWThd failed.");
    Return(KStatus::FAIL);
  }
  thd_->Copy(main_thd);
  state_ = PS_STARTED;
  Return(KStatus::SUCCESS);
}

void PipelineTask::SetStop() {
  is_stop_ = true;
}

void PipelineTask::Wait() {
  std::unique_lock<std::mutex> l(mutex_);
}

void PipelineTask::SetOperator(BaseOperator *oper) {
  operator_ = oper;
}

void PipelineTask::AddDependency(std::shared_ptr<PipelineTask> task) {
  ++total_dependencies_;
  task->parents_.push_back(this);
  dependencies_.push_back(task.get());
}

bool PipelineTask::is_can_schedule() {
  if (!HasDependency()) {
    return true;
  }

  if (PipelineTaskState::PS_HAS_OUTPUT == state_ || PipelineTaskState::PS_FINISHED == state_) {
    return true;
  }

  bool is_can_schedule = false;
  std::lock_guard<std::mutex> l(block_mutex_);
  if (is_cancel_) {
    return true;
  }
  size_t sz = pipeline_group_->dependencies_.size();
  for (k_uint32 i = 0; i < sz; ++i) {
    is_can_schedule = false;
    size_t task_sz = pipeline_group_->dependencies_[i]->tasks_.size();
    for (k_uint32 j = 0; j < task_sz; ++j) {
      if (auto sp = pipeline_group_->dependencies_[i]->tasks_[i].lock()) {
        if (PipelineTaskState::PS_BLOCKED == sp->state_) {
          continue;
        }
        if (!sp->is_can_schedule()) {
          continue;
        }
      }

      is_can_schedule = true;
      break;
    }
    if (false == is_can_schedule) {
      break;
    }
  }

  return is_can_schedule;
}

void PipelineTask::Cancel() {
  std::lock_guard<std::mutex> l(block_mutex_);
  is_cancel_ = true;
}

k_int32 PipelineTask::GetDegree() {
  OperatorType type = operator_->Type();
  if (type!= OperatorType::OPERATOR_LOCAL_OUT_BOUND && type!= OperatorType::OPERATOR_REMOTR_OUT_BOUND) {
    return 1;
  }
  OutboundOperator *oper = dynamic_cast<OutboundOperator *>(operator_);
  return oper->GetDegree();
}

void PipelineTask::SetTable(TABLE *table) {
  table_ = table;
}

KStatus PipelineTask::Clone(kwdbContext_p ctx, k_int32 num, std::vector<std::shared_ptr<PipelineTask>> &tasks,
                                                                        std::vector<BaseOperator*> &new_operators) {
  for (k_uint32 i = 0; i < num; ++i) {
    std::shared_ptr<PipelineTask> task = std::make_shared<PipelineTask>(pipeline_group_);
    if (nullptr == task) {
      LOG_ERROR("clone pipeline task failed.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      return KStatus::FAIL;
    }
    task->is_clone_ = true;
    BaseOperator *iter = operator_->Clone();
    if (nullptr == iter) {
      return KStatus::FAIL;
    }

    EEIteratorErrCode code = iter->Init(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      SafeDeletePointer(iter);
      return KStatus::FAIL;
    }

    std::vector<BaseOperator *> parents = operator_->GetParent();
    for (auto parent : parents) {
      iter->AddParent(parent);
    }

    task->Init(ctx);
    task->operator_ = iter;
    for (auto it : parents_) {
      it->AddDependency(task);
    }

    tasks.push_back(task);
    pipeline_group_->AddPipelineTask(task);
    new_operators.push_back(iter);
  }

  return KStatus::SUCCESS;
}

void PipelineTask::Close(kwdbContext_p ctx, const EEIteratorErrCode &code) {
  k_uint32 remd = pipeline_group_->FinishTask(code, EEPgErrorInfo::GetPgErrorInfo());
  if (0 == remd) {
    operator_->PushFinish(pipeline_group_->code_, 0, pipeline_group_->pg_info_);
  } else {
    operator_->PrintFinishLog();
  }

  if (thd_) {
    thd_->Reset();
  }

  if (is_clone_) {
    operator_->Close(ctx);
  }

  Finish(ctx);
}

void PipelineTask::Blocked(kwdbContext_p ctx) {
  is_running_ = false;
  state_ = PipelineTaskState::PS_BLOCKED;
  ExecPool::GetInstance().PushBlockedTask(shared_from_this());
}

void PipelineTask::Finish(kwdbContext_p ctx) {
  is_running_ = false;
  state_ = PipelineTaskState::PS_FINISHED;
}

void PipelineTask::UpdateStartTime() {
  start_time_ = std::chrono::high_resolution_clock::now();
}

int64_t PipelineTask::DurationTimes() {
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_time_);

  return duration.count();
}

void PipelineTask::Run(kwdbContext_p ctx) {
  std::unique_lock<std::mutex> l(mutex_);
  if (is_stop_) {
    return;
  }
  ctx->b_is_cancel = &(processors_->b_is_cancel_);
  is_running_ = true;
  EEPgErrorInfo::ResetPgErrorInfo();
  ctx->ts_engine = ts_engine_;
  ctx->relation_ctx = relation_ctx_;
  ctx->fetcher = fetcher_;
  ctx->timezone = timezone_;
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  current_thd = thd_;
  thd_->SetPipelineTask(this);
  auto &instance = ExecPool::GetInstance();
  auto &g_error_info = EEPgErrorInfo::GetPgErrorInfo();
  try {
    if (is_stop_ || CheckCancel(ctx) != SUCCESS) {
      Close(ctx, code);
      return;
    }

    if (PS_STARTED == state_) {
      code = operator_->Start(ctx);
      if (code != EEIteratorErrCode::EE_OK || g_error_info.code > 0 || CheckCancel(ctx) != SUCCESS) {
        Close(ctx, code);
        return;
      }
    }

    state_ = PipelineTaskState::PS_HAS_OUTPUT;
    BaseOperator *source = pipeline_group_->GetSource();
    BaseOperator *sink = pipeline_group_->GetSink();
    UpdateStartTime();

    while (true) {
      if (is_stop_ || CheckCancel(ctx) != KStatus::SUCCESS) {
        code = EEIteratorErrCode::EE_ERROR;
        Close(ctx, code);
        break;
      }

      if ((source && !source->HasOutput()) || (sink && !sink->NeedInput())) {
        Blocked(ctx);
        break;
      }

      DataChunkPtr ptr = nullptr;
      code = operator_->Next(ctx, ptr);
      if (EEIteratorErrCode::EE_NEXT_CONTINUE == code) {
        int64_t duration = DurationTimes();
        if (duration > YIELD_MAX_TIME_SPENT_NS) {
          if (instance.IsEmpty()) {
            UpdateStartTime();
            continue;
          } else {
            Blocked(ctx);
          }
          break;
        } else {
          continue;
        }
      }

      if (EEIteratorErrCode::EE_QUEUE_FULL == code) {
        Blocked(ctx);
        break;
      }

      if (EEIteratorErrCode::EE_OK != code || g_error_info.code > 0 || is_stop_) {
        Close(ctx, code);
        break;
      }
      int64_t duration = DurationTimes();
      if (duration > YIELD_MAX_TIME_SPENT_NS) {
        if (instance.IsEmpty()) {
          UpdateStartTime();
        } else {
          Blocked(ctx);
          break;
        }
      }
    }
  } catch (const std::bad_alloc &e) {
    LOG_ERROR("throw bad_alloc exception: %s.", e.what());
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
    Close(ctx, EEIteratorErrCode::EE_ERROR);
  } catch (const std::runtime_error &e) {
    LOG_ERROR("throw runtime_error exception: %s.", e.what());
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
    Close(ctx, EEIteratorErrCode::EE_ERROR);
  } catch (const std::exception &e) {
    LOG_ERROR("throw other exception: %s.", e.what());
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
    Close(ctx, EEIteratorErrCode::EE_ERROR);
  } catch (...) {
    LOG_ERROR("throw unknown exception.");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "unknown exception.");
    Close(ctx, EEIteratorErrCode::EE_ERROR);
  }
}

}  // namespace kwdbts
