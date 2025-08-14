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

#include "ee_pipeline_group.h"
#include "kwdb_type.h"
#include "ee_pipeline_task.h"
#include "ee_outbound_op.h"

namespace kwdbts {

PipelineGroup::PipelineGroup(Processors *processors) : processors_(processors) { }

PipelineGroup::~PipelineGroup() { }

void PipelineGroup::SetPipelineOperator(BaseOperator *oper) {
  operator_ = oper;
}

void PipelineGroup::SetSink(BaseOperator *sink) {
  sink_ = sink;
}

void PipelineGroup::SetSource(BaseOperator *source) {
  source_ = source;
}

k_uint32 PipelineGroup::FinishTask(EEIteratorErrCode code, const EEPgErrorInfo& pgInfo) {
  std::unique_lock<std::mutex> lock(mutex_);
  finish_task_++;
  if (code != EEIteratorErrCode::EE_OK &&
      code != EEIteratorErrCode::EE_END_OF_RECORD &&
      code != EEIteratorErrCode::EE_TIMESLICE_OUT) {
    code_ = code;
  }
  if (pgInfo.code > 0) {
    pg_info_ = pgInfo;
  }
  return total_task_ - finish_task_;
}

bool PipelineGroup::IsParallel() {
  enum OperatorType type = operator_->Type();
  if (type != OperatorType::OPERATOR_LOCAL_OUT_BOUND && type != OperatorType::OPERATOR_REMOTR_OUT_BOUND) {
    return false;
  }

  OutboundOperator *oper = dynamic_cast<OutboundOperator *>(operator_);
  k_int32 degree = oper->GetDegree();
  if (degree <= 1) {
    return false;
  }

  return true;
}

k_int32 PipelineGroup::GetDegree() {
  if (nullptr == sink_) {
    return 1;
  }

  enum OperatorType type = sink_->Type();
  if (type != OperatorType::OPERATOR_LOCAL_OUT_BOUND && type != OperatorType::OPERATOR_REMOTR_OUT_BOUND) {
    return 1;
  }

  OutboundOperator *oper = dynamic_cast<OutboundOperator *>(sink_);
  return oper->GetDegree();
}

void PipelineGroup::Cancel() {
  for (auto task : tasks_) {
    if (auto sp = task.lock()) {
      sp->Cancel();
    }
  }
}

void PipelineGroup::GetPipelines(std::vector<PipelineGroup *> &pipelines, bool include) {
  if (include) {
    pipelines.push_back(this);
  }

  size_t sz = dependencies_.size();
  for (k_uint32 i = 0; i < sz; ++i) {
    dependencies_[i]->GetPipelines(pipelines, true);
  }
}

void PipelineGroup::SetDependenciePipeline(PipelineGroup *child) {
  dependencies_.push_back(child);
}

std::shared_ptr<PipelineTask> PipelineGroup::CreateTask(kwdbContext_p ctx) {
  std::shared_ptr<PipelineTask> task = std::make_shared<PipelineTask>(this);
  if (nullptr == task) {
    LOG_ERROR("Create pipeline task failed.");
    return nullptr;
  }

  KStatus ret = task->Init(ctx);
  if (ret != KStatus::SUCCESS) {
    task.reset();
    return nullptr;
  }

  task->SetOperator(operator_);
  tasks_.push_back(task);
  ++total_task_;

  return task;
}

void PipelineGroup::AddPipelineTask(const std::shared_ptr<PipelineTask> &task) {
  tasks_.push_back(task);
  ++total_task_;
}

}   // namespace kwdbts
