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

#include <memory>
#include <vector>
#include "kwdb_type.h"
#include "cm_kwdb_context.h"
#include "ee_global.h"

namespace kwdbts {

class BaseOperator;
class PipelineTask;
class Processors;

class PipelineGroup {
 public:
  explicit PipelineGroup(Processors *processors);

  ~PipelineGroup();

  void SetPipelineOperator(BaseOperator *oper);

  void SetSink(BaseOperator *sink);

  BaseOperator *GetSink() { return sink_; }

  void SetSource(BaseOperator *source);

  BaseOperator *GetSource() { return source_; }

  void AddOperator(BaseOperator *oper) {
    operators_.push_back(oper);
  }

  k_uint32 FinishTask(EEIteratorErrCode code, const EEPgErrorInfo& pgInfo);

  std::vector<BaseOperator *>& GetOperator() { return operators_; }

  bool IsParallel();

  k_int32 GetDegree();

  bool HasOperator() { return operator_ != nullptr; }

  void GetPipelines(std::vector<PipelineGroup *> &pipelines, bool include);

  std::vector<PipelineGroup *> GetDependencies() {return dependencies_; }

  std::shared_ptr<PipelineTask> CreateTask(kwdbContext_p ctx);

  void AddPipelineTask(const std::shared_ptr<PipelineTask> &task);

  void SetDependenciePipeline(PipelineGroup *child);

 public:
  Processors *processors_{nullptr};
  BaseOperator *operator_{nullptr};
  std::vector<PipelineGroup *> dependencies_;
  std::vector<std::weak_ptr<PipelineTask> > tasks_;
  k_uint32 total_task_{0};
  k_uint32 finish_task_{0};
  std::mutex mutex_;
  EEPgErrorInfo pg_info_;
  EEIteratorErrCode code_{EEIteratorErrCode::EE_END_OF_RECORD};

 private:
  BaseOperator *sink_{nullptr};
  std::vector<BaseOperator *> operators_;
  BaseOperator *source_{nullptr};
};



}  // namespace kwdbts
