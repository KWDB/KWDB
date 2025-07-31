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

#include "ee_result_collector_op.h"
#include "ee_pipeline_group.h"
#include "ee_kwthd_context.h"
#include "ee_global.h"

namespace kwdbts {

EEIteratorErrCode ResultCollectorOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = childrens_[0]->Init(ctx);
  output_encoding_ = true;
  Return(code);
}

EEIteratorErrCode ResultCollectorOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = childrens_[0]->Start(ctx);
  Return(code);
}

EEIteratorErrCode ResultCollectorOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();

  EEIteratorErrCode code = childrens_[0]->Next(ctx, chunk);
  KWThdContext *thd = current_thd;
  if (nullptr != chunk) {
    if (EEPgErrorInfo::IsError()) {
      code = EEIteratorErrCode::EE_ERROR;
    } else {
     total_rows_ += chunk->Count();
     OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, thd, chunk);
    }
  }
  Return(code);
}

EEIteratorErrCode ResultCollectorOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Reset(ctx));
}

EEIteratorErrCode ResultCollectorOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Close(ctx));
}

KStatus ResultCollectorOperator::BuildPipeline(PipelineGroup *pipeline, Processors *processor) {
  pipeline->SetPipelineOperator(this);
  // pipeline->SetSource(this);
  return childrens_[0]->BuildPipeline(pipeline, processor);
}

}  // namespace kwdbts

