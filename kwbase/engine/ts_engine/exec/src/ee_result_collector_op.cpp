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
  // output_encoding_ = true;
  Return(code);
}

EEIteratorErrCode ResultCollectorOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  // EEIteratorErrCode code = childrens_[0]->Start(ctx);
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode ResultCollectorOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();

  std::unique_lock<std::mutex> l(mutex_);
  while (true) {
    if (is_finished_) {
      if (pg_info_.code > 0) {
        EEPgErrorInfo::SetPgErrorInfo(pg_info_.code, pg_info_.msg);
      }
      // LOG_ERROR("ResultCollectorOperator::Next is finished. code = %s, pginfo.code = %d, pginfo.msg = %s",
      //                 EEIteratorErrCodeToString(code_).c_str(), pg_info_.code, pg_info_.msg);
      Return(code_);
    }

    wait_cond_.wait_for(l, std::chrono::seconds(2));
  }
}

EEIteratorErrCode ResultCollectorOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Reset(ctx));
}

EEIteratorErrCode ResultCollectorOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Return(childrens_[0]->Close(ctx));
}

void ResultCollectorOperator::PushFinish(EEIteratorErrCode code, k_int32 stream_id, const EEPgErrorInfo& pgInfo) {
  std::lock_guard<std::mutex> l(mutex_);
  is_finished_ = true;
  code_ = code;
  pg_info_ = pgInfo;
  wait_cond_.notify_one();
}

KStatus ResultCollectorOperator::BuildPipeline(PipelineGroup *pipeline, Processors *processor) {
  pipeline->SetPipelineOperator(this);
  return childrens_[0]->BuildPipeline(pipeline, processor);
}

}  // namespace kwdbts

