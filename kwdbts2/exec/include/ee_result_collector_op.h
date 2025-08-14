// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details. Created by liguoliang on 2022/07/18.
#pragma once

#include "ee_base_op.h"

#include <condition_variable>
#include <mutex>

#include "ee_cancel_checker.h"

namespace kwdbts {

class ResultCollectorOperator : public BaseOperator {
 public:
  ResultCollectorOperator() { }

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  void PushFinish(EEIteratorErrCode code, k_int32 stream_id, const EEPgErrorInfo& pgInfo) override;

  KStatus BuildPipeline(PipelineGroup *pipeline, Processors *processor) override;

  enum OperatorType Type() override { return OperatorType::OPERATOR_RESULT_COLLECTOR; }

 private:
  bool is_finished_{false};
  std::mutex mutex_;
  std::condition_variable wait_cond_;
  EEPgErrorInfo pg_info_;
  EEIteratorErrCode code_{EEIteratorErrCode::EE_END_OF_RECORD};
};


}  // namespace kwdbts
