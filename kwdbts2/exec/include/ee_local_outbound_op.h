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

#include <queue>
#include <vector>
#include "ee_outbound_op.h"
#include "ee_operator_type.h"

namespace kwdbts {

class LocalOutboundOperator : public OutboundOperator {
 public:
  using OutboundOperator::OutboundOperator;
  LocalOutboundOperator(const LocalOutboundOperator& other, int32_t processor_id);

  ~LocalOutboundOperator() {}

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  KStatus PushChunk(DataChunkPtr &chunk, k_int32 stream_id, EEIteratorErrCode code) override;

  void PushFinish(EEIteratorErrCode code, k_int32 stream_id, const EEPgErrorInfo &pgInfo) override;

  void PrintFinishLog() override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr &chunk) override;

  KStatus CreateInputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_LOCAL_OUT_BOUND; }

  BaseOperator* Clone() override;

  k_uint32 total_push_count_{0};
  k_uint32 stream_id_{0};
};

}  // namespace kwdbts
