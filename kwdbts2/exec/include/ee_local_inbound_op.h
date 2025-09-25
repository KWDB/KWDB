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

#include <deque>
#include <vector>

#include "ee_inbound_op.h"
#include "ee_operator_type.h"

namespace kwdbts {

class LocalInboundOperator : public InboundOperator {
 public:
  using InboundOperator::InboundOperator;
  virtual ~LocalInboundOperator() = default;
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  void PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                  const EEPgErrorInfo& pgInfo) override;

  KStatus PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                    EEIteratorErrCode code = EEIteratorErrCode::EE_OK) override;
  KStatus PullChunk(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_LOCAL_IN_BOUND;}

  k_bool HasOutput() override;

 private:
  DataChunkPtr PullPassthroughChunk();

 protected:
  std::deque<DataChunkPtr> chunks_;  // queue
  k_uint32 queue_data_size_{0};
  bool is_finished_{0};
  std::mutex chunk_lock_;
  std::condition_variable wait_cond_;
  EEPgErrorInfo pg_info_;
  EEIteratorErrCode local_inbound_code_{EEIteratorErrCode::EE_OK};
};

}  // namespace kwdbts
