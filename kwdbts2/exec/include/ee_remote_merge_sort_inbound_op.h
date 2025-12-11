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
#include <memory>
#include "ee_remote_inbound_op.h"
#include "ee_operator_type.h"
#include "ee_sink_buffer.h"
#include "br_internal_service.pb.h"
namespace kwdbts {

class DataStreamRecvr;
class RemoteMergeSortInboundOperator : public RemoteInboundOperator {
 public:
  using RemoteInboundOperator::RemoteInboundOperator;
  virtual ~RemoteMergeSortInboundOperator() = default;
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  EEIteratorErrCode Close(kwdbContext_p ctx) override;
  // KStatus PullChunk(DataChunkPtr& chunk) override;
  k_bool HasOutput() override;
  k_bool IsFinished() override;
  k_bool NeedInput() override;
  enum OperatorType Type() override {
    return OperatorType::OPERATOR_REMOTE_MERGE_SORT_IN_BOUND;
  }
 private:
  EEIteratorErrCode TryPullChunk(kwdbContext_p ctx, DataChunkPtr& chunk);
  KStatus PullChunk(kwdbContext_p ctx, DataChunkPtr& chunk);
  KStatus GetNextMerging(kwdbContext_p ctx, DataChunkPtr& chunk);
 private:
  k_int64 num_rows_returned_ = 0;
  k_int64 num_rows_input_ = 0;
  k_int64 offset_ = 0;
  k_int64 limit_ = 0;
  std::atomic<k_bool> is_finished_{false};
};

}  // namespace kwdbts
