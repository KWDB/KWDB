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
#include <vector>
#include <memory>
#include <string>
#include "ee_sink_buffer.h"
#include "ee_inbound_op.h"
#include "ee_operator_type.h"
#include "br_internal_service.pb.h"
namespace kwdbts {

class DataStreamRecvr;
class RemoteInboundOperator : public InboundOperator {
 public:
  using InboundOperator::InboundOperator;
  virtual ~RemoteInboundOperator() = default;
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  EEIteratorErrCode Close(kwdbContext_p ctx) override;
  KStatus PullChunk(DataChunkPtr& chunk) override;
  KStatus PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                    EEIteratorErrCode code = EEIteratorErrCode::EE_OK) override;
  void PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                  const EEPgErrorInfo& pgInfo) override;

  k_bool HasOutput() override;
  k_bool IsFinished() override;
  KStatus SetFinishing() override;
  enum OperatorType Type() override {
    return OperatorType::OPERATOR_REMOTE_IN_BOUND;
  }
  void ReceiveChunkNotify(k_int32 nodeid = 0, k_int32 code = 0,
                          const std::string& msg = "");

  std::vector<Field*>& OutputFields() override;

  void SetOutputFieldInfo(const DataChunkPtr& chunk);

 private:
  EEIteratorErrCode TryPullChunk(kwdbContext_p ctx, DataChunkPtr& chunk);
  KStatus SetFinish();
  KStatus SendError(const EEPgErrorInfo& pgInfo);

 protected:
  std::shared_ptr<DataStreamRecvr> stream_recvr_ = nullptr;
  std::condition_variable not_fill_cv_;
  StatusPB status_;
  k_int32 send_count_{0};
  mutable std::mutex lock_;
  k_bool is_stop_{false};
  k_int64 local_request_seq{-1};

 private:
  size_t current_request_bytes_{0};
  PTransmitChunkParamsPtr chunk_request_ = nullptr;
  std::mutex  push_lock_;
  k_int32 new_chunks_num_{0};
  // std::atomic<k_int32> finish_local_num_ = 0;  // for localoutbound finish
};

}  // namespace kwdbts
