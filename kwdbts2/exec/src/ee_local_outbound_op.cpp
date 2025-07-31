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

#include "ee_local_outbound_op.h"
#include "ee_local_inbound_op.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

LocalOutboundOperator::LocalOutboundOperator(const LocalOutboundOperator& other, int32_t processor_id)
: OutboundOperator(other, processor_id) { }

EEIteratorErrCode LocalOutboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();

  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  ret = OutboundOperator::Init(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    LOG_ERROR("LocalOutboundOperator::Init failed\n");
    Return(ret);
  }
  const TSStreamEndpointSpec& streams = spec_->streams(0);
  stream_id_ = streams.stream_id();
  Return(ret);
}

KStatus LocalOutboundOperator::PushChunk(DataChunkPtr& chunk, k_int32 stream_id, EEIteratorErrCode code) {
  for (auto parent : parent_operators_) {
    InboundOperator* parent_inbound =
        dynamic_cast<InboundOperator*>(parent);
    if (parent_inbound) {
      KStatus ret = parent_inbound->PushChunk(chunk, stream_id, code);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
  }

  return KStatus::SUCCESS;
}

void LocalOutboundOperator::PushFinish(EEIteratorErrCode code,
                                       k_int32 stream_id,
                                       const EEPgErrorInfo& pgInfo) {
  PrintFinishLog();
  for (auto parent : parent_operators_) {
    InboundOperator* parent_inbound = dynamic_cast<InboundOperator*>(parent);
    if (parent_inbound) {
      parent_inbound->PushFinish(code, stream_id_, pgInfo);
    }
  }
}

void LocalOutboundOperator::PrintFinishLog() {
}

EEIteratorErrCode LocalOutboundOperator::Next(kwdbContext_p ctx,
                                              DataChunkPtr& chunk) {
  EnterFunc();
  chunk = nullptr;

  if (chunk_tmp_) {
    KStatus ret = PushChunk(chunk_tmp_, stream_id_, EEIteratorErrCode::EE_OK);
    if (ret != KStatus::SUCCESS) {
      Return(EEIteratorErrCode::EE_QUEUE_FULL);
    }

    chunk_tmp_ = nullptr;
  }

  DataChunkPtr chunk_tmp;
  EEIteratorErrCode code = childrens_[0]->Next(ctx, chunk_tmp);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  if (EEPgErrorInfo::IsError()) {
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }

  chunk_tmp->ResetLine();
  total_rows_ += chunk_tmp->Count();
  ++total_push_count_;
  KStatus ret = PushChunk(chunk_tmp, stream_id_, code);
  if (ret != KStatus::SUCCESS) {
    chunk_tmp_ = std::move(chunk_tmp);
    Return(EEIteratorErrCode::EE_QUEUE_FULL);
  }

  if (parent_operators_.empty()) {
    KWThdContext *thd = current_thd;
    chunk = std::move(chunk_tmp);
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, thd, chunk);
  }

  Return(code);
}

BaseOperator* LocalOutboundOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator* iter = NewIterator<LocalOutboundOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }

  return iter;
}

}  // namespace kwdbts
