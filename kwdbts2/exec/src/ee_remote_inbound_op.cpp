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

#include "ee_remote_inbound_op.h"

#include "br_data_stream_recvr.h"
#include "br_mgr.h"
#include "br_pass_through_chunk_buffer.h"
#include "ee_cancel_checker.h"
#include "ee_dml_exec.h"
#include "ee_exec_pool.h"
#include "ee_pipeline_task.h"

#define EXCHG_NODE_BUFFER_SIZE_BYTES 10485760

namespace kwdbts {

RemoteInboundOperator::~RemoteInboundOperator() {
  if (stream_recvr_) {
    BrMgr::GetInstance().GetDataStreamMgr()->DestroyPassThroughChunkBuffer(query_id_);
    stream_recvr_->SetReceiveNotify(nullptr);
    stream_recvr_->Close();
  }
}

EEIteratorErrCode RemoteInboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = InboundOperator::Init(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }
  DmlExec::BrpcInfo brpc_info;
  if (ctx && ctx->dml_exec_handle) {
    ctx->dml_exec_handle->GetBrpcInfo(brpc_info);
    query_id_ = brpc_info.query_id_;
  } else {
    LOG_ERROR("RemoteInboundOperator ctx is nullptr");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  if (KStatus::SUCCESS !=
      BrMgr::GetInstance().GetDataStreamMgr()->PreparePassThroughChunkBuffer(
          query_id_)) {
    LOG_ERROR("PreparePassThroughChunkBuffer faild");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  stream_recvr_ = BrMgr::GetInstance().GetDataStreamMgr()->CreateRecvr(
      query_id_, dest_processor_id_, stream_size_, EXCHG_NODE_BUFFER_SIZE_BYTES,
      asc_order_.empty() ? false : true, asc_order_.empty() ? false : true);
  if (stream_recvr_ == nullptr) {
    LOG_ERROR("CreateRecvr faild");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  stream_recvr_->SetReceiveNotify(
      [this](k_int32 nodeid, k_int32 code, const std::string& msg) {
        this->ReceiveChunkNotify(nodeid, code, msg);
      });

  status_.set_status_code(0);
  status_.add_error_msgs("");

  k_uint32 sz = childrens_.size();
  for (k_uint32 i = 0; i < sz; i++) {
    code = childrens_[i]->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("childrens[%d] init failed.", i);
      Return(code);
    }
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode RemoteInboundOperator::Next(kwdbContext_p ctx,
                                              DataChunkPtr& chunk) {
  EEIteratorErrCode code = TryPullChunk(ctx, chunk);
  if (parser_output_fields_) {
    SetOutputFieldInfo(chunk);
    parser_output_fields_ = false;
  }

  return code;
}
EEIteratorErrCode RemoteInboundOperator::TryPullChunk(kwdbContext_p ctx,
                                                      DataChunkPtr& chunk) {
  {
    std::unique_lock l(lock_);
    while (true) {
      if (is_stop_ || (CheckCancel(ctx) != SUCCESS)) {
        LOG_ERROR("RemoteInboundOperator be cancel or stop (queryid %ld).",
                  query_id_);
        return EEIteratorErrCode::EE_ERROR;
      }
      if (status_.status_code() > 0) {
        EEPgErrorInfo::SetPgErrorInfo(status_.status_code(),
                                      status_.error_msgs(0).c_str());
        LOG_ERROR(
            "RemoteInboundOperator PullChunk error, status_code:%d, msg:%s, "
            "query_id:%ld",
            status_.status_code(), status_.error_msgs(0).c_str(), query_id_);
        return EEIteratorErrCode::EE_ERROR;
      }
      // must wait brpc message callback finish, cannot stop.
      if (stream_recvr_->HasOutputForPipeline(0)) {
        break;
      }
      if (IsFinished()) {
        return EEIteratorErrCode::EE_END_OF_RECORD;
      }

      not_fill_cv_.wait_for(l, std::chrono::seconds(2));
      continue;
    }
    if (new_chunks_num_ > 0) {
      new_chunks_num_--;
    }
  }
  if (KStatus::SUCCESS != PullChunk(ctx, chunk)) {
    LOG_ERROR("PullChunk  failed, queryid %ld", query_id_);
    return EEIteratorErrCode::EE_ERROR;
  }
  total_rows_ += chunk->Count();
  return EEIteratorErrCode::EE_OK;
}

KStatus RemoteInboundOperator::PullChunk(kwdbContext_p ctx, DataChunkPtr& chunk) {
  if (KStatus::SUCCESS != stream_recvr_->GetChunkForPipeline(&chunk, 0)) {
    LOG_ERROR("eng to Get DataChunk, faild");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}
void RemoteInboundOperator::ReceiveChunkNotify(k_int32 nodeid, k_int32 code,
                                               const std::string& msg) {
  {
    std::unique_lock l(lock_);
    if (code > 0) {
      status_.set_status_code(code);
      status_.set_error_msgs(0, msg);
    } else if (0 == code) {
      new_chunks_num_++;
    }
  }
  // PushTaskToQueue();
  not_fill_cv_.notify_one();
}

std::vector<Field*>& RemoteInboundOperator::OutputFields() {
  if (childrens_.size() > 0) {
    return childrens_[0]->OutputFields();
  } else {
    return output_fields_;
  }
}

void RemoteInboundOperator::SetOutputFieldInfo(const DataChunkPtr& chunk) {
  if (nullptr == chunk) {
    return;
  }

  ColumnInfo* columnInfo = chunk->GetColumnInfo();
  k_uint32 col_num = chunk->ColumnNum();

  if (col_num != output_fields_.size()) {
    LOG_ERROR("col_num %u != output_fields_.size() %lu", col_num,
              output_fields_.size());
    return;
  }

  for (k_uint32 i = 0; i < col_num; i++) {
    ColumnInfo col_info = columnInfo[i];
    output_fields_[i]->table_ = table_;
    output_fields_[i]->set_storage_type(col_info.storage_type);
    output_fields_[i]->set_storage_length(col_info.storage_len);
  }
}

KStatus RemoteInboundOperator::SendError(const EEPgErrorInfo& pgInfo) {
  PTransmitChunkParamsPtr chunk_request =
      std::make_shared<PTransmitChunkParams>();
  StatusPB* status = new StatusPB();
  status->set_status_code(pgInfo.code);
  status->add_error_msgs(pgInfo.msg);
  chunk_request->set_allocated_status(&status_);
  chunk_request->set_dest_processor(dest_processor_id_);
  chunk_request->set_sender_id(stream_id_);
  chunk_request->set_be_number(stream_id_);
  chunk_request->set_eos(true);
  chunk_request->set_use_pass_through(true);
  chunk_request->set_query_id(query_id_);
  current_request_bytes_ = 0;
  BrMgr::GetInstance().GetDataStreamMgr()->TransmitChunk(*chunk_request_,
                                                         nullptr);
  chunk_request_.reset();
  return KStatus::SUCCESS;
}

KStatus RemoteInboundOperator::SetFinish() { return KStatus::SUCCESS; }

void RemoteInboundOperator::PushFinish(EEIteratorErrCode code,
                                       k_int32 stream_id,
                                       const EEPgErrorInfo& pgInfo) {
  std::unique_lock l(push_lock_);
  if (pgInfo.code > 0) {
    ReceiveChunkNotify(0, pgInfo.code, pgInfo.msg);
    return;
  }

  // k_bool eos = ((++finish_local_num_) == childrens_.size());
  if (chunk_request_ == nullptr) {
    chunk_request_ = std::make_shared<PTransmitChunkParams>();
    chunk_request_->set_dest_processor(dest_processor_id_);
    chunk_request_->set_sender_id(stream_id);
    chunk_request_->set_be_number(stream_id);
    chunk_request_->set_query_id(query_id_);
  }
  chunk_request_->set_eos(true);
  chunk_request_->set_use_pass_through(true);
  chunk_request_->set_sequence(++local_request_seq);
  current_request_bytes_ = 0;

  BrMgr::GetInstance().GetDataStreamMgr()->TransmitChunk(*chunk_request_,
                                                         nullptr);

  // if (send_count_ > 0) {
  //   LOG_ERROR(
  //       "[RemoteInboundOperator send_data success "
  //       "to:processor_id:%d,stream_id_:%d,query_id:%ld] [send_data_count:%d]
  //       "
  //       "--------------------------",
  //       dest_processor_id_, stream_id, query_id_, send_count_);
  // }

  // LOG_ERROR(
  //     "[RemoteInboundOperator send eos success "
  //     "to:processor_id:%d,stream_id_:%d,query_id:%ld] "
  //     "[send_data_count:%d]--------------------------",
  //     dest_processor_id_, stream_id, query_id_, send_count_);
  send_count_ = 0;

  chunk_request_.reset();
}

KStatus RemoteInboundOperator::PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                                         EEIteratorErrCode code) {
  std::unique_lock l(push_lock_);
  if (chunk_request_ == nullptr) {
    chunk_request_ = std::make_shared<PTransmitChunkParams>();
    chunk_request_->set_dest_processor(dest_processor_id_);
    chunk_request_->set_sender_id(stream_id);
    chunk_request_->set_be_number(stream_id);
    chunk_request_->set_query_id(query_id_);
  }
  DataChunk* tmp = chunk.get();
  if (nullptr != tmp && stream_recvr_ && tmp->Count() > 0) {
    size_t chunk_size = tmp->Size() + 20;
    stream_recvr_->GetPassThroughContext().AppendChunk(stream_id, chunk, chunk_size, 0);
    current_request_bytes_ += chunk_size;
    send_count_ += tmp->Count();
  }

  // if (current_request_bytes_ > 262144) {
  chunk_request_->set_eos(false);
  chunk_request_->set_use_pass_through(true);
  chunk_request_->set_sequence(++local_request_seq);
  current_request_bytes_ = 0;

  BrMgr::GetInstance().GetDataStreamMgr()->TransmitChunk(*chunk_request_,
                                                         nullptr);
  chunk_request_.reset();
  return KStatus::SUCCESS;
}

EEIteratorErrCode RemoteInboundOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  is_stop_ = true;
  {
    std::unique_lock l(lock_);
    not_fill_cv_.notify_all();
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  code = InboundOperator::Close(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }

  Return(EEIteratorErrCode::EE_OK);
}

k_bool RemoteInboundOperator::HasOutput() {
  if (stream_recvr_->HasOutputForPipeline(0)) {
    return true;
  }

  return IsFinished();
}

k_bool RemoteInboundOperator::IsFinished() {
  return stream_recvr_ && stream_recvr_->IsFinished();
}

KStatus RemoteInboundOperator::SetFinishing() {
  stream_recvr_->ShortCircuitForPipeline(0);
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
