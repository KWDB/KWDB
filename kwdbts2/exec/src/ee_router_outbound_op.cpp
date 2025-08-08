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

#include "ee_router_outbound_op.h"

#include <lz4.h>

#include <iomanip>
#include <random>

#include "br_internal_service_recoverable_stub.h"
#include "ee_cancel_checker.h"
#include "ee_combined_group_key.h"
#include "ee_defer.h"
#include "ee_dml_exec.h"
#include "ee_global.h"
#include "ee_inbound_op.h"
#include "ee_new_slice.h"
#include "ee_pb_plan.pb.h"
#include "ee_protobufC_serde.h"
#include "lg_api.h"
#define CAPACITY_SIZE 65536
namespace kwdbts {

KStatus RouterOutboundOperator::Channel::Init() {
  if (is_inited_) {
    return KStatus::SUCCESS;
  }
  if (rpc_dest_addr_.hostname_.empty()) {
    LOG_ERROR("rpc destination address's hostname is empty");
    return KStatus::FAIL;
  }
  if (query_id_ == -1) {
    is_inited_ = true;
    return KStatus::SUCCESS;
  }

  brpc_stub_ = BrMgr::GetInstance().GetBrpcStubCache()->GetStub(rpc_dest_addr_);
  if (brpc_stub_ == nullptr) {
    LOG_ERROR("The rpc stub cache is null");
    return KStatus::FAIL;
  }
  is_inited_ = true;
  CheckPassThrough();
  status_.set_status_code(0);
  status_.add_error_msgs("");
  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::Channel::CloseInternal() {
  if (this->query_id_ == -1) {
    return KStatus::FAIL;
  }

  if (chunks_.size() > 0 && chunks_[0] != nullptr) {
    if (KStatus::SUCCESS != SendOneChunk(chunks_[0], 0, false)) {
      LOG_ERROR("close SendOneChunk faild");
      return KStatus::FAIL;
    }
  }
  DataChunkPtr chunk = nullptr;
  if (KStatus::SUCCESS != SendOneChunk(chunk, 0, true)) {
    LOG_ERROR("close SendOneChunk faild");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::Channel::Close() { return CloseInternal(); }
KStatus RouterOutboundOperator::Channel::SendError(
    k_int32 error_code, const std::string& error_msg) {
  PSendExecStatusPtr request = std::make_shared<PSendExecStatus>();
  request->set_query_id(query_id_);
  request->set_error_code(error_code);
  request->set_exec_status(::kwdbts::ExecStatus::EXEC_STATUS_FAILED);
  request->set_dest_processor(dest_processor_id_);
  request->set_error_msg(error_msg);
  TransmitSimpleInfo info = {
      this->target_node_id_, 1, error_msg, brpc_stub_, NULL, std::move(request),
      rpc_dest_addr_};
  if (KStatus::SUCCESS != parent_->buffer_->SendErrMsg(info)) {
    LOG_ERROR("send_simple_msg failed");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::Channel::SendOneChunk(DataChunkPtr& chunk,
                                                        k_int32 driver_sequence,
                                                        k_bool eos) {
  k_bool is_real_sent = false;
  // if (0 != status_.status_code()) {
  //   SendError();
  //   return KStatus::SUCCESS;
  // }
  return SendOneChunk(chunk, driver_sequence, eos, &is_real_sent);
}

KStatus RouterOutboundOperator::Channel::SendOneChunk(DataChunkPtr& data_chunk,
                                                        k_int32 driver_sequence,
                                                        k_bool eos,
                                                        k_bool* is_real_sent) {
  DataChunk* chunk = data_chunk.get();
  *is_real_sent = false;
  if (chunk_request_ == nullptr) {
    chunk_request_ = std::make_shared<PTransmitChunkParams>();
    chunk_request_->set_dest_processor(dest_processor_id_);
    chunk_request_->set_sender_id(stream_id_);
    chunk_request_->set_be_number(stream_id_);
  }

  if (nullptr != chunk && chunk->Count() > 0) {
    send_count_ += chunk->Count();
    if (use_pass_through_) {
      size_t chunk_size = chunk->Size() + 20;
      pass_through_context_.AppendChunk(stream_id_, data_chunk, chunk_size,
                                        DEFAULT_DRIVER_SEQUENCE);
      current_request_bytes_ += chunk_size;
    } else {
      auto pchunk = chunk_request_->add_chunks();
      if (KStatus::SUCCESS !=
          parent_->SerializeChunk(chunk, pchunk, &is_first_chunk_)) {
        LOG_ERROR("SerializeChunk faild");
        return KStatus::FAIL;
      }
      current_request_bytes_ += pchunk->data().size();
    }
  }

  if (current_request_bytes_ > 262144 || eos) {
    chunk_request_->set_eos(eos);
    chunk_request_->set_use_pass_through(use_pass_through_);
    butil::IOBuf attachment;
    int64_t attachment_physical_bytes =
        parent_->ConstrucBrpcAttachment(chunk_request_, attachment);
    TransmitChunkInfo info = {this->target_node_id_,     brpc_stub_,
                              std::move(chunk_request_), attachment,
                              attachment_physical_bytes, rpc_dest_addr_};
    if (KStatus::SUCCESS != parent_->buffer_->AddRequest(info)) {
      LOG_ERROR("AddRequest faild");
      return KStatus::FAIL;
    }
    if (eos) {
      send_count_ = 0;
    }
    current_request_bytes_ = 0;
    chunk_request_.reset();
    *is_real_sent = true;
  }
  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::Channel::SendChunkRequest(
    PTransmitChunkParamsPtr chunk_request, const butil::IOBuf& attachment,
    k_int64 attachment_physical_bytes) {
  // if (0 != status_.status_code()) {
  //   SendError();
  //   return KStatus::SUCCESS;
  // }
  chunk_request->set_dest_processor(dest_processor_id_);
  chunk_request->set_sender_id(stream_id_);
  chunk_request->set_be_number(stream_id_);
  chunk_request->set_eos(false);
  chunk_request->set_use_pass_through(use_pass_through_);
  TransmitChunkInfo info = {this->target_node_id_,     brpc_stub_,
                            std::move(chunk_request),  attachment,
                            attachment_physical_bytes, rpc_dest_addr_};
  if (KStatus::SUCCESS != parent_->buffer_->AddRequest(info)) {
    LOG_ERROR("AddRequest faild");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::Channel::CheckRecvrReady() {
  PDialDataRecvrPtr request = std::make_shared<PDialDataRecvr>();
  request->set_query_id(query_id_);
  request->set_dest_processor(dest_processor_id_);
  TransmitSimpleInfo info = {
      this->target_node_id_, 0, "", brpc_stub_, std::move(request), NULL,
      rpc_dest_addr_};
  if (KStatus::SUCCESS != parent_->buffer_->SendSimpleMsg(info)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

k_bool RouterOutboundOperator::Channel::IsLocal() {
  if (type_ == StreamEndpointType::REMOTE) {
    return false;
  } else {
    return true;
  }
}

k_bool RouterOutboundOperator::Channel::CheckPassThrough() {
  pass_through_context_.Init();
  use_pass_through_ = IsLocal();  // later..
  return use_pass_through_;
}
KStatus RouterOutboundOperator::Channel::AddRowsSelective(
    DataChunk* chunk, k_int32 driver_sequence, const k_uint32* indexes,
    k_uint32 size) {
  if (driver_sequence >= static_cast<k_int32>(chunks_.size())) {
    chunks_.resize(driver_sequence + 1);
  }

  if (chunks_[driver_sequence] == nullptr) {
    chunks_[driver_sequence] = std::make_unique<DataChunk>(chunk->GetColumnInfo(), chunk->ColumnNum(), CAPACITY_SIZE);
    if (chunks_[driver_sequence] == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      return KStatus::FAIL;
    }
    if (!chunks_[driver_sequence]->Initialize()) {
      chunks_[driver_sequence] = nullptr;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      return KStatus::FAIL;
    }
  }

  if (chunks_[driver_sequence]->Count() + size > CAPACITY_SIZE) {
    if (KStatus::SUCCESS != SendOneChunk(chunks_[driver_sequence], driver_sequence, false)) {
      LOG_ERROR("SendOneChunk faild");
      return KStatus::FAIL;
    }
    // we only clear column data, because we need to reuse column schema
    if (nullptr != chunks_[driver_sequence]) {
      chunks_[driver_sequence]->Reset();
    } else {
      chunks_[driver_sequence] = std::make_unique<DataChunk>(chunk->GetColumnInfo(), chunk->ColumnNum(), CAPACITY_SIZE);
      if (chunks_[driver_sequence] == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        return KStatus::FAIL;
      }
      if (!chunks_[driver_sequence]->Initialize()) {
        chunks_[driver_sequence] = nullptr;
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        return KStatus::FAIL;
      }
    }
  }

  {
    chunks_[driver_sequence]->Append_Selective(chunk, indexes, size);
  }
  return KStatus::SUCCESS;
}

RouterOutboundOperator::RouterOutboundOperator(TsFetcherCollection* collection,
                                               TSOutputRouterSpec* spec,
                                               TABLE* table)
    : OutboundOperator(collection, spec, table), buffer_(nullptr) {}

RouterOutboundOperator::~RouterOutboundOperator() {}

EEIteratorErrCode RouterOutboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  ret = OutboundOperator::Init(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    LOG_ERROR("RouterOutboundOperator::Init failed\n");
    Return(ret);
  }
  DmlExec::BrpcInfo brpc_info;
  if (ctx && ctx->dml_exec_handle) {
    ctx->dml_exec_handle->GetBrpcInfo(brpc_info);
    query_id_ = brpc_info.query_id_;
  } else {
    LOG_ERROR("ctx is nullptr");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (KStatus::SUCCESS !=
      BrMgr::GetInstance().GetDataStreamMgr()->PreparePassThroughChunkBuffer(
          query_id_)) {
    LOG_ERROR("PreparePassThroughChunkBuffer faild");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  PassThroughChunkBuffer* pass_through_chunk_buffer =
      BrMgr::GetInstance().GetDataStreamMgr()->GetPassThroughChunkBuffer(
          query_id_);

  channels_.reserve(stream_size_);
  std::vector<FragmentDestination> destinations;
  for (k_int32 i = 0; i < stream_size_; i++) {
    TSStreamEndpointSpec spec = spec_->streams(i);
    k_int32 target_id = spec.target_node_id();
    k_int32 dest_processor = spec.dest_processor();
    k_int32 stream_id = spec.stream_id();
    auto it = target_id2channel_.find(target_id);
    if (it != target_id2channel_.end()) {
      channels_.emplace_back(it->second.get());
    } else {
      TNetworkAddress addr;
      if (0 == target_id) {
        addr = BrMgr::GetInstance().GetAddr();
      } else {
        addr.SetHostname(brpc_info.addrs_[target_id - 1].ip_);
        addr.SetPort(brpc_info.addrs_[target_id - 1].port_);
      }
      std::unique_ptr<Channel> channel = std::make_unique<Channel>(
          this, addr, query_id_, dest_processor, target_id, stream_id,
          pass_through_chunk_buffer);
      channel->SetType(spec.type());
      channels_.emplace_back(channel.get());
      target_id2channel_.emplace(target_id, std::move(channel));
      StatusPB status;
      status.set_status_code(0);
      channel_status_.emplace(target_id, status);
      FragmentDestination dest;
      dest.target_node_id = target_id;
      dest.brpc_addr = addr;
      dest.query_id = query_id_;
      destinations.emplace_back(dest);
    }
  }

  channel_indices_.resize(channels_.size());
  std::iota(channel_indices_.begin(), channel_indices_.end(), 0);
  std::shuffle(channel_indices_.begin(), channel_indices_.end(),
               std::mt19937(std::random_device()()));
  for (auto& [_, channel] : target_id2channel_) {
    if (KStatus::SUCCESS != channel->Init()) {
      LOG_ERROR("channel init faild");
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }
  buffer_ = make_shared<SinkBuffer>(destinations, 0);
  if (buffer_) {
    buffer_->SetReceiveNotify(
        [this](k_int32 nodeid, k_int32 code, const std::string& msg) { this->ReceiveNotify(nodeid, code, msg); });
    buffer_->SetReceiveNotifyEx([this]() { this->ReceiveNotifyEx(); });
  }
  status_.set_status_code(0);
  status_.add_error_msgs("");
  // if (!is_ready_) {
  //   is_ready_ = CheckReady(ctx);
  //   if(!is_ready_) {
  //     Return(EEIteratorErrCode::EE_ERROR);
  //   }
  // }
  // compress type ,default LZ4
  compress_type_ = CompressionTypePB::LZ4_COMPRESSION;
  GetBlockCompressionCodec(compress_type_, &compress_codec_);
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode RouterOutboundOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = OutboundOperator::Start(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    LOG_ERROR("RouterOutboundOperator::Start failed\n");
    k_int32 err_code = ret;
    std::string msg = "RouterOutboundOperator Start failed";
    auto& error_info = EEPgErrorInfo::GetPgErrorInfo();
    if (error_info.code > 0) {
      err_code = error_info.code;
      msg = std::string(error_info.msg);
    } else if (err_code == EEIteratorErrCode::EE_ERROR) {
      err_code = EEIteratorErrCode::EE_DATA_ERROR;
    }
    SendErrorMessage(ctx, err_code, msg);
    Return(ret);
  }
  buffer_->IncrSinker();
  Return(EEIteratorErrCode::EE_OK);
}

k_bool RouterOutboundOperator::SendErrorMessage(kwdbContext_p ctx,
                                                k_int32 error_code,
                                                const std::string& error_msg) {
  for (auto& [_, channel] : target_id2channel_) {
    if ((part_type_ ==
         TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER) &&
        channel->UsePassThrougth()) {
      continue;
    }

    channel->SendError(error_code, error_msg);
  }

  // wait
  std::unique_lock l(lock_);
  while (true) {
    // wait
    if (buffer_->IsFinishedExtra()) {
      break;
    }
    wait_cond_.wait_for(l, std::chrono::seconds(2));
    continue;
  }
  return true;
}

k_bool RouterOutboundOperator::CheckReady(kwdbContext_p ctx) {
  EnterFunc();
  k_bool ready = true;
  k_bool first = true;
  std::vector<k_int32> tmp_status;
  k_int64 start = MonotonicNanos();
  while (true) {
    if (CheckCancel(ctx) != SUCCESS) {
      LOG_ERROR("RouterOutboundOperator be cancel (queryid:%ld) .", query_id_);
      Return(false);
    }
    if (is_tp_stop_) {
      ready = false;
      Return(ready);
    }
    for (auto& [targetid, channel] : target_id2channel_) {
      if ((part_type_ ==
           TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER) &&
          channel->UsePassThrougth()) {
        continue;
      }

      if (!first && (std::find(tmp_status.begin(), tmp_status.end(),
                               targetid) == tmp_status.end())) {
        continue;
      }
      channel->CheckRecvrReady();
    }

    {
      std::unique_lock l(lock_);
      while (true) {
        // wait
        if (buffer_->IsFinishedExtra()) {
          break;
        }
        wait_cond_.wait_for(l, std::chrono::seconds(2));
        continue;
      }
      tmp_status.clear();
      for (auto& status : channel_status_) {
        if (status.second.status_code() != 0) {
          tmp_status.push_back(status.first);
          ready = false;
        }
      }
      if (ready) {
        status_.set_status_code(0);
        status_.add_error_msgs("");
        break;
      }
    }

    first = false;
    ready = true;
    k_int64 time_spent = MonotonicNanos() - start;
    if (time_spent >= 10 * 1000000000L) {
      LOG_ERROR("Data Receiver (queryid:%ld) connect timeout, quit.",
                query_id_);
      ready = false;
      break;
    }
    usleep(1);
  }
  Return(ready);
}

EEIteratorErrCode RouterOutboundOperator::Next(kwdbContext_p ctx,
                                               DataChunkPtr& chunk) {
  EnterFunc();
  if (CheckCancel(ctx) != SUCCESS) {
    LOG_ERROR("RouterOutboundOperator be cancel (queryid:%ld) .", query_id_);
    Return(EEIteratorErrCode::EE_ERROR);
  }

  DataChunkPtr temp_chunk = nullptr;
  EEIteratorErrCode code = childrens_[0]->Next(ctx, temp_chunk);
  if (EEIteratorErrCode::EE_OK != code) {
    if (EEIteratorErrCode::EE_TIMESLICE_OUT != code &&
        EEIteratorErrCode::EE_END_OF_RECORD != code) {
      LOG_ERROR("RouterOutboundOperator::Next failed, code:%d", code);
      auto& error_info = EEPgErrorInfo::GetPgErrorInfo();
      k_int32 err_code = code;
      std::string msg = "RouterOutboundOperator failed";
      if (error_info.code > 0) {
        err_code = error_info.code;
        msg = std::string(error_info.msg);
      } else if (err_code == EEIteratorErrCode::EE_ERROR) {
        err_code = EEIteratorErrCode::EE_DATA_ERROR;
      }
      SendErrorMessage(ctx, err_code, msg);
      Return(EEIteratorErrCode::EE_ERROR);
    }
    EEPgErrorInfo info;
    PushFinish(code, 0, info);
  } else {
    PushChunk(temp_chunk, 0);
    code = EEIteratorErrCode::EE_NEXT_CONTINUE;
  }

  {
    std::unique_lock l(lock_);
    while (true) {
      // wait
      if (buffer_->IsFinishedExtra()) {
        break;
      }
      wait_cond_.wait_for(l, std::chrono::seconds(2));
      continue;
    }

    if (status_.status_code() > 0) {
      EEPgErrorInfo::SetPgErrorInfo(status_.status_code(),
                                    status_.error_msgs(0).c_str());
      LOG_ERROR("RouterOutboundOperator send failed, status_code:%d, msg:%s",
                status_.status_code(), status_.error_msgs(0).c_str());
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }
  Return(code);
}

void RouterOutboundOperator::PushFinish(EEIteratorErrCode code,
                                        k_int32 stream_id,
                                        const EEPgErrorInfo& pgInfo) {
  // pg_info_ = pgInfo;
  SetFinishing();
}

int64_t RouterOutboundOperator::ConstrucBrpcAttachment(
    const PTransmitChunkParamsPtr& chunk_request, butil::IOBuf& attachment) {
  for (int i = 0; i < chunk_request->chunks().size(); ++i) {
    auto chunk = chunk_request->mutable_chunks(i);
    chunk->set_data_size(chunk->data().size());
    attachment.append(chunk->data());
    chunk->clear_data();
  }

  return 1024;
}

static void ConstructGroupKeys(IChunk* chunk, std::vector<k_uint32>& all_cols,
                               k_uint32 line, CombinedGroupKey& field_keys) {
  for (k_int32 i = 0; i < all_cols.size(); i++) {
    auto idx = all_cols[i];
    bool is_null = chunk->IsNull(line, idx);
    if (is_null) {
      field_keys.AddGroupKey(nullptr, i);
      continue;
    }
    DatumPtr ptr = chunk->GetData(line, idx);
    field_keys.AddGroupKey(ptr, i);
  }
}

KStatus RouterOutboundOperator::SetFinishing() {
  // _is_finished = true;
  std::unique_lock l(lock_);
  KStatus status = KStatus::SUCCESS;
  if (is_real_finished_) {
    return status;
  }
  if (chunk_request_ != nullptr) {
    butil::IOBuf attachment;
    int64_t attachment_physical_bytes =
        ConstrucBrpcAttachment(chunk_request_, attachment);
    for (const auto& [_, channel] : target_id2channel_) {
      PTransmitChunkParamsPtr copy =
          std::make_shared<PTransmitChunkParams>(*chunk_request_);
      channel->SetStatus(pg_info_.code, pg_info_.msg);
      if (KStatus::SUCCESS !=
          channel->SendChunkRequest(copy, attachment,
                                    attachment_physical_bytes)) {
        LOG_ERROR("finish SendChunkRequest fail");
        return KStatus::FAIL;
      }
    }
    current_request_bytes_ = 0;
    chunk_request_.reset();
  }

  for (auto& [_, channel] : target_id2channel_) {
    if ((part_type_ ==
         TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER) &&
        channel->UsePassThrougth()) {
      continue;
    }
    channel->SetStatus(pg_info_.code, pg_info_.msg);
    auto tmp_status = channel->Close();
    if (tmp_status != KStatus::SUCCESS) {
      status = tmp_status;
    }
  }
  is_real_finished_ = true;
  return status;
}

BaseOperator* RouterOutboundOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator* iter =
      NewIterator<RouterOutboundOperator>(collection_, spec_, table_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }

  return iter;
}

EEIteratorErrCode RouterOutboundOperator::Close(kwdbContext_p ctx) {
  is_tp_stop_ = true;
  {
    std::unique_lock l(lock_);
    while (true) {
      // must wait brpc message callback finish, cannot stop.
      if (buffer_->IsFinishedExtra()) {
        break;
      }
      wait_cond_.wait_for(l, std::chrono::seconds(2));
      continue;
    }
    is_real_finished_ = true;
  }

  buffer_.reset();
  size_t size = childrens_.size();
  for (size_t i = 0; i < size; i++) {
    if (childrens_[i]) {
      if (EEIteratorErrCode::EE_OK != childrens_[i]->Close(ctx)) {
        LOG_ERROR("Failed to close child operator");
        return EEIteratorErrCode::EE_ERROR;
      }
    }
  }
  return EEIteratorErrCode::EE_OK;
}

KStatus RouterOutboundOperator::PushChunk(DataChunkPtr& chunk,
                                          k_int32 stream_id,
                                          EEIteratorErrCode code) {
  uint16_t num_rows = chunk->Count();
  total_rows_ += num_rows;
  if (num_rows == 0) {
    return KStatus::SUCCESS;
  }

  DataChunk* send_chunk = chunk.get();
  if (part_type_ == TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_MIRROR) {
    if (chunk_request_ == nullptr) {
      chunk_request_ = std::make_shared<PTransmitChunkParams>();
    }

    // If we have any channel which can pass through chunks, we use
    // `SendOneChunk`(without serialization)
    k_bool has_not_pass_through = false;
    k_int32 pass_through_idx = -1;
    for (auto idx : channel_indices_) {
      if (channels_[idx]->UsePassThrougth()) {
        pass_through_idx = idx;
      } else {
        has_not_pass_through = true;
      }
    }

    if (has_not_pass_through) {
      ChunkPB* pchunk = chunk_request_->add_chunks();
      if (KStatus::SUCCESS !=
          SerializeChunk(send_chunk, pchunk, &is_first_chunk_)) {
        LOG_ERROR("OutboundOperator send chunk fail");
        return KStatus::FAIL;
      }
      current_request_bytes_ += pchunk->data().size();
      // 3. if request bytes exceede the threshold, send current request
      if (current_request_bytes_ > max_transmit_batched_bytes_) {
        butil::IOBuf attachment;
        int64_t attachment_physical_bytes =
            ConstrucBrpcAttachment(chunk_request_, attachment);
        for (auto idx : channel_indices_) {
          if (!channels_[idx]->UsePassThrougth()) {
            PTransmitChunkParamsPtr copy =
                std::make_shared<PTransmitChunkParams>(*chunk_request_);
            if (KStatus::SUCCESS !=
                channels_[idx]->SendChunkRequest(copy, attachment,
                                                 attachment_physical_bytes)) {
              LOG_ERROR("OutboundOperator SendChunkRequest fail");
              return KStatus::FAIL;
            }
          }
        }
        current_request_bytes_ = 0;
        chunk_request_.reset();
      }
    }

    if (-1 != pass_through_idx) {
      if (KStatus::SUCCESS != channels_[pass_through_idx]->SendOneChunk(
                                  chunk, DEFAULT_DRIVER_SEQUENCE, false)) {
        LOG_ERROR("OutboundOperator send chunk fail");
        return KStatus::FAIL;
      }
    }
  } else if (part_type_ ==
             TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH) {
    auto& channel = channels_[curr_channel_idx_];
    k_bool real_sent = false;
    if (KStatus::SUCCESS != channel->SendOneChunk(chunk,
                                                    DEFAULT_DRIVER_SEQUENCE,
                                                    false, &real_sent)) {
      LOG_ERROR("OutboundOperator send chunk fail");
      return KStatus::FAIL;
    }
  } else if (part_type_ ==
             TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH) {
    std::map<k_int32, std::vector<k_uint32>> channel2rownum;
    {
      // Compute hash for each partition column
      CombinedGroupKey group_key;
      if (!group_key.is_init_) {
        bool ret = group_key.Init(chunk->GetColumnInfo(), group_cols_);
        if (!ret) {
          return KStatus::FAIL;
        }
      }

      k_int32 channel_size = target_id2channel_.size();
      for (int row = 0; row < num_rows; row++) {
        ConstructGroupKeys(send_chunk, group_cols_, row, group_key);
        GroupKeyHasher keyhash;
        std::size_t hash = keyhash(group_key);
        channel2rownum[hash % channel_size].push_back(row);
      }
    }
    for (int32_t channel_id : channel_indices_) {
      if (-1 == channels_[channel_id]->GetQueryId()) {
        // dest bucket is no used, continue
        continue;
      }

      int driver_sequence = 0;
      channels_[channel_id]->AddRowsSelective(
          send_chunk, driver_sequence, channel2rownum[channel_id].data(),
          channel2rownum[channel_id].size());
    }
  } else if (part_type_ ==
             TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER) {
    for (auto idx : channel_indices_) {
      if (channels_[idx]->UsePassThrougth()) {
        continue;
      }

      auto& channel = channels_[idx];
      k_bool real_sent = false;
      if (KStatus::SUCCESS != channel->SendOneChunk(chunk,
                                                      DEFAULT_DRIVER_SEQUENCE,
                                                      false, &real_sent)) {
        LOG_ERROR("OutboundOperator send chunk fail");
        return KStatus::FAIL;
      }
    }
  } else if (part_type_ ==
             TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_RANGE) {
    LOG_ERROR("TSOutputRouterSpec_Type_BY_RANGE not implemented");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus RouterOutboundOperator::SerializeChunk(DataChunk* src, ChunkPB* dst,
                                               k_bool* is_first_chunk) {
  ProtobufChunkSerrialde serial;
  if (compress_type_ == CompressionTypePB::NO_COMPRESSION ||
      src->IsEncoding()) {
    *dst = serial.SerializeChunk(src, is_first_chunk);
    dst->set_compress_type(CompressionTypePB::NO_COMPRESSION);
  } else {
    // std::string serialized;
    size_t col_size = 0;
    k_int64 offset = 0;
    auto* column_info = src->GetColumnInfo();
    k_uint32 bitmap_size = src->BitmapSize();
    *dst = serial.SerializeColumn(src, is_first_chunk);
    k_int64 data_size = 0;
    if (compress_codec_ != nullptr) {
      k_uint32 col_num = src->ColumnNum();
      for (size_t i = 0; i < col_num; ++i) {
        // ColumnPB* column_pb = dst->mutable_columns(i);
        // KSlice input(column_pb->data());
        // size_t serial_size = column_pb->data().size();
        ColumnPB* column_pb = dst->add_columns();
        col_size = column_info[i].fixed_storage_len * src->Capacity() + bitmap_size;
        KSlice input(src->GetData() + offset, col_size);
        offset += col_size;
        if (UseCompressionPool(compress_codec_->Type())) {
          KSlice compressed_slice;
          if (KStatus::FAIL ==
              compress_codec_->Compress(input, &compressed_slice, true, col_size, nullptr, &compression_scratch_)) {
            LOG_ERROR("compress fail");
            return KStatus::FAIL;
          }
        } else {
          k_int32 max_compressed_size = compress_codec_->MaxCompressedLen(col_size);
          if (compression_scratch_.size() < max_compressed_size) {
            compression_scratch_.resize(max_compressed_size);
          }
          KSlice compressed_slice{compression_scratch_.data(), compression_scratch_.size()};
          KSlice input(column_pb->data());
          if (KStatus::FAIL == compress_codec_->Compress(input, &compressed_slice)) {
            LOG_ERROR("compress fail");
            return KStatus::FAIL;
          }
          compression_scratch_.resize(compressed_slice.size);
        }
        // column_pb->clear_data();
        column_pb->set_compressed_size(compression_scratch_.size());
        column_pb->set_uncompressed_size(col_size);
        data_size += compression_scratch_.size();
        column_pb->mutable_data()->swap(compression_scratch_);
        compression_scratch_.clear();
      }

      k_int64 metadata_size = src->ColumnNum() * 3 * 8;
      std::string* serialized_data = dst->mutable_data();
      serialized_data->resize(20 + data_size + metadata_size);
      auto* buff = reinterpret_cast<uint8_t*>(serialized_data->data());
      encode_fixed32(buff + 0, 1);
      encode_fixed32(buff + 4, src->Count());
      encode_fixed32(buff + 8, src->Capacity());
      encode_fixed32(buff + 12, src->Size());
      buff = buff + 20;
      for (int i = 0; i < dst->columns_size(); ++i) {
        const ColumnPB& column = dst->columns(i);
        k_int64 column_uncompressed_size = column.uncompressed_size();
        encode_fixed32(buff + 0, column_uncompressed_size);
        k_int64 column_compressed_size = column.compressed_size();
        encode_fixed32(buff + 8, column_compressed_size);
        k_int64 column_offset = column.offset();
        encode_fixed32(buff + 16, column_offset);
        buff = buff + 24;
        // write data
        memcpy(buff, column.data().c_str(), column_compressed_size);
        buff = buff + column_compressed_size;
      }
      dst->set_compress_type(compress_type_);
      for (int i = 0; i < dst->columns_size(); ++i) {
        dst->mutable_columns(i)->clear_data();
      }
    }
    return KStatus::SUCCESS;
  }
  return KStatus::SUCCESS;
}
}  // namespace kwdbts
