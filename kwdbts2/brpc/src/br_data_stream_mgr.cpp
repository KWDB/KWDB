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

#include "br_data_stream_mgr.h"

#include "br_data_stream_recvr.h"
#include "br_internal_service.pb.h"
#include "ee_defer.h"

namespace kwdbts {

DataStreamMgr::~DataStreamMgr() {
  CleanupAllReceivers();
  pass_through_chunk_buffer_manager_.Close();
}

PassThroughChunkBuffer* DataStreamMgr::GetPassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.Get(query_id);
}

KStatus DataStreamMgr::PreparePassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.OpenQueryInstance(query_id);
}

KStatus DataStreamMgr::DestroyPassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.CloseQueryInstance(query_id);
}

std::shared_ptr<DataStreamRecvr> DataStreamMgr::CreateRecvr(const KQueryId& query_id,
                                                            KProcessorId dest_processor_id,
                                                            k_int32 num_senders,
                                                            k_int32 buffer_size, k_bool is_merging,
                                                            k_bool keep_order) {
  PassThroughChunkBuffer* pass_through_chunk_buffer = GetPassThroughChunkBuffer(query_id);
  std::shared_ptr<DataStreamRecvr> recvr(
      KNEW DataStreamRecvr(this, query_id, dest_processor_id, num_senders, buffer_size, is_merging,
                           keep_order, pass_through_chunk_buffer));
  if (recvr == nullptr) {
    LOG_ERROR(
        "Failed to create recvr, query_id=%ld, dest_processor_id=%d, error: new "
        "DataStreamRecvr failed",
        query_id, dest_processor_id);
    return nullptr;
  }

  RegisterRecvr(query_id, dest_processor_id, recvr);

  return recvr;
}

void DataStreamMgr::DeregisterRecvr(const KQueryId& query_id, KProcessorId dest_processor_id) {
  std::shared_ptr<DataStreamRecvr> target_recvr = RemoveRecvr(query_id, dest_processor_id);

  if (target_recvr) {
    target_recvr->CancelStream();
  } else {
    LOG_ERROR("unknown row receiver id: query_id=%ld, dest_processor_id=%d", query_id,
              dest_processor_id);
  }
}

BRStatus DataStreamMgr::DialDataRecvr(const PDialDataRecvr& request) {
  const KQueryId& query_id = request.query_id();
  const KProcessorId& dest_processor_id = request.dest_processor();
  std::shared_ptr<DataStreamRecvr> recvr = FindRecvr(query_id, dest_processor_id);
  if (recvr == nullptr) {
    return BRStatus::NotReady("recvr is nullptr");
  }

  return BRStatus::OK();
}

BRStatus DataStreamMgr::TransmitChunk(const PTransmitChunkParams& request,
                                      ::google::protobuf::Closure** done) {
  const KQueryId& query_id = request.query_id();
  const KProcessorId& dest_processor_id = request.dest_processor();
  std::shared_ptr<DataStreamRecvr> recvr = FindRecvr(query_id, dest_processor_id);
  if (recvr == nullptr) {
    // LOG_ERROR(
    //     "TransmitChunk failed, recvr is nullptr, query_id=%ld, "
    //     "dest_processor_id=%d",
    //     query_id, dest_processor_id);
    return BRStatus::NotFoundRecv("recvr is nullptr");
  }

  k_bool eos = request.eos();

  DeferOp op([&eos, &recvr, &request]() {
    if (eos) {
      recvr->RemoveSender(request.sender_id(), request.be_number());
    }
  });

  if (request.chunks_size() > 0 || request.use_pass_through()) {
    return recvr->AddChunks(request, eos ? nullptr : done);
  }

  return BRStatus::OK();
}

BRStatus DataStreamMgr::SendExecStatus(const PSendExecStatus& request) {
  const KQueryId& query_id = request.query_id();
  const KProcessorId& dest_processor_id = request.dest_processor();
  std::shared_ptr<DataStreamRecvr> recvr = FindRecvr(query_id, dest_processor_id);
  if (recvr == nullptr) {
    // LOG_ERROR(
    //     "SendExecStatus failed, recvr is nullptr, query_id=%ld, "
    //     "dest_processor_id=%d",
    //     query_id, dest_processor_id);
    return BRStatus::NotFoundRecv("recvr is nullptr");
  }

  if (request.exec_status() == EXEC_STATUS_FAILED) {
    recvr->SendErrorNotify(request.error_code(), request.error_msg());
  }

  return BRStatus::OK();
}

void DataStreamMgr::CancelRecvrStream(const std::shared_ptr<DataStreamRecvr>& recvr) {
  if (recvr) {
    recvr->CancelStream();
  }
}

void DataStreamMgr::CloseRecvrStream(const std::shared_ptr<DataStreamRecvr>& recvr) {
  if (recvr) {
    recvr->Close();
  }
}

}  // namespace kwdbts
