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
  std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
  // ensure receivers are properly closed before the instances are released
  for (k_int32 i = 0; i < BUCKET_NUM; ++i) {
    recvrs.clear();
    {
      // fill recvrs under lock
      std::lock_guard<Mutex> l(lock_[i]);
      for (auto& iter : receiver_map_[i]) {
        for (auto& sub_iter : *(iter.second)) {
          recvrs.push_back(sub_iter.second);
        }
      }
    }

    for (auto& recvr : recvrs) {
      if (recvr) {
        recvr->Close();
      }
    }
  }

  pass_through_chunk_buffer_manager_.Close();
}

inline k_uint32 DataStreamMgr::GetBucket(const KQueryId& query_id) { return query_id % BUCKET_NUM; }

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
        "failed to create recvr, query_id=%ld, dest_processor_id=%d, error:new "
        "DataStreamRecvr failed",
        query_id, dest_processor_id);
    return nullptr;
  }

  k_uint32 bucket = GetBucket(query_id);
  auto& receiver_map = receiver_map_[bucket];
  std::lock_guard<Mutex> l(lock_[bucket]);
  auto iter = receiver_map.find(query_id);
  if (iter == receiver_map.end()) {
    receiver_map.insert(std::make_pair(query_id, std::make_shared<RecvrMap>()));
    iter = receiver_map.find(query_id);
    processor_count_ += 1;
  }
  iter->second->insert(std::make_pair(dest_processor_id, recvr));
  receiver_count_ += 1;

  return recvr;
}

std::shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvr(const KQueryId& query_id,
                                                          KProcessorId dest_processor_id) {
  k_uint32 bucket = GetBucket(query_id);
  auto& receiver_map = receiver_map_[bucket];
  std::lock_guard<Mutex> l(lock_[bucket]);

  auto iter = receiver_map.find(query_id);
  if (iter != receiver_map.end()) {
    auto sub_iter = iter->second->find(dest_processor_id);
    if (sub_iter != iter->second->end()) {
      return sub_iter->second;
    }
  }

  return nullptr;
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

BRStatus DataStreamMgr::SendExecStatus(const PSendExecStatus& request) {
  const KQueryId& query_id = request.query_id();
  const KProcessorId& dest_processor_id = request.dest_processor();
  std::shared_ptr<DataStreamRecvr> recvr = FindRecvr(query_id, dest_processor_id);
  if (recvr == nullptr) {
    LOG_ERROR(
        "SendExecStatus failed, recvr is nullptr, query_id=%ld, "
        "dest_processor_id=%d",
        query_id, dest_processor_id);
    return BRStatus::NotFoundRecv("recvr is nullptr");
  }

  if (request.exec_status() == EXEC_STATUS_FAILED) {
    recvr->SendErrorNotify(request.error_code(), request.error_msg());
  }

  return BRStatus::OK();
}

BRStatus DataStreamMgr::TransmitChunk(const PTransmitChunkParams& request,
                                      ::google::protobuf::Closure** done) {
  const KQueryId& query_id = request.query_id();
  const KProcessorId& dest_processor_id = request.dest_processor();
  std::shared_ptr<DataStreamRecvr> recvr = FindRecvr(query_id, dest_processor_id);
  if (recvr == nullptr) {
    LOG_ERROR(
        "TransmitChunk failed, recvr is nullptr, query_id=%ld, "
        "dest_processor_id=%d",
        query_id, dest_processor_id);
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

void DataStreamMgr::DeregisterRecvr(const KQueryId& query_id, KProcessorId dest_processor_id) {
  std::shared_ptr<DataStreamRecvr> target_recvr;
  k_uint32 bucket = GetBucket(query_id);
  auto& receiver_map = receiver_map_[bucket];
  {
    std::lock_guard<Mutex> l(lock_[bucket]);
    auto iter = receiver_map.find(query_id);
    if (iter != receiver_map.end()) {
      auto sub_iter = iter->second->find(dest_processor_id);
      if (sub_iter != iter->second->end()) {
        target_recvr = sub_iter->second;
        iter->second->erase(sub_iter);
        receiver_count_ -= 1;

        if (iter->second->empty()) {
          receiver_map.erase(iter);
          processor_count_ -= 1;
        }
      }
    }
  }

  // Notify concurrent add_data() requests that the stream has been terminated.
  // cancel_stream maybe take a long time, so we handle it out of lock.
  if (target_recvr) {
    target_recvr->CancelStream();
  } else {
    LOG_ERROR("unknown row receiver id: query_id=%ld, dest_processor_id=%d", query_id,
              dest_processor_id);
  }
}

void DataStreamMgr::Close() {
  for (k_size_t i = 0; i < BUCKET_NUM; i++) {
    std::lock_guard<Mutex> l(lock_[i]);
    for (auto& iter : receiver_map_[i]) {
      for (auto& sub_iter : *iter.second) {
        sub_iter.second->CancelStream();
      }
    }
  }
}

void DataStreamMgr::Cancel(const KQueryId& query_id) {
  std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
  k_uint32 bucket = GetBucket(query_id);
  auto& receiver_map = receiver_map_[bucket];
  {
    std::lock_guard<Mutex> l(lock_[bucket]);
    auto iter = receiver_map.find(query_id);
    if (iter != receiver_map.end()) {
      for (auto& sub_iter : *iter->second) {
        recvrs.push_back(sub_iter.second);
      }
    }
  }

  // cancel_stream maybe take a long time, so we handle it out of lock.
  for (auto& it : recvrs) {
    it->CancelStream();
  }
}

KStatus DataStreamMgr::PreparePassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.OpenQueryInstance(query_id);
}

KStatus DataStreamMgr::DestroyPassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.CloseQueryInstance(query_id);
}

PassThroughChunkBuffer* DataStreamMgr::GetPassThroughChunkBuffer(const KQueryId& query_id) {
  return pass_through_chunk_buffer_manager_.Get(query_id);
}

}  // namespace kwdbts
