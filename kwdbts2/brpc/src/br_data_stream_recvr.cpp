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

#include "br_data_stream_recvr.h"

#include <unordered_map>

#include "br_sender_queue.h"
#include "ee_sort_chunk_merger.h"

namespace kwdbts {

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, const KQueryId& query_id,
                                 KProcessorId dest_processor_id, k_int32 num_senders,
                                 k_int32 total_buffer_limit, k_bool is_merging, k_bool keep_order,
                                 PassThroughChunkBuffer* pass_through_chunk_buffer)
    : mgr_(stream_mgr),
      query_id_(query_id),
      dest_processor_id_(dest_processor_id),
      total_buffer_limit_(total_buffer_limit),
      is_merging_(is_merging),
      keep_order_(keep_order),
      num_buffered_bytes_(0),
      pass_through_context_(pass_through_chunk_buffer, query_id, dest_processor_id),
      next_queue_index_(0) {
  k_int32 num_queues = is_merging ? num_senders : 1;
  sender_queues_.reserve(num_queues);
  k_int32 num_sender_per_queue = is_merging ? 1 : num_senders;
  for (int i = 0; i < num_queues; ++i) {
    SenderQueue* queue =
        sender_queue_pool_.Add(new PipelineSenderQueue(this, num_sender_per_queue));
    sender_queues_.push_back(queue);
  }

  pass_through_context_.Init();
}

BRStatus DataStreamRecvr::AddChunks(const PTransmitChunkParams& request,
                                    ::google::protobuf::Closure** done) {
  auto notify = this->DeferNotify();
  k_int32 sender_id = request.sender_id();
  k_int32 queue_index = 0;

  if (is_merging_) {
    // Use the sender_id to queue_index mapping with mutex protection
    {
      std::lock_guard<std::mutex> lock(sender_map_mutex_);
      auto it = sender_to_queue_map_.find(sender_id);
      if (it != sender_to_queue_map_.end()) {
        // Use existing mapping
        queue_index = it->second;
      } else {
        // Create new mapping using next available queue
        k_int32 current_index = next_queue_index_.load();
        if (current_index < sender_queues_.size()) {
          queue_index = current_index;
          sender_to_queue_map_[sender_id] = queue_index;
          next_queue_index_.fetch_add(1);
        } else {
          std::ostringstream oss;
          oss << "query_id: " << request.query_id() << " sender_id: " << sender_id
              << " has no queue";
          auto msg = oss.str();
          LOG_ERROR("%s", msg.c_str());
          return BRStatus::InternalError(msg);
        }
      }
    }
  } else {
    // When not merging, always use queue 0
    queue_index = 0;
  }

  if (keep_order_) {
    return sender_queues_[queue_index]->AddChunksAndKeepOrder(request, done);
  } else {
    return sender_queues_[queue_index]->AddChunks(request, done);
  }
}

void DataStreamRecvr::RemoveSender(k_int32 sender_id, k_int32 be_number) {
  auto notify = this->DeferNotify(-1, "");  // -1 finish 1

  // Find the queue associated with this sender_id, with mutex protection
  k_int32 queue_index = 0;
  if (is_merging_) {
    std::lock_guard<std::mutex> lock(sender_map_mutex_);
    auto it = sender_to_queue_map_.find(sender_id);
    if (it != sender_to_queue_map_.end()) {
      queue_index = it->second;
    } else {
      k_int32 current_index = next_queue_index_.load();
      if (current_index < sender_queues_.size()) {
        queue_index = current_index;
        sender_to_queue_map_[sender_id] = queue_index;
        next_queue_index_.fetch_add(1);
      } else {
        std::ostringstream oss;
        oss << "query_id: " << QueryId() << " sender_id: " << sender_id << " has no queue";
        auto msg = oss.str();
        LOG_ERROR("%s", msg.c_str());
      }
    }
  }
  sender_queues_[queue_index]->DecrementSenders(be_number);
}

void DataStreamRecvr::CancelStream() {
  for (auto& _sender_queue : sender_queues_) {
    _sender_queue->Cancel();
  }
}

void DataStreamRecvr::Close() {
  if (closed_) {
    return;
  }

  for (auto& _sender_queue : sender_queues_) {
    _sender_queue->Close();
  }
  mgr_->DeregisterRecvr(QueryId(), DestProcessorId());
  mgr_ = nullptr;
  closed_ = true;
}

DataStreamRecvr::~DataStreamRecvr() {
  if (!closed_) {
    Close();
  }
  if (mgr_ != nullptr) {
    LOG_ERROR("recvr is not closed, query_id=%ld, dest_processor_id=%d", query_id_,
              dest_processor_id_);
  }
}

KStatus DataStreamRecvr::GetChunkForPipeline(std::unique_ptr<DataChunk>* chunk,
                                             const k_int32 driver_sequence) {
  DCHECK_EQ(sender_queues_.size(), 1);
  DataChunk* tmp_chunk = nullptr;
  KStatus status = sender_queues_[0]->GetChunk(&tmp_chunk, driver_sequence);
  if (status != KStatus::SUCCESS) {
    LOG_ERROR("failed to get chunk, query_id=%ld, dest_processor_id=%d", query_id_,
              dest_processor_id_);
  }
  chunk->reset(tmp_chunk);
  return status;
}

KStatus DataStreamRecvr::GetNextForPipeline(std::unique_ptr<DataChunk>* chunk,
                                            std::atomic<k_bool>* eos, k_bool* should_exit) {
  DCHECK(cascade_merger_);
  KStatus status = cascade_merger_->GetNextMergeChunk(chunk, eos, should_exit);
  if (status != KStatus::SUCCESS) {
    LOG_ERROR("failed to get merge chunk, query_id=%ld, dest_processor_id=%d", query_id_,
              dest_processor_id_);
  }
  return status;
}

void DataStreamRecvr::ShortCircuitForPipeline(const k_int32 driver_sequence) {
  auto* sender_queue = static_cast<PipelineSenderQueue*>(sender_queues_[0]);
  return sender_queue->ShortCircuit(0);
}

k_bool DataStreamRecvr::HasOutputForPipeline(const k_int32 driver_sequence) const {
  auto* sender_queue = static_cast<PipelineSenderQueue*>(sender_queues_[0]);
  return sender_queue->HasOutput(0);
}

k_bool DataStreamRecvr::IsFinished() const {
  auto* sender_queue = static_cast<PipelineSenderQueue*>(sender_queues_[0]);
  return sender_queue->IsFinished();
}

k_bool DataStreamRecvr::IsDataReady() {
  if (cascade_merger_) {
    return cascade_merger_->IsDataReady();
  }
  return false;
}

KStatus DataStreamRecvr::CreateMergerForPipeline(kwdbContext_p ctx,
                                                 const std::vector<k_uint32>* order_column,
                                                 const std::vector<k_bool>* is_asc,
                                                 const std::vector<k_bool>* is_null_first) {
  DCHECK(is_merging_);
  cascade_merger_ = std::make_unique<CascadeDataChunkMerger>(ctx);
  std::vector<DataChunkProvider> providers;
  for (SenderQueue* q : sender_queues_) {
    DataChunkProvider provider = [q](DataChunkPtr* outChunk, k_bool* eos) -> bool {
      // data ready
      if (outChunk == nullptr || eos == nullptr) {
        return q->HasChunk();
      }
      if (!q->HasChunk()) {
        return false;
      }
      DataChunk* chunk;
      if (q->TryGetChunk(&chunk)) {
        outChunk->reset(chunk);
        return true;
      }
      *eos = true;
      return false;
    };
    providers.push_back(std::move(provider));
  }

  if (!cascade_merger_->Init(providers, order_column, is_asc, is_null_first)) {
    LOG_ERROR("failed to init cascade merger, query_id=%ld, dest_processor_id=%d", query_id_,
              dest_processor_id_);
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

}  // end namespace kwdbts
