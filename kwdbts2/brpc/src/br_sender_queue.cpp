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

#include "br_sender_queue.h"

#include <brpc/server.h>
#include <brpc/socket.h>

#include <iomanip>

#include "br_data_stream_recvr.h"
#include "br_internal_service.h"
#include "br_mgr.h"
#include "ee_block_compress.h"
#include "ee_defer.h"
#include "ee_fast_string.h"
#include "ee_new_slice.h"
#include "ee_protobufC_serde.h"
#include "lg_api.h"

namespace kwdbts {

KStatus DataStreamRecvr::SenderQueue::BuildChunkMeta(const ChunkPB& pb_chunk) {
  if (col_info_initialized_.load(std::memory_order_acquire)) {
    return KStatus::SUCCESS;
  }

  std::lock_guard<std::mutex> lock(col_info_mutex_);

  if (col_info_initialized_.load(std::memory_order_relaxed)) {
    return KStatus::SUCCESS;
  }

  const auto& column_info_list = pb_chunk.column_info();
  if (!column_info_list.empty()) {
    const k_int32 new_col_num = static_cast<k_int32>(column_info_list.size());
    ColumnInfo* new_col_info = KNEW ColumnInfo[new_col_num];
    if (new_col_info == nullptr) {
      LOG_ERROR("Failed to allocate memory for column info, size: %d", new_col_num);
      return KStatus::FAIL;
    }

    for (k_int32 i = 0; i < new_col_num; ++i) {
      const auto& pb_col_info = column_info_list[i];
      auto& col_info = new_col_info[i];

      col_info.allow_null = pb_col_info.allow_null();
      col_info.fixed_storage_len = pb_col_info.fixed_storage_len();
      col_info.return_type = static_cast<KWDBTypeFamily>(pb_col_info.return_type());
      col_info.storage_len = pb_col_info.storage_len();
      col_info.storage_type = static_cast<roachpb::DataType>(pb_col_info.storage_type());
      col_info.is_string = pb_col_info.is_string();
    }

    col_num_ = new_col_num;
    col_info_ = new_col_info;

    col_info_initialized_.store(true, std::memory_order_release);
  }

  return KStatus::SUCCESS;
}

KStatus DataStreamRecvr::SenderQueue::DeserializeChunk(const ChunkPB& pchunk, DataChunkPtr& chunk) {
  if (UNLIKELY(!col_info_initialized_.load(std::memory_order_acquire))) {
    LOG_ERROR("Column info not initialized before deserialization");
    return KStatus::FAIL;
  }

  ProtobufChunkSerrialde serial;
  if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
    serial.Deserialize(chunk, pchunk.data(), pchunk.is_encoding(), col_info_, col_num_);
  } else {
    std::string_view buff = pchunk.data();
    auto* cur = reinterpret_cast<const uint8_t*>(buff.data());

    k_uint32 version = decode_fixed32(cur);
    if (version != 1) {
      LOG_ERROR("invalid version {%u}", version);
      return KStatus::FAIL;
    }
    cur += 4;
    k_uint32 num_rows = decode_fixed32(cur);
    cur += 4;
    k_uint32 capacity = decode_fixed32(cur);
    cur += 4;
    k_uint32 size = decode_fixed32(cur);
    cur += 8;
    size_t uncompressed_size = 0;
    size_t offset = 0;
    {
      const BlockCompressionCodec* codec = nullptr;
      if (KStatus::FAIL == GetBlockCompressionCodec(pchunk.compress_type(), &codec)) {
        LOG_ERROR("GetBlockCompressionCodec failed, compress_type: %d", pchunk.compress_type());
        return KStatus::FAIL;
      }
      for (k_int32 i = 0; i < col_num_; i++) {
        uncompressed_size = decode_fixed64(cur);
        cur += 8;
        k_int64 compressed_size = decode_fixed64(cur);
        cur += 8;
        k_int64 column_offset = decode_fixed64(cur);
        cur += 8;
        faststring uncompressed_buffer;
        uncompressed_buffer.resize(uncompressed_size);
        KSlice output{uncompressed_buffer.data(), uncompressed_size};
        KSlice compressed_data(cur, compressed_size);
        if (codec != nullptr && compressed_data.get_size() > 0) {
          if (KStatus::FAIL == codec->Decompress(compressed_data, &output)) {
            LOG_ERROR("decompress failed, queryid:%ld", recvr_->QueryId());
            return KStatus::FAIL;
          }
        }
        cur += compressed_size;
        std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer.data()),
                              uncompressed_size);
        auto* uncompress_buf = reinterpret_cast<const uint8_t*>(buff.data());
        if (chunk == nullptr) {
          chunk = std::make_unique<DataChunk>(col_info_, col_num_, capacity);
          if (chunk == nullptr) {
            LOG_ERROR("Deserialize make unique data chunk failed");
            return KStatus::FAIL;
          }
          chunk->Initialize();
          chunk->SetCount(num_rows);
        }
        memcpy(chunk->GetData() + offset, uncompress_buf, uncompressed_size);
        offset += uncompressed_size;
      }
    }
  }
  return KStatus::SUCCESS;
}

DataStreamRecvr::PipelineSenderQueue::PipelineSenderQueue(DataStreamRecvr* parent_recvr,
                                                          k_int32 num_senders)
    : SenderQueue(parent_recvr), num_remaining_senders_(num_senders), chunk_queue_states_(1) {
  chunk_queues_.emplace_back();
}

KStatus DataStreamRecvr::PipelineSenderQueue::GetChunk(DataChunk** chunk,
                                                       const k_int32 driver_sequence) {
  KStatus ret = KStatus::SUCCESS;
  if (is_cancelled_) {
    LOG_ERROR("Receiver is cancelled");
    return KStatus::FAIL;
  }

  auto& chunk_queue = chunk_queues_[0];
  auto& chunk_queue_state = chunk_queue_states_[0];

  ChunkItem item;
  if (!chunk_queue.TryPop(item)) {
    chunk_queue_state.unpluging = false;
    LOG_ERROR("DataStreamRecvr no new data, stop unpluging");
    return ret;
  }

  DeferOp defer_op([&]() {
    auto* closure = item.closure;
    if (closure != nullptr) {
      closure->Run();
      chunk_queue_state.blocked_closure_num--;
    }
  });

  if (item.chunk_ptr == nullptr) {
    DataChunkPtr chunk_ptr = nullptr;
    if (DeserializeChunk(item.pchunk, chunk_ptr)) {
      *chunk = chunk_ptr.release();
    } else {
      LOG_ERROR("DeserializeChunk failed");
      ret = KStatus::FAIL;
    }
  } else {
    *chunk = item.chunk_ptr.release();
  }

  total_chunks_--;
  recvr_->num_buffered_bytes_ -= item.chunk_bytes;

  return ret;
}

KStatus DataStreamRecvr::PipelineSenderQueue::TryGetChunk(DataChunk** chunk) {
  if (is_cancelled_) {
    LOG_ERROR("Receiver is cancelled");
    return KStatus::FAIL;
  }
  auto& chunk_queue = chunk_queues_[0];
  auto& chunk_queue_state = chunk_queue_states_[0];
  ChunkItem item;
  if (!chunk_queue.TryPop(item)) {
    chunk_queue_state.unpluging = false;
    return KStatus::FAIL;
  }
  DCHECK(item.chunk_ptr != nullptr);
  *chunk = item.chunk_ptr.release();

  auto* closure = item.closure;
  if (closure != nullptr) {
    closure->Run();
    chunk_queue_state.blocked_closure_num--;
  }
  total_chunks_--;
  return KStatus::SUCCESS;
}

BRStatus DataStreamRecvr::PipelineSenderQueue::AddChunks(const PTransmitChunkParams& request,
                                                         ::google::protobuf::Closure** done) {
  return AddChunksInternal<false>(request, done);
}

BRStatus DataStreamRecvr::PipelineSenderQueue::AddChunksAndKeepOrder(
    const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
  return AddChunksInternal<true>(request, done);
}

void DataStreamRecvr::PipelineSenderQueue::DecrementSenders(k_int32 be_number) {
  std::lock_guard<Mutex> l(lock_);
  if (UNLIKELY(sender_eos_set_.find(be_number) != sender_eos_set_.end())) {
    LOG_ERROR("More than one EOS from %d in query id %ld on processor id %d", be_number,
              recvr_->QueryId(), recvr_->DestProcessorId());
    return;
  }
  sender_eos_set_.insert(be_number);

  if (num_remaining_senders_ > 0) {
    num_remaining_senders_--;
  }
}

void DataStreamRecvr::PipelineSenderQueue::Cancel() {
  is_cancelled_ = true;
  CleanBufferQueues();
}

void DataStreamRecvr::PipelineSenderQueue::Close() { CleanBufferQueues(); }

void DataStreamRecvr::PipelineSenderQueue::CleanBufferQueues() {
  std::lock_guard<Mutex> l(lock_);

  for (k_int32 i = 0; i < chunk_queues_.size(); i++) {
    auto& chunk_queue = chunk_queues_[i];
    auto& chunk_queue_state = chunk_queue_states_[i];

    ChunkItem item;
    while (chunk_queue.Size() > 0) {
      if (chunk_queue.TryPop(item)) {
        if (item.closure != nullptr) {
          item.closure->Run();
          chunk_queue_state.blocked_closure_num--;
        }
        --total_chunks_;
        recvr_->num_buffered_bytes_ -= item.chunk_bytes;
      }
    }
  }

  for (auto& [_, chunk_queues] : buffered_chunk_queues_) {
    for (auto& [_, chunk_queue] : chunk_queues) {
      for (auto& item : chunk_queue) {
        if (item.closure != nullptr) {
          item.closure->Run();
        }
      }
      chunk_queue.clear();
    }
  }
}

void DataStreamRecvr::PipelineSenderQueue::CheckLeakClosure() {
  std::lock_guard<Mutex> l(lock_);

  // Check for leaks in the main queue
  for (k_int32 i = 0; i < chunk_queues_.size(); i++) {
    auto& chunk_queue = chunk_queues_[i];
    ChunkItem item;
    while (chunk_queue.Size() > 0) {
      if (chunk_queue.TryPop(item)) {
        if (item.closure != nullptr) {
          LOG_ERROR("leak closure detected");
        }
      }
    }
  }

  // Check for leaks in the buffered queues
  for (auto& [_, chunk_queues] : buffered_chunk_queues_) {
    for (auto& [_, chunk_queue] : chunk_queues) {
      for (auto& item : chunk_queue) {
        if (item.closure != nullptr) {
          LOG_ERROR("leak closure detected");
        }
      }
    }
  }
}

KStatus DataStreamRecvr::PipelineSenderQueue::TryToBuildChunkMeta(
    const PTransmitChunkParams& request) {
  if (request.use_pass_through()) {
    return KStatus::SUCCESS;
  }

  return BuildChunkMeta(request.chunks(0));
}

KStatus DataStreamRecvr::PipelineSenderQueue::GetChunksFromPassThrough(k_int32 sender_id,
                                                                       k_int32& total_chunk_bytes,
                                                                       ChunkList& chunks) {
  ChunkUniquePtrVector swap_chunks;
  std::vector<k_size_t> swap_bytes;

  recvr_->pass_through_context_.PullChunks(sender_id, &swap_chunks, &swap_bytes);
  if (swap_chunks.size() != swap_bytes.size()) {
    LOG_ERROR(
        "swap_chunks.size(%ld) != swap_bytes.size(%ld), query_id: %ld, "
        "sender_id: %d",
        swap_chunks.size(), swap_bytes.size(), recvr_->QueryId(), sender_id);
    return KStatus::FAIL;
  }

  for (k_int32 i = 0; i < swap_chunks.size(); i++) {
    chunks.emplace_back(swap_bytes[i], swap_chunks[i].second, nullptr,
                        std::move(swap_chunks[i].first));
    total_chunk_bytes += swap_bytes[i];
  }

  return KStatus::SUCCESS;
}

template <k_bool need_deserialization>
KStatus DataStreamRecvr::PipelineSenderQueue::GetChunksFromRequest(
    const PTransmitChunkParams& request, k_int32& total_chunk_bytes, ChunkList& chunks) {
  for (auto i = 0; i < request.chunks().size(); i++) {
    auto& pchunk = request.chunks().Get(i);
    k_int64 chunk_bytes = pchunk.data().size();
    if (need_deserialization) {
      DataChunkPtr chunk_ptr = nullptr;
      if (DeserializeChunk(pchunk, chunk_ptr)) {
        chunks.emplace_back(chunk_bytes, -1, nullptr, std::move(chunk_ptr));
      } else {
        LOG_ERROR("DeserializeChunk failed");
        return KStatus::FAIL;
      }
    } else {
      chunks.emplace_back(chunk_bytes, -1, nullptr, pchunk);
    }
    total_chunk_bytes += chunk_bytes;
  }

  return KStatus::SUCCESS;
}

k_bool DataStreamRecvr::PipelineSenderQueue::HasChunk() {
  if (is_cancelled_) {
    return true;
  }
  if (chunk_queues_[0].Size() == 0 && num_remaining_senders_ > 0) {
    return false;
  }
  return true;
}

template <k_bool keep_order>
BRStatus DataStreamRecvr::PipelineSenderQueue::AddChunksInternal(
    const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
  const k_bool use_pass_through = request.use_pass_through();

  if (request.chunks_size() == 0 && !use_pass_through) {
    std::ostringstream oss;
    oss << "query_id: " << request.query_id() << " has no chunks";
    auto msg = oss.str();
    LOG_ERROR("%s", msg.c_str());
    return BRStatus::InternalError(msg);
  }

  // Check que status
  if (is_cancelled_ || num_remaining_senders_ <= 0) {
    LOG_ERROR("query adds chunks to %s", is_cancelled_ ? "cancelled queue" : "ended queue");
    return BRStatus::OK();
  }

  // Building chunk metadata
  if (TryToBuildChunkMeta(request) != KStatus::SUCCESS) {
    std::ostringstream oss;
    oss << "try to build chunk meta failed";
    auto msg = oss.str();
    LOG_ERROR("%s", msg.c_str());
    return BRStatus::InternalError(msg);
  }

  k_int32 total_chunk_bytes = 0;

  ChunkList chunks;
  KStatus status = use_pass_through
                       ? GetChunksFromPassThrough(request.sender_id(), total_chunk_bytes, chunks)
                       : keep_order
                             ? GetChunksFromRequest<true>(request, total_chunk_bytes, chunks)
                             : GetChunksFromRequest<false>(request, total_chunk_bytes, chunks);
  if (status != KStatus::SUCCESS) {
    std::ostringstream oss;
    oss << "get chunks from request failed";
    auto msg = oss.str();
    LOG_ERROR("%s", msg.c_str());
    return BRStatus::InternalError(msg);
  }

  if (is_cancelled_) {
    LOG_ERROR("query is cancelled, query_id=%ld", request.query_id());
    return BRStatus::OK();
  }

  if (keep_order) {
    const k_int32 be_number = request.be_number();
    const k_int64 sequence = request.sequence();
    std::lock_guard<Mutex> l(lock_);

    if (is_cancelled_) {
      LOG_ERROR("Cancelled receiver cannot add_chunk for keep order!");
      return BRStatus::OK();
    }

    if (max_processed_sequences_.find(be_number) == max_processed_sequences_.end()) {
      max_processed_sequences_[be_number] = -1;
    }

    if (buffered_chunk_queues_.find(be_number) == buffered_chunk_queues_.end()) {
      buffered_chunk_queues_[be_number] = std::unordered_map<k_int64, ChunkList>();
    }

    auto& chunk_queues = buffered_chunk_queues_[be_number];

    if (!chunks.empty() && done != nullptr && recvr_->ExceedsLimit(total_chunk_bytes)) {
      chunks.back().closure = *done;
      *done = nullptr;
    }

    chunk_queues[sequence] = std::move(chunks);
    std::unordered_map<k_int64, ChunkList>::iterator it;
    k_int64& max_processed_sequence = max_processed_sequences_[be_number];

    // max_processed_sequence + 1 means the first unprocessed sequence
    while ((it = chunk_queues.find(max_processed_sequence + 1)) != chunk_queues.end()) {
      ChunkList& unprocessed_chunk_queue = (*it).second;

      // Now, all the packets with sequance <= unprocessed_sequence have been received
      // so chunks of unprocessed_sequence can be flushed to ready queue
      for (auto& item : unprocessed_chunk_queue) {
        size_t chunk_bytes = item.chunk_bytes;
        auto* closure = item.closure;
        chunk_queues_[0].Push(std::move(item));
        chunk_queue_states_[0].blocked_closure_num += closure != nullptr;
        total_chunks_++;
        recvr_->num_buffered_bytes_ += chunk_bytes;
      }

      chunk_queues.erase(it);
      ++max_processed_sequence;
    }
  } else {
    // If exceeds limit, append closure to the last chunk
    if (!chunks.empty() && done != nullptr && recvr_->ExceedsLimit(total_chunk_bytes)) {
      chunks.back().closure = *done;
      *done = nullptr;
    }

    if (chunk_queue_states_[0].is_short_circuited.load(std::memory_order_relaxed)) {
      chunks.clear();
    }

    // Process all chunks
    for (auto& chunk : chunks) {
      k_int32 chunk_bytes = chunk.chunk_bytes;
      auto* closure = chunk.closure;

      chunk_queues_[0].Push(std::move(chunk));
      chunk_queue_states_[0].blocked_closure_num += closure != nullptr;
      total_chunks_++;
      total_chunks_tmp_++;
      recvr_->num_buffered_bytes_ += chunk_bytes;
    }
  }

  if (is_cancelled_ || chunk_queue_states_[0].is_short_circuited.load(std::memory_order_relaxed)) {
    CleanBufferQueues();
  }

  return BRStatus::OK();
}

void DataStreamRecvr::PipelineSenderQueue::ShortCircuit(const k_int32 driver_sequence) {
  chunk_queue_states_[0].is_short_circuited.store(true, std::memory_order_relaxed);
}

k_bool DataStreamRecvr::PipelineSenderQueue::HasOutput(const k_int32 driver_sequence) {
  if (is_cancelled_.load()) {
    return false;
  }

  k_int32 chunk_num = chunk_queues_[0].Size();
  auto& chunk_queue_state = chunk_queue_states_[0];
  // introduce an unplug mechanism similar to scan operator to reduce scheduling
  // overhead

  // 1. in the unplug state, return true if there is a chunk, otherwise return
  // false and exit the unplug state
  if (chunk_queue_state.unpluging) {
    if (chunk_num > 0) {
      return true;
    }
    chunk_queue_state.unpluging = false;
    return false;
  }
  // 2. if this queue is not in the unplug state, try to batch as much chunk as
  // possible before returning
  if (chunk_num >= kUnplugBufferThreshold) {
    chunk_queue_state.unpluging = true;
    return true;
  }

  k_bool is_buffer_full = recvr_->num_buffered_bytes_ > recvr_->total_buffer_limit_;
  // 3. if buffer is full and this queue has chunks, return true to release the
  // buffer capacity ASAP
  if (is_buffer_full && chunk_num > 0) {
    return true;
  }
  // 4. if there is no new data, return true if this queue has chunks
  if (num_remaining_senders_ == 0) {
    return chunk_num > 0;
  }
  // 5. if this queue has blocked closures, return true to release the closure
  // ASAP to trigger the next transmit requests
  return chunk_queue_state.blocked_closure_num > 0;
}

k_bool DataStreamRecvr::PipelineSenderQueue::IsFinished() const {
  return is_cancelled_ || (num_remaining_senders_ == 0 && total_chunks_ == 0);
}

}  // namespace kwdbts
