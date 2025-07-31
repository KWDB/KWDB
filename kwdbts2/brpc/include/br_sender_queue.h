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

#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "br_data_stream_mgr.h"
#include "br_data_stream_recvr.h"
#include "br_internal_service.pb.h"
#include "ee_data_chunk.h"
#include "kwdb_type.h"

namespace kwdbts {

using ChunkUniquePtrVector = std::vector<std::pair<DataChunkPtr, k_int32>>;

class DataStreamRecvr::SenderQueue {
 public:
  explicit SenderQueue(DataStreamRecvr* parent_recvr) : recvr_(parent_recvr) {}
  virtual ~SenderQueue() {
    std::lock_guard<std::mutex> lock(col_info_mutex_);
    SafeDeleteArray(col_info_);
    col_num_ = 0;
    col_info_initialized_.store(false, std::memory_order_relaxed);
  }

  // get chunk from this sender queue, driver_sequence is only meaningful in
  // pipeline engine
  virtual KStatus GetChunk(DataChunk** chunk, const k_int32 driver_sequence = -1) = 0;
  virtual KStatus TryGetChunk(DataChunk** chunk) = 0;
  // add chunks to this sender queue if this stream has not been cancelled
  virtual BRStatus AddChunks(const PTransmitChunkParams& request,
                             ::google::protobuf::Closure** done) = 0;

  virtual BRStatus AddChunksAndKeepOrder(const PTransmitChunkParams& request,
                                         ::google::protobuf::Closure** done) = 0;

  // Decrement the number of remaining senders for this queue
  virtual void DecrementSenders(k_int32 be_number) = 0;

  virtual void Cancel() = 0;

  virtual void Close() = 0;

  virtual k_bool HasChunk() = 0;

 protected:
  KStatus BuildChunkMeta(const ChunkPB& pb_chunk);
  KStatus DeserializeChunk(const ChunkPB& pchunk, DataChunkPtr& chunk);

  DataStreamRecvr* recvr_ = nullptr;
  ColumnInfo* col_info_{nullptr};
  k_int32 col_num_{0};

  mutable std::mutex col_info_mutex_;
  std::atomic<bool> col_info_initialized_{false};
};

class DataStreamRecvr::PipelineSenderQueue final : public DataStreamRecvr::SenderQueue {
 public:
  PipelineSenderQueue(DataStreamRecvr* parent_recvr, k_int32 num_senders);
  ~PipelineSenderQueue() override {
    CheckLeakClosure();
    Close();
  }

  KStatus GetChunk(DataChunk** chunk, const k_int32 driver_sequence = -1) override;
  KStatus TryGetChunk(DataChunk** chunk) override;
  k_bool HasChunk() override;
  BRStatus AddChunks(const PTransmitChunkParams& request,
                     ::google::protobuf::Closure** done) override;

  BRStatus AddChunksAndKeepOrder(const PTransmitChunkParams& request,
                                 ::google::protobuf::Closure** done) override;

  void DecrementSenders(k_int32 be_number) override;

  void Cancel() override;

  void Close() override;

  void ShortCircuit(const k_int32 driver_sequence);

  k_bool HasOutput(const k_int32 driver_sequence);

  k_bool IsFinished() const;

 private:
  struct ChunkItem {
    k_int64 chunk_bytes = 0;
    // Invalid if SenderQueue::is_pipeline_level_shuffle_ is false
    k_int32 driver_sequence = -1;
    // When the memory of the ChunkQueue exceeds the limit,
    // we have to hold closure of the request, so as not to let the sender
    // continue to send data. A Request may have multiple Chunks, so only when
    // the last DataChunk of the Request is consumed, the callback is
    // closed->run() Let the sender continue to send data
    google::protobuf::Closure* closure = nullptr;
    // if pass_through is used, the chunk will be stored in chunk_ptr.
    // otherwise, the chunk that has not been deserialized will be stored in
    // pchunk and deserialized lazily during get_chunk
    DataChunkPtr chunk_ptr;
    ChunkPB pchunk;
    // Time in nano of saving closure
    k_int64 queue_enter_time = -1;

    ChunkItem() = default;

    ChunkItem(k_int64 chunk_bytes, k_int32 driver_sequence, google::protobuf::Closure* closure,
              DataChunkPtr&& chunk_ptr)
        : chunk_bytes(chunk_bytes),
          driver_sequence(driver_sequence),
          closure(closure),
          chunk_ptr(std::move(chunk_ptr)) {}

    ChunkItem(k_int64 chunk_bytes, k_int32 driver_sequence, google::protobuf::Closure* closure,
              ChunkPB pchunk)
        : chunk_bytes(chunk_bytes),
          driver_sequence(driver_sequence),
          closure(closure),
          pchunk(std::move(pchunk)) {}
  };

  typedef std::list<ChunkItem> ChunkList;

  class ChunkQueue {
   public:
    ChunkQueue() = default;
    ~ChunkQueue() = default;

    ChunkQueue(const ChunkQueue&) = delete;
    ChunkQueue& operator=(const ChunkQueue&) = delete;

    ChunkQueue(ChunkQueue&& other) noexcept
        : queue_(std::move(other.queue_)), size_(other.size_.load()) {
      other.size_.store(0);
    }

    ChunkQueue& operator=(ChunkQueue&& other) noexcept {
      if (this != &other) {
        queue_ = std::move(other.queue_);
        size_.store(other.size_.load());
        other.size_.store(0);
      }
      return *this;
    }

    k_bool Push(ChunkItem item) {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push(std::move(item));
      ++size_;
      return true;
    }

    k_bool TryPop(ChunkItem& item) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (queue_.empty()) {
        return false;
      }
      item = std::move(queue_.front());
      queue_.pop();
      --size_;
      return true;
    }

    k_size_t Size() const { return size_.load(); }

    k_bool IsEmpty() const { return size_.load() == 0; }

   private:
    std::queue<ChunkItem> queue_;
    mutable std::mutex mutex_;
    std::atomic<k_size_t> size_{0};
  };

  void CleanBufferQueues();
  void CheckLeakClosure();

  KStatus GetChunksFromPassThrough(const k_int32 sender_id, k_int32& total_chunk_bytes,
                                   ChunkList& chunks);

  template <k_bool need_deserialization>
  KStatus GetChunksFromRequest(const PTransmitChunkParams& request, k_int32& total_chunk_bytes,
                               ChunkList& chunks);

  KStatus TryToBuildChunkMeta(const PTransmitChunkParams& request);

  template <k_bool keep_order>
  BRStatus AddChunksInternal(const PTransmitChunkParams& request,
                             ::google::protobuf::Closure** done);

  std::atomic<k_bool> is_cancelled_{false};
  std::atomic<k_int32> num_remaining_senders_;

  typedef SpinLock Mutex;
  Mutex lock_;

  std::vector<ChunkQueue> chunk_queues_;

  struct ChunkQueueState {
    // Record the number of blocked closure in the queue
    std::atomic_int32_t blocked_closure_num;
    // Record whether the queue is in the unplug state.
    // In the unplug state, has_output will return true directly if there is a
    // chunk in the queue. Otherwise, it will try to batch enough chunks to
    // reduce the scheduling overhead.
    k_bool unpluging = false;
    std::atomic<k_bool> is_short_circuited;
  };
  std::vector<ChunkQueueState> chunk_queue_states_;
  std::atomic<k_int32> total_chunks_{0};
  std::atomic<k_int32> total_chunks_tmp_{0};
  std::unordered_set<k_int32> sender_eos_set_;
  std::unordered_map<k_int32, std::unordered_map<k_int64, ChunkList>> buffered_chunk_queues_;
  std::unordered_map<k_int32, k_int64> max_processed_sequences_;
  static constexpr k_int32 kUnplugBufferThreshold = 16;
};

}  // namespace kwdbts
