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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "br_data_stream_mgr.h"
#include "br_internal_service.pb.h"
#include "br_object_pool.h"
#include "br_pass_through_chunk_buffer.h"
#include "ee_data_chunk.h"
#include "br_defer.h"
#include "ee_sort_chunk_merger.h"
#include "kwdb_type.h"

namespace kwdbts {

class DataStreamMgr;
class PTransmitChunkParams;
class PassThroughChunkBuffer;
class PassThroughContext;

class DataStreamRecvr {
 public:
  ~DataStreamRecvr();

  KStatus GetChunkForPipeline(std::unique_ptr<DataChunk>* chunk, const k_int32 driver_sequence);
  KStatus GetNextForPipeline(std::unique_ptr<DataChunk>* chunk, std::atomic<k_bool>* eos,
                             k_bool* should_exit);
  // Deregister from DataStreamMgr instance, which shares ownership of this
  // instance.
  void Close();

  const KQueryId& QueryId() const { return query_id_; }
  KProcessorId DestProcessorId() const { return dest_processor_id_; }

  void ShortCircuitForPipeline(const k_int32 driver_sequence);

  k_bool HasOutputForPipeline(const k_int32 driver_sequence) const;

  k_bool IsFinished() const;
  k_bool IsDataReady();
  auto DeferNotify(k_int32 code = 0, const std::string& msg = "") {
    return DeferOp([this, code, msg] {
      std::lock_guard<std::mutex> lock(notify_mutex_);
      if (notify_source_op_callback_) {
        notify_source_op_callback_(0, code, msg);
      }
    });
  }

  void SetReceiveNotify(ReceiveNotify notify) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    notify_source_op_callback_ = notify;
  }

  void SendErrorNotify(k_int32 code, const string& msg) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    if (notify_source_op_callback_) {
      notify_source_op_callback_(0, code, msg);
    }
  }

  PassThroughContext& GetPassThroughContext() { return pass_through_context_; }
  KStatus CreateMergerForPipeline(kwdbContext_p ctx, const std::vector<k_uint32>* order_column,
                                  const std::vector<k_bool>* is_asc,
                                  const std::vector<k_bool>* is_null_first);

 private:
  friend class DataStreamMgr;
  class SenderQueue;
  class PipelineSenderQueue;

  DataStreamRecvr(DataStreamMgr* stream_mgr, const KQueryId& query_id,
                  KProcessorId dest_processor_id, k_int32 num_senders, k_int32 total_buffer_limit,
                  k_bool is_merging, k_bool keep_order,
                  PassThroughChunkBuffer* pass_through_chunk_buffer);

  // If receive queue is full, done is enqueue pending, and return with *done is
  // nullptr
  BRStatus AddChunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

  // Indicate that a particular sender is done. Delegated to the appropriate
  // sender queue. Called from DataStreamMgr.
  void RemoveSender(k_int32 sender_id, k_int32 be_number);

  // Empties the sender queues and notifies all waiting consumers of
  // cancellation.
  void CancelStream();

  // Return true if the addition of a new batch of size 'chunk_size' would
  // exceed the total buffer limit.
  k_bool ExceedsLimit(k_int32 chunk_size) {
    return num_buffered_bytes_ + chunk_size > total_buffer_limit_;
  }

  // DataStreamMgr instance used to create this recvr. (Not owned)
  DataStreamMgr* mgr_;

  // Query id and destination processor id of the destination exchange node this
  // receiver is used by.
  KQueryId query_id_;
  KProcessorId dest_processor_id_;
  k_bool is_merging_{false};
  k_bool keep_order_{false};

  // soft upper limit on the total amount of buffering allowed for this stream
  // across all sender queues. we stop acking incoming data once the amount of
  // buffered data exceeds this value
  k_int32 total_buffer_limit_;

  // total number of bytes held across all sender queues.
  std::atomic<k_int32> num_buffered_bytes_{0};

  // One or more queues of row batches received from senders. If is_merging_ is
  // true, there is one SenderQueue for each sender. Otherwise, row batches from
  // all senders are placed in the same SenderQueue. The SenderQueue instances
  // are owned by the receiver and placed in sender_queue_pool_.
  std::vector<SenderQueue*> sender_queues_;

  // Pool of sender queues.
  ObjectPool sender_queue_pool_;

  PassThroughContext pass_through_context_;

  k_bool closed_ = false;
  ReceiveNotify notify_source_op_callback_{nullptr};

  // Merger for merging sorted chunks in cascade style
  std::unique_ptr<DataChunkMerger> cascade_merger_;

  // Map from sender_id to queue index in sender_queues_
  std::unordered_map<k_int32, k_int32> sender_to_queue_map_;
  // Mutex to protect concurrent access to sender_to_queue_map_
  mutable std::mutex sender_map_mutex_;

  // Next available queue index for mapping (atomic for thread safety)
  std::atomic<k_int32> next_queue_index_{0};

  // notify mutex
  mutable std::mutex notify_mutex_;
};

}  // namespace kwdbts
