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
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include "br_global.h"
#include "br_pass_through_chunk_buffer.h"
#include "kwdb_type.h"

namespace google::protobuf {
class Closure;
}

namespace kwdbts {

class DataStreamRecvr;
class PTransmitChunkParams;

class DataStreamMgr {
 public:
  DataStreamMgr() = default;
  ~DataStreamMgr();

  // Create a receiver for a specific query_id/dest_processor_id destination;
  // If is_merging is true, the receiver maintains a separate queue of incoming
  // row batches for each sender and merges the sorted streams from each sender
  // into a single stream. Ownership of the receiver is shared between this
  // DataStream mgr instance and the caller.
  std::shared_ptr<DataStreamRecvr> CreateRecvr(const KQueryId& query_id,
                                               KProcessorId dest_processor_id, k_int32 num_senders,
                                               k_int32 buffer_size, k_bool is_merging,
                                               k_bool keep_order);

  BRStatus DialDataRecvr(const PDialDataRecvr& request);

  BRStatus TransmitChunk(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);

  BRStatus SendExecStatus(const PSendExecStatus& request);

  // Closes all receivers registered for query_id immediately.
  void Cancel(const KQueryId& query_id);
  void Close();

  KStatus PreparePassThroughChunkBuffer(const KQueryId& query_id);
  KStatus DestroyPassThroughChunkBuffer(const KQueryId& query_id);
  PassThroughChunkBuffer* GetPassThroughChunkBuffer(const KQueryId& query_id);

 private:
  friend class DataStreamRecvr;
  static const k_uint32 BUCKET_NUM = 127;

  // protects all fields below
  typedef std::mutex Mutex;
  Mutex lock_[BUCKET_NUM];

  // map from hash value of fragment instance id/node id pair to stream
  // receivers; Ownership of the stream revcr is shared between this instance
  // and the caller of CreateRecvr().
  typedef std::unordered_map<KProcessorId, std::shared_ptr<DataStreamRecvr>> RecvrMap;
  typedef std::unordered_map<KQueryId, std::shared_ptr<RecvrMap>> StreamMap;
  StreamMap receiver_map_[BUCKET_NUM];
  std::atomic<k_uint32> processor_count_{0};
  std::atomic<k_uint32> receiver_count_{0};

  // Return the receiver for given query_id/dest_processor_id,
  // or NULL if not found.
  std::shared_ptr<DataStreamRecvr> FindRecvr(const KQueryId& query_id,
                                             KProcessorId dest_processor_id);

  // Remove receiver block for query_id/dest_processor_id from the map.
  void DeregisterRecvr(const KQueryId& query_id, KProcessorId dest_processor_id);

  k_uint32 GetBucket(const KQueryId& query_id);

  PassThroughChunkBufferManager pass_through_chunk_buffer_manager_;
};

}  // namespace kwdbts
