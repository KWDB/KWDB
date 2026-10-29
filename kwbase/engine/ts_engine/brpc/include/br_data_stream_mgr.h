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
#include <utility>
#include <vector>

#include "br_global.h"
#include "br_pass_through_chunk_buffer.h"
#include "kwdb_type.h"

namespace google::protobuf {
class Closure;
}

namespace kwdbts {

class PTransmitChunkParams;
class DataStreamRecvr;

// DataStreamMgr manages the lifecycle and access to DataStreamRecvr objects and
// pass-through chunk buffers for distributed data streaming.
class DataStreamMgr {
 public:
  DataStreamMgr() = default;
  ~DataStreamMgr();

  // Creates a new DataStreamRecvr for the given query and processor.
  std::shared_ptr<DataStreamRecvr> CreateRecvr(const KQueryId& query_id,
                                               KProcessorId dest_processor_id, k_int32 num_senders,
                                               k_int32 buffer_size, k_bool is_merging,
                                               k_bool keep_order);

  // Closes all receivers and releases resources.
  void Close() {
    for (k_size_t i = 0; i < kBucketNum; ++i) {
      std::lock_guard<Mutex> lock(lock_[i]);
      for (auto& receiver_pair : receiver_map_[i]) {
        for (auto& recvr_pair : *receiver_pair.second) {
          CancelRecvrStream(recvr_pair.second);
        }
      }
    }
  }

  // Cancels all receivers associated with the given query.
  void Cancel(const KQueryId& query_id) {
    std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
    k_uint32 bucket = GetBucket(query_id);
    auto& receiver_map = receiver_map_[bucket];
    {
      std::lock_guard<Mutex> lock(lock_[bucket]);
      auto iter = receiver_map.find(query_id);
      if (iter != receiver_map.end()) {
        for (const auto& recvr_pair : *iter->second) {
          recvrs.push_back(recvr_pair.second);
        }
      }
    }

    for (const auto& recvr : recvrs) {
      CancelRecvrStream(recvr);
    }
  }

  // Gets the pass-through chunk buffer for the given query.
  PassThroughChunkBuffer* GetPassThroughChunkBuffer(const KQueryId& query_id);

  // Prepares a pass-through chunk buffer for the given query.
  KStatus PreparePassThroughChunkBuffer(const KQueryId& query_id);

  // Destroys the pass-through chunk buffer for the given query.
  KStatus DestroyPassThroughChunkBuffer(const KQueryId& query_id);

  // Handles a request to dial a data receiver.
  BRStatus DialDataRecvr(const PDialDataRecvr& request);

  // Handles a request to transmit a chunk.
  BRStatus TransmitChunk(const PTransmitChunkParams& request, google::protobuf::Closure** done);

  // Handles a request to send execution status.
  BRStatus SendExecStatus(const PSendExecStatus& request);

 private:
  static constexpr k_uint32 kBucketNum = 127;
  friend class DataStreamRecvr;

  using RecvrMap = std::unordered_map<KProcessorId, std::shared_ptr<DataStreamRecvr>>;
  using StreamMap = std::unordered_map<KQueryId, std::shared_ptr<RecvrMap>>;
  using Mutex = std::mutex;

  PassThroughChunkBufferManager pass_through_chunk_buffer_manager_;

  StreamMap receiver_map_[kBucketNum];

  std::atomic<k_uint32> processor_count_{0};
  std::atomic<k_uint32> receiver_count_{0};

  Mutex lock_[kBucketNum];

  // Returns the bucket index for the given query_id.
  k_uint32 GetBucket(const KQueryId& query_id) { return query_id % kBucketNum; }

  // Registers the receiver for the given query and processor.
  void RegisterRecvr(const KQueryId& query_id, KProcessorId dest_processor_id,
                     const std::shared_ptr<DataStreamRecvr>& recvr) {
    k_uint32 bucket = GetBucket(query_id);
    auto& receiver_map = receiver_map_[bucket];
    std::lock_guard<Mutex> lock(lock_[bucket]);
    auto iter = receiver_map.find(query_id);
    if (iter == receiver_map.end()) {
      receiver_map.insert(std::make_pair(query_id, std::make_shared<RecvrMap>()));
      iter = receiver_map.find(query_id);
      processor_count_ += 1;
    }
    iter->second->insert(std::make_pair(dest_processor_id, recvr));
    receiver_count_ += 1;
  }

  // Deregisters the receiver for the given query and processor.
  void DeregisterRecvr(const KQueryId& query_id, KProcessorId dest_processor_id);

  // Finds the receiver for the given query and processor.
  std::shared_ptr<DataStreamRecvr> FindRecvr(const KQueryId& query_id,
                                             KProcessorId dest_processor_id) {
    k_uint32 bucket = GetBucket(query_id);
    auto& receiver_map = receiver_map_[bucket];
    std::lock_guard<Mutex> lock(lock_[bucket]);

    auto iter = receiver_map.find(query_id);
    if (iter != receiver_map.end()) {
      auto sub_iter = iter->second->find(dest_processor_id);
      if (sub_iter != iter->second->end()) {
        return sub_iter->second;
      }
    }

    return nullptr;
  }

  // Cleanup all receivers and close them properly
  void CleanupAllReceivers() {
    std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
    for (k_int32 i = 0; i < kBucketNum; ++i) {
      recvrs.clear();
      {
        std::lock_guard<Mutex> l(lock_[i]);
        for (auto& iter : receiver_map_[i]) {
          for (auto& sub_iter : *(iter.second)) {
            recvrs.push_back(sub_iter.second);
          }
        }
      }

      for (auto& recvr : recvrs) {
        CloseRecvrStream(recvr);
      }
    }
  }

  std::shared_ptr<DataStreamRecvr> RemoveRecvr(const KQueryId& query_id,
                                               KProcessorId dest_processor_id) {
    std::shared_ptr<DataStreamRecvr> target_recvr;
    k_uint32 bucket = GetBucket(query_id);
    auto& receiver_map = receiver_map_[bucket];
    {
      std::lock_guard<Mutex> lock(lock_[bucket]);
      auto iter = receiver_map.find(query_id);
      auto sub_iter = iter->second->find(dest_processor_id);
      if (iter != receiver_map.end()) {
        if (sub_iter != iter->second->end()) {
          target_recvr = sub_iter->second;
          iter->second->erase(sub_iter);
          --receiver_count_;

          if (iter->second->empty()) {
            receiver_map.erase(iter);
            --processor_count_;
          }
        }
      }
    }

    return target_recvr;
  }

  void CancelRecvrStream(const std::shared_ptr<DataStreamRecvr>& recvr);
  void CloseRecvrStream(const std::shared_ptr<DataStreamRecvr>& recvr);
};

}  // namespace kwdbts
