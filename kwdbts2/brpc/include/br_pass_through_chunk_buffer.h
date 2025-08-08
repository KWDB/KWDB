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
#include <map>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ee_data_chunk.h"
#include "kwdb_type.h"

namespace kwdbts {

using ChunkUniquePtrVector = std::vector<std::pair<DataChunkPtr, k_int32>>;
// To manage pass through chunks between sink/sources in the same process.
class PassThroughChannel;

// Manages a buffer for passing chunks between components in the same process.
class PassThroughChunkBuffer {
 public:
  using Key = std::tuple<KQueryId, KProcessorId>;

  // Hash function for the Key type.
  struct KeyHash {
    k_size_t operator()(const Key& key) const {
      const auto& [query_id, processor_id] = key;
      k_size_t hash = query_id;
      hash ^= processor_id;
      hash = (hash ^ (hash >> 32)) * 0x45d9f3b;
      return hash;
    }
  };

  explicit PassThroughChunkBuffer(const KQueryId& query_id);
  ~PassThroughChunkBuffer();

  // Gets or creates a channel for the given key.
  PassThroughChannel* GetOrCreateChannel(const Key& key);

  // Increases the reference count and returns the new count.
  k_int32 Ref() { return ++ref_count_; }

  // Decreases the reference count and returns the new count.
  k_int32 Unref() {
    ref_count_ -= 1;
    return ref_count_;
  }

 private:
  std::mutex mutex_;
  const KQueryId query_id_;
  std::unordered_map<Key, PassThroughChannel*, KeyHash> key_to_channel_;
  k_int32 ref_count_;
};

// Context for managing pass through operations.
class PassThroughContext {
 public:
  PassThroughContext(PassThroughChunkBuffer* chunk_buffer, const KQueryId& query_id,
                     KProcessorId processor_id)
      : chunk_buffer_(chunk_buffer), query_id_(query_id), processor_id_(processor_id) {}

  void Init();
  void AppendChunk(k_int32 sender_id, DataChunkPtr& chunk, k_size_t chunk_size,
                   k_int32 driver_sequence);
  void PullChunks(k_int32 sender_id, ChunkUniquePtrVector* chunks, std::vector<k_size_t>* bytes);

 private:
  // Holds the chunk buffer to prevent early deallocation.
  PassThroughChunkBuffer* chunk_buffer_ = nullptr;
  KQueryId query_id_;
  KProcessorId processor_id_;
  PassThroughChannel* channel_ = nullptr;
};

// Manages multiple PassThroughChunkBuffer instances.
class PassThroughChunkBufferManager {
 public:
  KStatus OpenQueryInstance(const KQueryId& query_id);
  KStatus CloseQueryInstance(const KQueryId& query_id);
  PassThroughChunkBuffer* Get(const KQueryId& query_id);
  void Close();

 private:
  std::mutex mutex_;
  std::unordered_map<KQueryId, PassThroughChunkBuffer*> query_id_to_buffer_;
};

}  // namespace kwdbts
