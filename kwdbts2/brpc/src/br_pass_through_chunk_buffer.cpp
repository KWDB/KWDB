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

#include "br_pass_through_chunk_buffer.h"

namespace kwdbts {

// Channel for each sender ID.
class PassThroughSenderChannel {
 public:
  PassThroughSenderChannel() {}
  ~PassThroughSenderChannel() {}

  // Appends a chunk to the buffer.
  void AppendChunk(DataChunkPtr& chunk, k_size_t chunk_size, k_int32 driver_sequence) {
    std::unique_lock lock(mutex_);
    buffer_.emplace_back(std::make_pair(std::move(chunk), driver_sequence));
    bytes_.push_back(chunk_size);
  }

  // Pulls chunks from the buffer.
  void PullChunks(ChunkUniquePtrVector* chunks, std::vector<k_size_t>* bytes) {
    std::unique_lock lock(mutex_);
    chunks->swap(buffer_);
    bytes->swap(bytes_);
  }

 private:
  std::mutex mutex_;  // Lock for push/pull operations.
  ChunkUniquePtrVector buffer_;
  std::vector<k_size_t> bytes_;
};

// Channel for each [query_id, processor_id] pair.
class PassThroughChannel {
 public:
  PassThroughSenderChannel* GetOrCreateSenderChannel(k_int32 sender_id) {
    std::unique_lock lock(mutex_);
    auto it = sender_id_to_channel_.find(sender_id);
    if (it == sender_id_to_channel_.end()) {
      auto* channel = new PassThroughSenderChannel();
      sender_id_to_channel_.emplace(sender_id, channel);
      return channel;
    }
    return it->second;
  }

  ~PassThroughChannel() {
    for (auto& it : sender_id_to_channel_) {
      delete it.second;
    }
    sender_id_to_channel_.clear();
  }

 private:
  std::mutex mutex_;
  std::unordered_map<k_int32, PassThroughSenderChannel*> sender_id_to_channel_;
};

PassThroughChunkBuffer::PassThroughChunkBuffer(const KQueryId& query_id)
    : mutex_(), query_id_(query_id), ref_count_(1) {}

PassThroughChunkBuffer::~PassThroughChunkBuffer() {
  if (UNLIKELY(ref_count_ != 0)) {
    LOG_ERROR(
        "PassThroughChunkBuffer reference leak detected! query_id=%ld, "
        "ref_count=%d",
        query_id_, ref_count_);
  }
  for (auto& it : key_to_channel_) {
    delete it.second;
  }
  key_to_channel_.clear();
}

PassThroughChannel* PassThroughChunkBuffer::GetOrCreateChannel(const Key& key) {
  std::unique_lock lock(mutex_);
  auto it = key_to_channel_.find(key);
  if (it == key_to_channel_.end()) {
    auto* channel = new PassThroughChannel();
    key_to_channel_.emplace(key, channel);
    return channel;
  }
  return it->second;
}

void PassThroughContext::Init() {
  channel_ =
      chunk_buffer_->GetOrCreateChannel(PassThroughChunkBuffer::Key(query_id_, processor_id_));
}

void PassThroughContext::AppendChunk(k_int32 sender_id, DataChunkPtr& chunk, k_size_t chunk_size,
                                     k_int32 driver_sequence) {
  PassThroughSenderChannel* sender_channel = channel_->GetOrCreateSenderChannel(sender_id);
  sender_channel->AppendChunk(chunk, chunk_size, driver_sequence);
}

void PassThroughContext::PullChunks(k_int32 sender_id, ChunkUniquePtrVector* chunks,
                                    std::vector<k_size_t>* bytes) {
  PassThroughSenderChannel* sender_channel = channel_->GetOrCreateSenderChannel(sender_id);
  sender_channel->PullChunks(chunks, bytes);
}

KStatus PassThroughChunkBufferManager::OpenQueryInstance(const KQueryId& query_id) {
  std::unique_lock lock(mutex_);
  auto it = query_id_to_buffer_.find(query_id);
  if (it == query_id_to_buffer_.end()) {
    auto* buffer = new PassThroughChunkBuffer(query_id);
    if (buffer == nullptr) {
      LOG_ERROR(
          "PassThroughChunkBufferManager::OpenQueryInstance, query_id = %ld, "
          "new PassThroughChunkBuffer failed",
          query_id);
      return KStatus::FAIL;
    }
    query_id_to_buffer_.emplace(query_id, buffer);
  } else {
    it->second->Ref();
  }
  return KStatus::SUCCESS;
}

void PassThroughChunkBufferManager::Close() {
  std::unique_lock lock(mutex_);
  for (auto it = query_id_to_buffer_.begin(); it != query_id_to_buffer_.end();) {
    delete it->second;
    it = query_id_to_buffer_.erase(it);
  }
}

KStatus PassThroughChunkBufferManager::CloseQueryInstance(const KQueryId& query_id) {
  std::unique_lock lock(mutex_);
  auto it = query_id_to_buffer_.find(query_id);
  if (it != query_id_to_buffer_.end()) {
    k_int32 rc = it->second->Unref();
    if (rc == 0) {
      delete it->second;
      query_id_to_buffer_.erase(it);
    }
  }
  return KStatus::SUCCESS;
}

PassThroughChunkBuffer* PassThroughChunkBufferManager::Get(const KQueryId& query_id) {
  std::unique_lock lock(mutex_);
  auto it = query_id_to_buffer_.find(query_id);
  if (it == query_id_to_buffer_.end()) {
    return nullptr;
  }
  return it->second;
}

}  // namespace kwdbts
