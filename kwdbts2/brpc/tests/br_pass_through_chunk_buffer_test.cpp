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

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "ee_data_chunk.h"

namespace kwdbts {

namespace {

// Creates a minimal DataChunk instance used as test payload.
DataChunkPtr CreateSimpleChunk() { return std::make_unique<DataChunk>(); }

class ScopedQueryInstance {
 public:
  ScopedQueryInstance(PassThroughChunkBufferManager* manager, KQueryId query_id)
      : manager_(manager), query_id_(query_id) {}

  void Activate() { active_ = true; }

  ~ScopedQueryInstance() {
    if (active_) {
      manager_->CloseQueryInstance(query_id_);
    }
  }

 private:
  PassThroughChunkBufferManager* manager_;
  KQueryId query_id_;
  bool active_ = false;
};

}  // namespace

// Verifies that AppendChunk stores chunks and PullChunks atomically transfers
// ownership of all buffered chunks and their byte sizes in FIFO order.
// A subsequent PullChunks call on an empty channel must return empty vectors.
TEST(BrPassThroughChunkBufferTest, SenderChannelSwapsBufferedChunks) {
  PassThroughSenderChannel sender_channel;

  auto first_chunk = CreateSimpleChunk();
  auto second_chunk = CreateSimpleChunk();
  // Append two chunks with distinct sizes and driver sequence numbers.
  sender_channel.AppendChunk(first_chunk, 32, 11);
  sender_channel.AppendChunk(second_chunk, 64, 22);

  ChunkUniquePtrVector chunks;
  std::vector<k_size_t> bytes;
  // Pull all buffered chunks; internal buffers should be swapped out.
  sender_channel.PullChunks(&chunks, &bytes);

  ASSERT_EQ(chunks.size(), 2u);
  ASSERT_EQ(bytes.size(), 2u);
  EXPECT_EQ(bytes[0], 32u);
  EXPECT_EQ(bytes[1], 64u);
  EXPECT_EQ(chunks[0].second, 11);
  EXPECT_EQ(chunks[1].second, 22);
  EXPECT_NE(chunks[0].first, nullptr);
  EXPECT_NE(chunks[1].first, nullptr);

  chunks.clear();
  bytes.clear();
  // Second pull must yield nothing because the channel is now empty.
  sender_channel.PullChunks(&chunks, &bytes);
  EXPECT_TRUE(chunks.empty());
  EXPECT_TRUE(bytes.empty());
}

// Verifies that:
//   1. GetOrCreateChannel returns the same channel object for the same key
//      and a different object for a different key.
//   2. Chunks appended through one PassThroughContext are only visible when
//      pulling from that same context (keyed by processor_id and sender_id),
//      ensuring strict per-key and per-sender isolation.
TEST(BrPassThroughChunkBufferTest, ChannelAndContextAreIsolatedByKeyAndSender) {
  PassThroughChunkBuffer buffer(88);

  PassThroughChunkBuffer::Key first_key(88, 1);
  PassThroughChunkBuffer::Key second_key(88, 2);
  // Same key must resolve to the same channel instance.
  PassThroughChannel* first_channel = buffer.GetOrCreateChannel(first_key);
  EXPECT_EQ(first_channel, buffer.GetOrCreateChannel(first_key));
  // Different key must yield a distinct channel instance.
  EXPECT_NE(first_channel, buffer.GetOrCreateChannel(second_key));

  PassThroughContext first_context(&buffer, 88, 1);
  PassThroughContext second_context(&buffer, 88, 2);
  first_context.Init();
  second_context.Init();

  auto first_chunk = CreateSimpleChunk();
  auto second_chunk = CreateSimpleChunk();
  auto other_sender_chunk = CreateSimpleChunk();

  // Append chunks through different contexts and senders.
  first_context.AppendChunk(7, first_chunk, 101, 1);
  second_context.AppendChunk(7, second_chunk, 202, 2);
  first_context.AppendChunk(8, other_sender_chunk, 303, 3);

  ChunkUniquePtrVector chunks;
  std::vector<k_size_t> bytes;

  // first_context sender 7 should only see its own chunk (size=101, seq=1).
  first_context.PullChunks(7, &chunks, &bytes);
  ASSERT_EQ(chunks.size(), 1u);
  ASSERT_EQ(bytes.size(), 1u);
  EXPECT_EQ(bytes[0], 101u);
  EXPECT_EQ(chunks[0].second, 1);

  chunks.clear();
  bytes.clear();
  // second_context sender 7 should only see its own chunk (size=202, seq=2).
  second_context.PullChunks(7, &chunks, &bytes);
  ASSERT_EQ(chunks.size(), 1u);
  ASSERT_EQ(bytes.size(), 1u);
  EXPECT_EQ(bytes[0], 202u);
  EXPECT_EQ(chunks[0].second, 2);

  chunks.clear();
  bytes.clear();
  // first_context sender 8 should only see its own chunk (size=303, seq=3).
  first_context.PullChunks(8, &chunks, &bytes);
  ASSERT_EQ(chunks.size(), 1u);
  ASSERT_EQ(bytes.size(), 1u);
  EXPECT_EQ(bytes[0], 303u);
  EXPECT_EQ(chunks[0].second, 3);

  EXPECT_EQ(buffer.Unref(), 0);
}

// Verifies reference counting semantics on PassThroughChunkBuffer and the full
// lifecycle of PassThroughChunkBufferManager:
//   - Ref/Unref adjust the count correctly.
//   - OpenQueryInstance is idempotent; each open increments the ref count.
//   - CloseQueryInstance on an unknown query_id succeeds without side effects.
//   - The buffer is removed only when the ref count reaches zero.
//   - Close() flushes all remaining buffers regardless of ref counts.
TEST(BrPassThroughChunkBufferTest, ReferenceCountingAndManagerLifecycleAreStable) {
  PassThroughChunkBuffer buffer(12345);
  // Initial ref count is 1; Ref() should return 2.
  EXPECT_EQ(buffer.Ref(), 2);
  // Unref() should return 1 (buffer still alive).
  EXPECT_EQ(buffer.Unref(), 1);

  PassThroughChunkBufferManager manager;
  // First open creates the buffer; Get() must return non-null.
  EXPECT_EQ(manager.OpenQueryInstance(12345), KStatus::SUCCESS);
  EXPECT_NE(manager.Get(12345), nullptr);
  // Second open on the same query_id increments the ref count.
  EXPECT_EQ(manager.OpenQueryInstance(12345), KStatus::SUCCESS);
  // Closing an unknown query_id must succeed gracefully.
  EXPECT_EQ(manager.CloseQueryInstance(99999), KStatus::SUCCESS);
  // First close decrements ref count; buffer must still exist.
  EXPECT_EQ(manager.CloseQueryInstance(12345), KStatus::SUCCESS);
  EXPECT_NE(manager.Get(12345), nullptr);
  // Second close drops ref count to zero; buffer must be removed.
  EXPECT_EQ(manager.CloseQueryInstance(12345), KStatus::SUCCESS);
  EXPECT_EQ(manager.Get(12345), nullptr);

  // Open two independent queries, close each, then call Close() to flush all.
  EXPECT_EQ(manager.OpenQueryInstance(1), KStatus::SUCCESS);
  EXPECT_EQ(manager.OpenQueryInstance(2), KStatus::SUCCESS);
  EXPECT_EQ(manager.CloseQueryInstance(1), KStatus::SUCCESS);
  EXPECT_EQ(manager.CloseQueryInstance(2), KStatus::SUCCESS);
  manager.Close();
  EXPECT_EQ(manager.Get(1), nullptr);
  EXPECT_EQ(manager.Get(2), nullptr);

  EXPECT_EQ(buffer.Unref(), 0);
}

// Verifies that concurrent writes from multiple threads do not lose any chunks.
//
// Each thread appends kChunkPerThread chunks through its own PassThroughContext.
// After all threads complete, sequential reads must recover exactly
// kChunkPerThread chunks per sender with correct byte sizes and sequence numbers.
TEST(BrPassThroughChunkBufferTest, ConcurrentContextsDoNotLoseChunks) {
  PassThroughChunkBufferManager manager;
  ScopedQueryInstance query_instance(&manager, 77);
  ASSERT_EQ(manager.OpenQueryInstance(77), KStatus::SUCCESS);
  query_instance.Activate();
  PassThroughChunkBuffer* buffer = manager.Get(77);
  ASSERT_NE(buffer, nullptr);

  constexpr int kThreadCount = 4;
  constexpr int kChunkPerThread = 8;
  std::vector<std::thread> threads;
  // Launch kThreadCount threads; each appends kChunkPerThread chunks.
  for (int i = 0; i < kThreadCount; ++i) {
    threads.emplace_back([buffer, i]() {
      PassThroughContext context(buffer, 77, 100 + i);
      context.Init();
      for (int j = 0; j < kChunkPerThread; ++j) {
        auto chunk = CreateSimpleChunk();
        context.AppendChunk(i, chunk, static_cast<k_size_t>(i * 100 + j), j);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify each sender's chunks were stored completely and in order.
  for (int i = 0; i < kThreadCount; ++i) {
    PassThroughContext context(buffer, 77, 100 + i);
    context.Init();
    ChunkUniquePtrVector chunks;
    std::vector<k_size_t> bytes;
    context.PullChunks(i, &chunks, &bytes);
    ASSERT_EQ(chunks.size(), static_cast<size_t>(kChunkPerThread));
    ASSERT_EQ(bytes.size(), static_cast<size_t>(kChunkPerThread));
    for (int j = 0; j < kChunkPerThread; ++j) {
      EXPECT_EQ(chunks[j].second, j);
      EXPECT_EQ(bytes[j], static_cast<k_size_t>(i * 100 + j));
    }
  }
}

}  // namespace kwdbts
