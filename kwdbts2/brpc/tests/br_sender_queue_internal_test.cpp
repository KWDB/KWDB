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

#include <google/protobuf/stubs/callback.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

// Expose private/protected members for white-box testing.
#define private public
#define protected public
#include "br_data_stream_mgr.h"
#include "br_data_stream_recvr.h"
#include "br_sender_queue.h"
#undef protected
#undef private
#include "ee_mempool.h"
#include "ee_protobuf_serde.h"

namespace kwdbts {

namespace {

// A protobuf Closure that counts how many times Run() has been invoked.
// Used to verify that completion callbacks are triggered exactly once.
class CountingClosure : public google::protobuf::Closure {
 public:
  // Increments the run counter each time the closure is executed.
  void Run() override { ++run_count; }

  // Number of times Run() has been called. Accessed from multiple threads.
  std::atomic<int> run_count{0};
};

// Creates an empty DataChunk wrapped in a unique_ptr.
DataChunkPtr CreateChunk() { return std::make_unique<DataChunk>(); }

// Builds a minimal ChunkPB with one INT column and the given payload string.
// The returned protobuf is suitable for testing serialization-independent paths
// (e.g. pass-through or pre-built metadata tests).
ChunkPB MakeChunkPb(const std::string& data = "payload") {
  ChunkPB chunk;
  chunk.set_data(data);
  chunk.set_data_size(data.size());
  chunk.set_serialized_size(data.size());
  chunk.set_is_encoding(false);
  chunk.set_compress_type(NO_COMPRESSION);
  auto* info = chunk.add_column_info();
  info->set_storage_len(4);
  info->set_fixed_storage_len(4);
  info->set_storage_type(static_cast<int>(roachpb::DataType::INT));
  info->set_return_type(static_cast<int>(KWDBTypeFamily::IntFamily));
  info->set_is_string(static_cast<int>(KWStringType::NON_STRING));
  info->set_allow_null(true);
  return chunk;
}

// Constructs a serialized (non-pass-through) PTransmitChunkParams.
//
// @param query_id       Identifies the query this chunk belongs to.
// @param processor_id   Destination processor for routing.
// @param sender_id      Logical sender index.
// @param be_number      Backend node number of the sender.
// @param sequence       Sequence number for ordered delivery.
// @param data           Raw payload string embedded in the ChunkPB.
// @return               A fully populated PTransmitChunkParams.
PTransmitChunkParams MakeSerializedRequest(KQueryId query_id, KProcessorId processor_id,
                                           k_int32 sender_id, k_int32 be_number, k_int64 sequence,
                                           const std::string& data = "payload") {
  PTransmitChunkParams request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  request.set_sender_id(sender_id);
  request.set_be_number(be_number);
  request.set_sequence(sequence);
  request.set_use_pass_through(false);
  request.add_chunks()->CopyFrom(MakeChunkPb(data));
  return request;
}

// Constructs a pass-through PTransmitChunkParams (no embedded chunk data).
//
// @param query_id       Identifies the query this request belongs to.
// @param processor_id   Destination processor for routing.
// @param sender_id      Logical sender index.
// @param be_number      Backend node number of the sender.
// @param sequence       Sequence number for ordered delivery.
// @return               A PTransmitChunkParams with use_pass_through=true.
PTransmitChunkParams MakePassThroughRequest(KQueryId query_id, KProcessorId processor_id,
                                            k_int32 sender_id, k_int32 be_number,
                                            k_int64 sequence) {
  PTransmitChunkParams request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  request.set_sender_id(sender_id);
  request.set_be_number(be_number);
  request.set_sequence(sequence);
  request.set_use_pass_through(true);
  return request;
}

// RAII guard that initializes the global row buffer pool on construction and
// releases it on destruction. Ensures that tests requiring memory pool
// allocation do not interfere with each other or leave global state dirty.
class ScopedBufferPool {
 public:
  // Initializes the global buffer pool if it has not been created yet.
  ScopedBufferPool() {
    if (g_pstBufferPoolInfo == nullptr) {
      g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
      owned_ = true;
    }
  }

  // Releases the global buffer pool if this instance owns it.
  ~ScopedBufferPool() {
    if (owned_) {
      EXPECT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
      g_pstBufferPoolInfo = nullptr;
    }
  }

 private:
  // True if this guard created the pool and is responsible for cleanup.
  bool owned_ = false;
};

// Builds a fully serialized ChunkPB containing one BIGINT column with value 42.
// The result is ready for deserialization through ProtobufChunkSerrialde and
// can be used to exercise ordered-delivery code paths.
ChunkPB MakeSerializableChunkPb() {
  ScopedBufferPool buffer_pool;
  ColumnInfo col_info[1];
  col_info[0] = ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);

  auto chunk = std::make_unique<DataChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  const k_int64 value = 42;
  chunk->AddCount();
  EXPECT_EQ(
      chunk->InsertData(0, 0, reinterpret_cast<char*>(const_cast<k_int64*>(&value)), sizeof(value)),
      KStatus::SUCCESS);

  k_bool is_first_chunk = true;
  ProtobufChunkSerrialde serde;
  ChunkPB serialized = serde.SerializeChunk(chunk.get(), &is_first_chunk);
  serialized.set_compress_type(NO_COMPRESSION);
  serialized.set_is_encoding(false);
  serialized.set_data_size(serialized.data().size());
  // Guard against zero serialized_size to avoid downstream validation failures.
  if (serialized.serialized_size() == 0) {
    serialized.set_serialized_size(serialized.data().size());
  }
  return serialized;
}

}  // namespace

// Verifies that ChunkQueue supports move construction and move assignment.
//
// After a move, the source queue must be empty and the destination must
// contain the transferred items. TryPop should drain the queue correctly.
TEST(BrSenderQueueInternalTest, ChunkQueueSupportsMoveConstructionAndAssignment) {
  using PipelineQueue = DataStreamRecvr::PipelineSenderQueue;
  using ChunkItem = PipelineQueue::ChunkItem;
  using ChunkQueue = PipelineQueue::ChunkQueue;

  ChunkQueue first;
  EXPECT_TRUE(first.IsEmpty());
  EXPECT_TRUE(first.Push(ChunkItem(12, 7, nullptr, CreateChunk())));
  EXPECT_EQ(first.Size(), 1u);

  ChunkQueue moved(std::move(first));
  EXPECT_EQ(first.Size(), 0u);
  EXPECT_EQ(moved.Size(), 1u);

  ChunkQueue assigned;
  assigned = std::move(moved);
  EXPECT_EQ(moved.Size(), 0u);
  EXPECT_EQ(assigned.Size(), 1u);

  ChunkItem item;
  EXPECT_TRUE(assigned.TryPop(item));
  EXPECT_TRUE(assigned.IsEmpty());
  EXPECT_FALSE(assigned.TryPop(item));
}

// Verifies HasOutput() and HasChunk() reflect queue state transitions:
// blocked closures, buffered bytes, threshold-triggered unplugging, and
// cancellation. Also confirms that Cancel() marks the queue as finished.
TEST(BrSenderQueueInternalTest, HasOutputTracksBufferedAndCancelledStates) {
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3001), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3001, 31, 1, 10, false, false);
  ASSERT_NE(recvr, nullptr);

  auto* queue = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr->sender_queues_[0]);
  EXPECT_FALSE(queue->HasChunk());
  EXPECT_FALSE(queue->HasOutput(0));

  queue->chunk_queue_states_[0].blocked_closure_num = 1;
  EXPECT_TRUE(queue->HasOutput(0));
  queue->chunk_queue_states_[0].blocked_closure_num = 0;

  queue->chunk_queues_[0].Push(
      DataStreamRecvr::PipelineSenderQueue::ChunkItem(4, 1, nullptr, CreateChunk()));
  queue->total_chunks_ = 1;
  recvr->num_buffered_bytes_ = 100;
  EXPECT_TRUE(queue->HasOutput(0));
  recvr->num_buffered_bytes_ = 0;

  for (int i = 0; i < DataStreamRecvr::PipelineSenderQueue::kUnplugBufferThreshold; ++i) {
    queue->chunk_queues_[0].Push(
        DataStreamRecvr::PipelineSenderQueue::ChunkItem(1, i, nullptr, CreateChunk()));
    ++queue->total_chunks_;
  }
  EXPECT_TRUE(queue->HasOutput(0));
  EXPECT_TRUE(queue->chunk_queue_states_[0].unpluging);

  DataStreamRecvr::PipelineSenderQueue::ChunkItem item;
  while (queue->chunk_queues_[0].TryPop(item)) {
    --queue->total_chunks_;
  }
  EXPECT_FALSE(queue->HasOutput(0));

  queue->Cancel();
  EXPECT_TRUE(queue->HasChunk());
  EXPECT_FALSE(queue->HasOutput(0));
  EXPECT_TRUE(queue->IsFinished());

  recvr->closed_ = true;
  recvr->mgr_ = nullptr;
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3001), KStatus::SUCCESS);
}

// Verifies that CleanBufferQueues() drains both the active chunk queue and
// the buffered chunk queues, invokes all pending done-closures exactly once,
// and resets counters (blocked_closure_num, total_chunks, num_buffered_bytes).
TEST(BrSenderQueueInternalTest, CleanBufferQueuesRunsClosuresAndClearsState) {
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3002), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3002, 32, 1, 128, false, false);
  ASSERT_NE(recvr, nullptr);

  auto* queue = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr->sender_queues_[0]);
  CountingClosure queued_done;
  CountingClosure buffered_done;

  queue->chunk_queues_[0].Push(
      DataStreamRecvr::PipelineSenderQueue::ChunkItem(16, 1, &queued_done, CreateChunk()));
  queue->chunk_queue_states_[0].blocked_closure_num = 1;
  queue->total_chunks_ = 1;
  recvr->num_buffered_bytes_ = 16;
  queue->buffered_chunk_queues_[9][2].emplace_back(
      DataStreamRecvr::PipelineSenderQueue::ChunkItem(8, 2, &buffered_done, CreateChunk()));

  queue->CleanBufferQueues();

  EXPECT_EQ(queued_done.run_count.load(), 1);
  EXPECT_EQ(buffered_done.run_count.load(), 1);
  EXPECT_EQ(queue->chunk_queue_states_[0].blocked_closure_num.load(), 0);
  EXPECT_EQ(queue->total_chunks_.load(), 0);
  EXPECT_EQ(recvr->num_buffered_bytes_.load(), 0);
  EXPECT_TRUE(queue->chunk_queues_[0].IsEmpty());
  EXPECT_TRUE(queue->buffered_chunk_queues_[9][2].empty());

  recvr->closed_ = true;
  recvr->mgr_ = nullptr;
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3002), KStatus::SUCCESS);
}

// Verifies AddChunks() error handling:
//   - Returns INTERNAL_ERROR when the request contains no chunks.
//   - Returns OK (and marks HasChunk) when the queue is already cancelled.
//   - Returns OK when all senders have sent EOS (num_remaining_senders == 0).
TEST(BrSenderQueueInternalTest, AddChunksRejectsEmptyRequestAndHandlesCancelledOrEndedQueue) {
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3010), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3010, 40, 1, 8, false, false);
  ASSERT_NE(recvr, nullptr);

  auto* queue = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr->sender_queues_[0]);
  queue->chunk_queue_states_[0].blocked_closure_num = 0;

  PTransmitChunkParams empty_request;
  empty_request.set_query_id(3010);
  empty_request.set_dest_processor(40);
  empty_request.set_sender_id(1);
  empty_request.set_be_number(2);
  empty_request.set_use_pass_through(false);
  google::protobuf::Closure* done = nullptr;
  EXPECT_EQ(queue->AddChunks(empty_request, &done).code(), BRStatusCode::INTERNAL_ERROR);

  PTransmitChunkParams chunk_request = MakeSerializedRequest(3010, 40, 1, 2, 0);
  queue->Cancel();
  EXPECT_EQ(queue->AddChunks(chunk_request, &done).code(), BRStatusCode::OK);
  EXPECT_TRUE(queue->HasChunk());
  EXPECT_FALSE(queue->HasOutput(0));

  auto recvr2 = manager.CreateRecvr(3010, 41, 1, 8, false, false);
  ASSERT_NE(recvr2, nullptr);
  auto* queue2 = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr2->sender_queues_[0]);
  queue2->chunk_queue_states_[0].blocked_closure_num = 0;
  queue2->DecrementSenders(3);
  queue2->DecrementSenders(3);
  EXPECT_EQ(queue2->num_remaining_senders_.load(), 0);
  EXPECT_EQ(queue2->sender_eos_set_.size(), 1u);
  EXPECT_TRUE(queue2->HasChunk());
  EXPECT_EQ(queue2->AddChunks(chunk_request, &done).code(), BRStatusCode::OK);

  queue2->chunk_queues_[0].Push(
      DataStreamRecvr::PipelineSenderQueue::ChunkItem(4, 0, nullptr, CreateChunk()));
  queue2->total_chunks_ = 1;
  EXPECT_TRUE(queue2->HasChunk());

  recvr->closed_ = true;
  recvr->mgr_ = nullptr;
  recvr2->closed_ = true;
  recvr2->mgr_ = nullptr;
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3010), KStatus::SUCCESS);
}

// Covers failure paths in BuildChunkMeta() and GetChunk():
//   - BuildChunkMeta() succeeds and is idempotent after col_info is cached.
//   - DeserializeChunk() fails for unsupported or unknown compression types.
//   - GetChunk() fails and runs the pending done-closure when deserialization
//     fails, resetting all associated counters.
TEST(BrSenderQueueInternalTest, BuildChunkMetaAndGetChunkFailurePathsAreCovered) {
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3011), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3011, 42, 1, 4, false, false);
  ASSERT_NE(recvr, nullptr);

  auto* queue = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr->sender_queues_[0]);
  queue->chunk_queue_states_[0].blocked_closure_num = 0;

  ChunkPB chunk_pb = MakeChunkPb("abc");
  EXPECT_EQ(queue->BuildChunkMeta(chunk_pb), KStatus::SUCCESS);
  EXPECT_TRUE(queue->col_info_initialized_.load());
  EXPECT_EQ(queue->col_num_, 1);
  EXPECT_EQ(queue->BuildChunkMeta(chunk_pb), KStatus::SUCCESS);

  ChunkPB bad_version_pb;
  bad_version_pb.set_compress_type(SNAPPY_COMPRESSION);
  bad_version_pb.set_data(std::string(4, '\0'));
  DataChunkPtr deserialized;
  EXPECT_EQ(queue->DeserializeChunk(bad_version_pb, deserialized), KStatus::FAIL);

  ChunkPB invalid_compress_pb;
  invalid_compress_pb.set_compress_type(UNKNOWN_COMPRESSION);
  invalid_compress_pb.set_data(std::string(24, '\0'));
  EXPECT_EQ(queue->DeserializeChunk(invalid_compress_pb, deserialized), KStatus::FAIL);

  CountingClosure done;
  queue->chunk_queues_[0].Push(
      DataStreamRecvr::PipelineSenderQueue::ChunkItem(3, 0, &done, MakeChunkPb("abc")));
  queue->chunk_queue_states_[0].blocked_closure_num = 1;
  queue->total_chunks_ = 1;
  recvr->num_buffered_bytes_ = 3;

  DataChunk* chunk = nullptr;
  EXPECT_EQ(queue->GetChunk(&chunk, 0), KStatus::FAIL);
  EXPECT_EQ(chunk, nullptr);
  EXPECT_EQ(done.run_count.load(), 1);
  EXPECT_EQ(queue->chunk_queue_states_[0].blocked_closure_num.load(), 0);
  EXPECT_EQ(queue->total_chunks_.load(), 0);
  EXPECT_EQ(recvr->num_buffered_bytes_.load(), 0);

  recvr->closed_ = true;
  recvr->mgr_ = nullptr;
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3011), KStatus::SUCCESS);
}

// Covers two AddChunks paths:
//   1. Non-ordered (AddChunks): chunks are enqueued directly; GetChunk fails
//      because deserialization is unsupported for the test payload, but the
//      done-closure is still invoked and counters are reset.
//   2. Ordered (AddChunksAndKeepOrder): out-of-order chunks are buffered by
//      sequence number and flushed when the earlier sequence arrives, then
//      GetChunk succeeds for each buffered chunk in order.
TEST(BrSenderQueueInternalTest, AddChunksCoversSerializedAndKeepOrderPaths) {
  ScopedBufferPool buffer_pool;
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3012), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3012, 43, 1, 1, false, false);
  ASSERT_NE(recvr, nullptr);
  auto* queue = static_cast<DataStreamRecvr::PipelineSenderQueue*>(recvr->sender_queues_[0]);
  queue->chunk_queue_states_[0].blocked_closure_num = 0;

  PTransmitChunkParams non_ordered_request = MakeSerializedRequest(3012, 43, 1, 1, 0, "abcdefgh");
  CountingClosure non_ordered_done;
  google::protobuf::Closure* non_ordered_done_ptr = &non_ordered_done;
  EXPECT_EQ(queue->AddChunks(non_ordered_request, &non_ordered_done_ptr).code(), BRStatusCode::OK);
  EXPECT_EQ(non_ordered_done_ptr, nullptr);
  EXPECT_EQ(queue->total_chunks_.load(), 1);
  EXPECT_EQ(queue->chunk_queue_states_[0].blocked_closure_num.load(), 1);

  DataChunk* chunk = nullptr;
  EXPECT_EQ(queue->GetChunk(&chunk, 0), KStatus::FAIL);
  EXPECT_EQ(chunk, nullptr);
  EXPECT_EQ(non_ordered_done.run_count.load(), 1);
  EXPECT_EQ(queue->total_chunks_.load(), 0);

  auto ordered_recvr = manager.CreateRecvr(3012, 44, 1, 1, false, true);
  ASSERT_NE(ordered_recvr, nullptr);
  auto* ordered_queue =
      static_cast<DataStreamRecvr::PipelineSenderQueue*>(ordered_recvr->sender_queues_[0]);
  ordered_queue->chunk_queue_states_[0].blocked_closure_num = 0;

  PTransmitChunkParams seq1_request = MakePassThroughRequest(3012, 44, 2, 5, 1);
  seq1_request.set_use_pass_through(false);
  seq1_request.add_chunks()->CopyFrom(MakeSerializableChunkPb());
  CountingClosure seq1_done;
  google::protobuf::Closure* seq1_done_ptr = &seq1_done;
  EXPECT_EQ(ordered_queue->AddChunksAndKeepOrder(seq1_request, &seq1_done_ptr).code(),
            BRStatusCode::OK);
  EXPECT_EQ(seq1_done_ptr, nullptr);
  EXPECT_EQ(ordered_queue->total_chunks_.load(), 0);
  EXPECT_EQ(ordered_queue->buffered_chunk_queues_[5].size(), 1u);

  PTransmitChunkParams seq0_request = MakePassThroughRequest(3012, 44, 2, 5, 0);
  seq0_request.set_use_pass_through(false);
  seq0_request.add_chunks()->CopyFrom(MakeSerializableChunkPb());
  CountingClosure seq0_done;
  google::protobuf::Closure* seq0_done_ptr = &seq0_done;
  EXPECT_EQ(ordered_queue->AddChunksAndKeepOrder(seq0_request, &seq0_done_ptr).code(),
            BRStatusCode::OK);
  EXPECT_EQ(seq0_done_ptr, nullptr);
  EXPECT_EQ(ordered_queue->buffered_chunk_queues_[5].size(), 0u);
  EXPECT_EQ(ordered_queue->total_chunks_.load(), 2);
  EXPECT_EQ(ordered_queue->chunk_queue_states_[0].blocked_closure_num.load(), 2);

  EXPECT_EQ(ordered_queue->GetChunk(&chunk, 0), KStatus::SUCCESS);
  std::unique_ptr<DataChunk> first_chunk(chunk);
  EXPECT_NE(first_chunk, nullptr);
  EXPECT_EQ(ordered_queue->GetChunk(&chunk, 0), KStatus::SUCCESS);
  std::unique_ptr<DataChunk> second_chunk(chunk);
  EXPECT_NE(second_chunk, nullptr);
  EXPECT_EQ(seq0_done.run_count.load(), 1);
  EXPECT_EQ(seq1_done.run_count.load(), 1);
  EXPECT_EQ(ordered_queue->chunk_queue_states_[0].blocked_closure_num.load(), 0);

  recvr->closed_ = true;
  recvr->mgr_ = nullptr;
  ordered_recvr->closed_ = true;
  ordered_recvr->mgr_ = nullptr;
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3012), KStatus::SUCCESS);
}

// Verifies merging-receiver behavior:
//   - The first AddChunks call from a new sender assigns it queue index 0 and
//     advances next_queue_index_.
//   - A second call from the same sender reuses the existing mapping.
//   - A sender from a different backend (be_number mismatch) is rejected with
//     INTERNAL_ERROR.
//   - RemoveSender() inserts a mapping for unseen senders.
//   - Close() is idempotent: calling it twice leaves closed_ == true.
TEST(BrSenderQueueInternalTest, MergingReceiverMapsSendersAndCloseIsIdempotent) {
  DataStreamMgr manager;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(3013), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(3013, 45, 1, 16, true, false);
  ASSERT_NE(recvr, nullptr);

  PTransmitChunkParams sender10 = MakePassThroughRequest(3013, 45, 10, 1, 0);
  google::protobuf::Closure* done = nullptr;
  EXPECT_EQ(recvr->AddChunks(sender10, &done).code(), BRStatusCode::OK);
  EXPECT_EQ(recvr->sender_to_queue_map_[10], 0);
  EXPECT_EQ(recvr->next_queue_index_.load(), 1);
  EXPECT_EQ(recvr->GetTotalChunks(10), 0);

  EXPECT_EQ(recvr->AddChunks(sender10, &done).code(), BRStatusCode::OK);
  EXPECT_EQ(recvr->next_queue_index_.load(), 1);

  PTransmitChunkParams sender20 = MakePassThroughRequest(3013, 45, 20, 2, 0);
  EXPECT_EQ(recvr->AddChunks(sender20, &done).code(), BRStatusCode::INTERNAL_ERROR);

  auto recvr2 = manager.CreateRecvr(3013, 46, 1, 16, true, false);
  ASSERT_NE(recvr2, nullptr);
  recvr2->RemoveSender(30, 7);
  EXPECT_EQ(recvr2->sender_to_queue_map_[30], 0);
  EXPECT_TRUE(recvr2->IsDataReady() == false);

  recvr->Close();
  recvr->Close();
  EXPECT_TRUE(recvr->closed_);

  recvr2->Close();
  EXPECT_TRUE(recvr2->closed_);
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(3013), KStatus::SUCCESS);
}

}  // namespace kwdbts
