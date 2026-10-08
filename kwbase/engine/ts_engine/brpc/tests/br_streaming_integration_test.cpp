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

#include "br_data_stream_mgr.h"
#include "br_data_stream_recvr.h"

namespace kwdbts {

namespace {

// CountingClosure is a simple google::protobuf::Closure implementation that
// increments a counter each time Run() is called. Used to verify that the
// done callback is invoked the expected number of times during chunk delivery.
class CountingClosure : public google::protobuf::Closure {
 public:
  // Increments run_count atomically on each invocation.
  void Run() override { ++run_count; }

  // Atomic counter tracking total number of Run() calls.
  std::atomic<int> run_count{0};
};

// NotifyRecorder captures the arguments of the most recent ReceiveNotify
// callback and counts how many times the callback has been invoked.
// Used to verify that error notifications are dispatched with the correct
// node_id, error code, and message.
struct NotifyRecorder {
  int node_id = 0;
  int code = 0;
  std::string msg;
  int call_count = 0;

  // Returns a ReceiveNotify lambda that records the notification parameters
  // into this struct and increments call_count.
  ReceiveNotify AsCallback() {
    return [this](k_int32 in_node_id, k_int32 in_code, const std::string& in_msg) {
      node_id = in_node_id;
      code = in_code;
      msg = in_msg;
      ++call_count;
    };
  }
};

// Creates an empty DataChunk wrapped in a unique_ptr.
DataChunkPtr CreateChunk() { return std::make_unique<DataChunk>(); }

// Builds a PTransmitChunkParams proto message for sending a data chunk.
//
// @param query_id       Identifies the owning query.
// @param processor_id   Destination processor (receiver) ID.
// @param sender_id      ID of the sender node.
// @param be_number      Backend node number of the sender.
// @param sequence       Monotonically increasing sequence number of this chunk.
// @param eos            If true, signals end-of-stream from this sender.
PTransmitChunkParams MakeTransmitRequest(KQueryId query_id, KProcessorId processor_id,
                                         k_int32 sender_id, k_int32 be_number, k_int64 sequence,
                                         k_bool eos = false) {
  PTransmitChunkParams request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  request.set_sender_id(sender_id);
  request.set_be_number(be_number);
  request.set_sequence(sequence);
  request.set_eos(eos);
  request.set_use_pass_through(true);
  return request;
}

// Builds a PDialDataRecvr proto message used to probe whether a receiver
// is registered for the given query and processor.
PDialDataRecvr MakeDialRequest(KQueryId query_id, KProcessorId processor_id) {
  PDialDataRecvr request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  return request;
}

// Builds a PSendExecStatus proto message representing a failed execution status.
//
// @param query_id      Identifies the owning query.
// @param processor_id  Destination processor (receiver) ID.
// @param status        Execution status code; defaults to EXEC_STATUS_FAILED.
PSendExecStatus MakeExecStatusRequest(KQueryId query_id, KProcessorId processor_id,
                                      ExecStatus status = EXEC_STATUS_FAILED) {
  PSendExecStatus request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  request.set_exec_status(status);
  request.set_error_code(777);
  request.set_error_msg("stream failed");
  return request;
}

}  // namespace

// Verifies that operations on a DataStreamMgr that has no registered receiver
// return the expected status codes, and that pass-through buffer lifecycle
// management (prepare / get / destroy) works correctly.
TEST(BrStreamingIntegrationTest, MissingReceiversReturnExpectedStatusCodes) {
  DataStreamMgr manager;

  // Buffer should not exist before preparation.
  EXPECT_EQ(manager.GetPassThroughChunkBuffer(1000), nullptr);
  EXPECT_EQ(manager.PreparePassThroughChunkBuffer(1000), KStatus::SUCCESS);
  EXPECT_NE(manager.GetPassThroughChunkBuffer(1000), nullptr);

  // Dialing a receiver that was never registered must return NOT_READY.
  BRStatus dial_status = manager.DialDataRecvr(MakeDialRequest(999, 1));
  EXPECT_EQ(dial_status.code(), BRStatusCode::NOT_READY);

  // Transmitting to a non-existent receiver must return NOT_FOUND_RECV.
  PTransmitChunkParams transmit_request = MakeTransmitRequest(999, 1, 7, 8, 0);
  google::protobuf::Closure* done = nullptr;
  BRStatus transmit_status = manager.TransmitChunk(transmit_request, &done);
  EXPECT_EQ(transmit_status.code(), BRStatusCode::NOT_FOUND_RECV);

  // Sending exec status to a non-existent receiver must return NOT_FOUND_RECV.
  BRStatus exec_status = manager.SendExecStatus(MakeExecStatusRequest(999, 1));
  EXPECT_EQ(exec_status.code(), BRStatusCode::NOT_FOUND_RECV);

  // Destroying the buffer must succeed and leave no dangling entry.
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(1000), KStatus::SUCCESS);
  EXPECT_EQ(manager.GetPassThroughChunkBuffer(1000), nullptr);
}

// Verifies that when keep_order mode is enabled the receiver buffers chunks
// that arrive out of sequence and only exposes them to the pipeline after all
// preceding sequence numbers have been delivered. Specifically:
//   - A chunk with sequence 1 is held until sequence 0 arrives.
//   - Once sequence 0 arrives both chunks become visible and are dequeued
//     in order, releasing their respective done callbacks.
//   - An EOS marker finalizes the receiver.
TEST(BrStreamingIntegrationTest, KeepOrderRequestsBufferUntilSequenceGapCloses) {
  DataStreamMgr manager;
  const KQueryId query_id = 2001;
  const KProcessorId processor_id = 21;
  const k_int32 sender_id = 3;
  const k_int32 be_number = 44;

  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(query_id), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(query_id, processor_id, 1, 1, false, true);
  ASSERT_NE(recvr, nullptr);

  auto chunk_seq1 = CreateChunk();
  recvr->GetPassThroughContext().AppendChunk(sender_id, chunk_seq1, 64, 101);
  PTransmitChunkParams seq1_request =
      MakeTransmitRequest(query_id, processor_id, sender_id, be_number, 1);
  CountingClosure seq1_done;
  google::protobuf::Closure* seq1_done_ptr = &seq1_done;
  BRStatus seq1_status = manager.TransmitChunk(seq1_request, &seq1_done_ptr);
  EXPECT_EQ(seq1_status.code(), BRStatusCode::OK);
  EXPECT_EQ(seq1_done_ptr, nullptr);
  EXPECT_EQ(recvr->GetTotalChunks(sender_id), 0);
  EXPECT_FALSE(recvr->HasOutputForPipeline(0));

  auto chunk_seq0 = CreateChunk();
  recvr->GetPassThroughContext().AppendChunk(sender_id, chunk_seq0, 64, 100);
  PTransmitChunkParams seq0_request =
      MakeTransmitRequest(query_id, processor_id, sender_id, be_number, 0);
  CountingClosure seq0_done;
  google::protobuf::Closure* seq0_done_ptr = &seq0_done;
  BRStatus seq0_status = manager.TransmitChunk(seq0_request, &seq0_done_ptr);
  EXPECT_EQ(seq0_status.code(), BRStatusCode::OK);
  EXPECT_EQ(seq0_done_ptr, nullptr);
  EXPECT_EQ(recvr->GetTotalChunks(sender_id), 2);
  EXPECT_TRUE(recvr->HasOutputForPipeline(0));
  EXPECT_FALSE(recvr->IsFinished());

  std::unique_ptr<DataChunk> first;
  std::unique_ptr<DataChunk> second;
  EXPECT_EQ(recvr->GetChunkForPipeline(&first, 0), KStatus::SUCCESS);
  EXPECT_EQ(seq0_done.run_count.load(), 1);
  EXPECT_EQ(seq1_done.run_count.load(), 0);
  EXPECT_EQ(recvr->GetChunkForPipeline(&second, 0), KStatus::SUCCESS);
  EXPECT_EQ(seq1_done.run_count.load(), 1);
  EXPECT_EQ(recvr->GetTotalChunks(sender_id), 0);

  PTransmitChunkParams eos_request =
      MakeTransmitRequest(query_id, processor_id, sender_id, be_number, 2, true);
  google::protobuf::Closure* eos_done = nullptr;
  EXPECT_EQ(manager.TransmitChunk(eos_request, &eos_done).code(), BRStatusCode::OK);
  EXPECT_TRUE(recvr->IsFinished());
  EXPECT_EQ(manager.TransmitChunk(eos_request, &eos_done).code(), BRStatusCode::OK);

  recvr->Close();
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(query_id), KStatus::SUCCESS);
}

// Verifies that a failed execution status triggers the registered ReceiveNotify
// callback with the correct error code and message, and that subsequently
// calling Cancel() marks the receiver as finished and causes
// GetChunkForPipeline() to return KStatus::FAIL.
TEST(BrStreamingIntegrationTest, ErrorNotifyAndCancelPropagateToReceiver) {
  DataStreamMgr manager;
  const KQueryId query_id = 2002;
  const KProcessorId processor_id = 22;

  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(query_id), KStatus::SUCCESS);
  auto recvr = manager.CreateRecvr(query_id, processor_id, 1, 128, false, false);
  ASSERT_NE(recvr, nullptr);

  NotifyRecorder recorder;
  recvr->SetReceiveNotify(recorder.AsCallback());

  BRStatus exec_status = manager.SendExecStatus(MakeExecStatusRequest(query_id, processor_id));
  EXPECT_EQ(exec_status.code(), BRStatusCode::OK);
  EXPECT_EQ(recorder.call_count, 1);
  EXPECT_EQ(recorder.code, 777);
  EXPECT_EQ(recorder.msg, "stream failed");

  manager.Cancel(query_id);
  EXPECT_TRUE(recvr->IsFinished());

  std::unique_ptr<DataChunk> chunk;
  EXPECT_EQ(recvr->GetChunkForPipeline(&chunk, 0), KStatus::FAIL);

  recvr->Close();
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(query_id), KStatus::SUCCESS);
}

// Verifies two independent receiver configurations in a single test:
//
// Short-circuit: After ShortCircuitForPipeline() is called, incoming chunks
// are silently dropped (GetTotalChunks returns 0) and the receiver is
// considered finished after the EOS marker is received.
//
// Merging: A receiver created with is_merging=true correctly tracks chunks
// from multiple distinct senders, maintaining per-sender chunk counts.
TEST(BrStreamingIntegrationTest, ShortCircuitDropsFutureChunksAndMergingMapsSenders) {
  DataStreamMgr manager;

  const KQueryId short_query = 2003;
  const KProcessorId short_processor = 23;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(short_query), KStatus::SUCCESS);
  auto short_recvr = manager.CreateRecvr(short_query, short_processor, 1, 128, false, false);
  ASSERT_NE(short_recvr, nullptr);

  short_recvr->ShortCircuitForPipeline(0);
  auto short_chunk = CreateChunk();
  short_recvr->GetPassThroughContext().AppendChunk(9, short_chunk, 32, 1);
  PTransmitChunkParams short_request = MakeTransmitRequest(short_query, short_processor, 9, 1, 0);
  google::protobuf::Closure* short_done = nullptr;
  EXPECT_EQ(manager.TransmitChunk(short_request, &short_done).code(), BRStatusCode::OK);
  EXPECT_EQ(short_recvr->GetTotalChunks(9), 0);
  EXPECT_FALSE(short_recvr->HasOutputForPipeline(0));

  PTransmitChunkParams short_eos = MakeTransmitRequest(short_query, short_processor, 9, 1, 1, true);
  EXPECT_EQ(manager.TransmitChunk(short_eos, &short_done).code(), BRStatusCode::OK);
  EXPECT_TRUE(short_recvr->IsFinished());
  short_recvr->Close();
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(short_query), KStatus::SUCCESS);

  const KQueryId merge_query = 2004;
  const KProcessorId merge_processor = 24;
  ASSERT_EQ(manager.PreparePassThroughChunkBuffer(merge_query), KStatus::SUCCESS);
  auto merge_recvr = manager.CreateRecvr(merge_query, merge_processor, 2, 128, true, false);
  ASSERT_NE(merge_recvr, nullptr);

  auto sender_a_chunk = CreateChunk();
  auto sender_b_chunk = CreateChunk();
  merge_recvr->GetPassThroughContext().AppendChunk(10, sender_a_chunk, 16, 1);
  merge_recvr->GetPassThroughContext().AppendChunk(20, sender_b_chunk, 24, 2);
  PTransmitChunkParams sender_a = MakeTransmitRequest(merge_query, merge_processor, 10, 10, 0);
  PTransmitChunkParams sender_b = MakeTransmitRequest(merge_query, merge_processor, 20, 20, 0);
  google::protobuf::Closure* merge_done = nullptr;
  EXPECT_EQ(manager.TransmitChunk(sender_a, &merge_done).code(), BRStatusCode::OK);
  EXPECT_EQ(manager.TransmitChunk(sender_b, &merge_done).code(), BRStatusCode::OK);
  EXPECT_EQ(merge_recvr->GetTotalChunks(10), 1);
  EXPECT_EQ(merge_recvr->GetTotalChunks(20), 1);

  merge_recvr->Close();
  EXPECT_EQ(manager.DestroyPassThroughChunkBuffer(merge_query), KStatus::SUCCESS);
}

}  // namespace kwdbts
