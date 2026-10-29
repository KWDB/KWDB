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

// Unit tests for BoxServiceBase<BoxService>, which is the internal BRPC service
// implementation responsible for handling DialDataRecvr, TransmitChunk, and
// SendExecStatus RPC calls. Tests cover normal paths, error conditions
// (e.g., missing receiver, full thread pool, attachment size mismatch), and
// null-callback robustness.

#include <google/protobuf/stubs/callback.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "brpc/controller.h"

// Expose private members of BrMgr and BoxServiceBase for test-only state
// injection and singleton reset.
#define private public
#include "br_internal_service.h"
#include "br_mgr.h"
#undef private

#include "br_data_stream_mgr.h"
#include "br_data_stream_recvr.h"
#include "br_priority_thread_pool.h"

namespace kwdbts {

namespace {

// Resets all member fields of the BrMgr singleton to their default state.
// Called in SetUp() and TearDown() to ensure each test starts and ends with a
// clean, isolated singleton, preventing cross-test state leakage.
void ResetBrMgrSingletonState() {
  auto& mgr = BrMgr::GetInstance();
  mgr.Cleanup();
  mgr.brpc_stub_cache_.reset();
  mgr.data_stream_mgr_.reset();
  mgr.query_rpc_pool_.reset();
  mgr.brpc_server_.reset();
  mgr.auth_.reset();
  mgr.cluster_id_.clear();
  mgr.address_.SetHostname("");
  mgr.address_.SetPort(0);
}

// A Closure implementation that atomically increments a counter each time
// Run() is called. Used to verify that synchronous done callbacks are invoked
// exactly once after an RPC method returns.
class CountingClosure : public google::protobuf::Closure {
 public:
  void Run() override { ++run_count; }

  // Number of times Run() has been invoked.
  std::atomic<int> run_count{0};
};

// A Closure implementation that supports blocking the test thread until the
// closure has been run by a background worker. Used to verify asynchronous
// done callbacks dispatched through the PriorityThreadPool.
class WaitingClosure : public google::protobuf::Closure {
 public:
  // Increments the run counter, signals notified_, and wakes all waiters.
  void Run() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      run_count_++;
      notified_ = true;
    }
    cv_.notify_all();
  }

  // Blocks the calling thread until Run() is invoked or the 2-second timeout
  // expires. Returns true if Run() was called before the timeout.
  bool WaitForRun() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(2), [this]() { return notified_; });
  }

  // Returns the number of times Run() has been called. Thread-safe.
  int run_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return run_count_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  // Set to true once Run() is called; guards the condition variable wait.
  bool notified_ = false;
  int run_count_ = 0;
};

// Records the arguments and invocation count of a ReceiveNotify callback.
// Installed on a DataStreamRecvr to verify that SendExecStatus correctly
// triggers the registered notification with the expected parameters.
struct NotifyRecorder {
  // Last node_id received via the notification callback.
  int node_id = 0;
  // Last error code received via the notification callback.
  int code = 0;
  // Last error message received via the notification callback.
  std::string msg;
  // Total number of times the notification callback has been invoked.
  int call_count = 0;

  // Returns a ReceiveNotify lambda that captures this recorder and populates
  // its fields whenever the notification fires.
  ReceiveNotify AsCallback() {
    return [this](k_int32 in_node_id, k_int32 in_code, const std::string& in_msg) {
      node_id = in_node_id;
      code = in_code;
      msg = in_msg;
      ++call_count;
    };
  }
};

// Constructs a PTransmitChunkParams protobuf message configured for
// pass-through mode with the specified routing and sequencing fields.
//
// @param query_id       Unique identifier of the query.
// @param processor_id   Destination processor identifier.
// @param sender_id      Identifier of the sending node.
// @param be_number      Back-end instance number.
// @param sequence       Sequence number of this chunk transmission.
// @return               A fully populated PTransmitChunkParams request.
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

// Constructs a PDialDataRecvr protobuf message targeting the specified query
// and processor.
//
// @param query_id      Unique identifier of the query.
// @param processor_id  Destination processor identifier.
// @return              A fully populated PDialDataRecvr request.
PDialDataRecvr MakeDialRequest(KQueryId query_id, KProcessorId processor_id) {
  PDialDataRecvr request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  return request;
}

// Constructs a PSendExecStatus protobuf message representing a failed
// execution status with a fixed error code and message, targeting the
// specified query and processor.
//
// @param query_id      Unique identifier of the query.
// @param processor_id  Destination processor identifier.
// @return              A PSendExecStatus request with EXEC_STATUS_FAILED,
//                      error_code=321, and error_msg="internal-service-test".
PSendExecStatus MakeExecStatusRequest(KQueryId query_id, KProcessorId processor_id) {
  PSendExecStatus request;
  request.set_query_id(query_id);
  request.set_dest_processor(processor_id);
  request.set_exec_status(EXEC_STATUS_FAILED);
  request.set_error_code(321);
  request.set_error_msg("internal-service-test");
  return request;
}

// Test fixture for BoxServiceBase<BoxService> (internal BRPC service).
//
// Each test runs with a fresh BrMgr singleton state: the singleton is reset in
// SetUp() and TearDown() to prevent cross-test contamination. A real
// DataStreamMgr is injected directly into the singleton so that service
// methods exercise actual stream management logic without a full BrMgr::Init().
class BrInternalServiceTest : public ::testing::Test {
 protected:
  // Resets the BrMgr singleton and injects a fresh DataStreamMgr.
  void SetUp() override {
    ResetBrMgrSingletonState();
    auto& mgr = BrMgr::GetInstance();
    mgr.data_stream_mgr_ = std::make_unique<DataStreamMgr>();
  }

  // Resets the BrMgr singleton to avoid polluting subsequent tests.
  void TearDown() override { ResetBrMgrSingletonState(); }

  // Registers a pass-through chunk buffer and creates a DataStreamRecvr for
  // the given query and processor. Asserts that both operations succeed.
  //
  // @param query_id      Unique identifier of the query.
  // @param processor_id  Destination processor identifier.
  // @return              Shared pointer to the newly created DataStreamRecvr.
  std::shared_ptr<DataStreamRecvr> PrepareRecvr(KQueryId query_id, KProcessorId processor_id) {
    auto* mgr = BrMgr::GetInstance().GetDataStreamMgr();
    EXPECT_EQ(mgr->PreparePassThroughChunkBuffer(query_id), KStatus::SUCCESS);
    auto recvr = mgr->CreateRecvr(query_id, processor_id, 1, 64, false, false);
    EXPECT_NE(recvr, nullptr);
    return recvr;
  }

  // The service under test. Holds a pointer to the BrMgr singleton so it
  // operates on the same DataStreamMgr injected in SetUp().
  BoxServiceBase<BoxService> service_{&BrMgr::GetInstance()};
};

}  // namespace

// Verifies the end-to-end behavior of DialDataRecvr and SendExecStatus against
// the DataStreamMgr:
//   1. DialDataRecvrInternal returns NOT_READY for a non-existent receiver.
//   2. DialDataRecvr returns OK and invokes done synchronously for a registered
//      receiver.
//   3. SendExecStatus returns OK, invokes done synchronously, and fires the
//      ReceiveNotify callback on the receiver with the correct error code and
//      message.
TEST_F(BrInternalServiceTest, DialAndExecWrappersReflectStreamManagerState) {
  auto recvr = PrepareRecvr(5001, 51);
  NotifyRecorder recorder;
  recvr->SetReceiveNotify(recorder.AsCallback());

  // Dialing a non-existent receiver should return NOT_READY.
  PDialDataRecvr missing_request = MakeDialRequest(9999, 1);
  PDialDataRecvrResult dial_missing_response;
  service_.DialDataRecvrInternal(nullptr, nullptr, &missing_request, &dial_missing_response);
  EXPECT_EQ(static_cast<BRStatusCode>(dial_missing_response.status().status_code()),
            BRStatusCode::NOT_READY);

  // Dialing a registered receiver should succeed and call done exactly once.
  PDialDataRecvr dial_request = MakeDialRequest(5001, 51);
  PDialDataRecvrResult dial_response;
  CountingClosure dial_done;
  service_.DialDataRecvr(nullptr, &dial_request, &dial_response, &dial_done);
  EXPECT_EQ(static_cast<BRStatusCode>(dial_response.status().status_code()), BRStatusCode::OK);
  EXPECT_EQ(dial_done.run_count.load(), 1);

  // SendExecStatus should propagate the error code and message to the
  // registered ReceiveNotify callback.
  PSendExecStatus exec_request = MakeExecStatusRequest(5001, 51);
  PSendExecStatusResult exec_response;
  CountingClosure exec_done;
  service_.SendExecStatus(nullptr, &exec_request, &exec_response, &exec_done);
  EXPECT_EQ(static_cast<BRStatusCode>(exec_response.status().status_code()), BRStatusCode::OK);
  EXPECT_EQ(exec_done.run_count.load(), 1);
  EXPECT_EQ(recorder.call_count, 1);
  EXPECT_EQ(recorder.code, 321);
  EXPECT_EQ(recorder.msg, "internal-service-test");
}

// Verifies that TransmitChunk responds with SERVICE_UNAVAILABLE and invokes
// done synchronously when the underlying PriorityThreadPool cannot accept new
// tasks (zero threads and zero queue capacity).
TEST_F(BrInternalServiceTest, TransmitChunkRejectsWhenThreadPoolQueueIsFull) {
  auto& mgr = BrMgr::GetInstance();
  // A thread pool with 0 threads and 0 queue capacity will always reject tasks.
  mgr.query_rpc_pool_ = std::make_unique<PriorityThreadPool>("svc-full", 0, 0);

  PTransmitChunkParams request = MakePassThroughRequest(7001, 71, 1, 1, 0);
  PTransmitChunkResult response;
  CountingClosure done;
  brpc::Controller controller;

  service_.TransmitChunk(&controller, &request, &response, &done);

  EXPECT_EQ(static_cast<BRStatusCode>(response.status().status_code()),
            BRStatusCode::SERVICE_UNAVAILABLE);
  EXPECT_EQ(done.run_count.load(), 1);
}

// Verifies that TransmitChunk dispatches work asynchronously to the thread
// pool and, upon completion, returns OK and delivers the chunk to the correct
// DataStreamRecvr.
TEST_F(BrInternalServiceTest, TransmitChunkWrapperRunsAsyncWorkAgainstDataStreamManager) {
  auto& mgr = BrMgr::GetInstance();
  // A thread pool with 1 worker and queue capacity 4 allows tasks to run.
  mgr.query_rpc_pool_ = std::make_unique<PriorityThreadPool>("svc-ok", 1, 4);

  auto recvr = PrepareRecvr(6001, 61);
  // Pre-populate the pass-through context so the service finds a matching chunk
  // for sender_id=7.
  auto pass_through_chunk = std::make_unique<DataChunk>();
  recvr->GetPassThroughContext().AppendChunk(7, pass_through_chunk, 24, 11);

  PTransmitChunkParams request = MakePassThroughRequest(6001, 61, 7, 8, 0);
  PTransmitChunkResult response;
  // WaitingClosure blocks the test until the async work completes.
  WaitingClosure done;
  brpc::Controller controller;

  service_.TransmitChunk(&controller, &request, &response, &done);

  // Wait up to 2 seconds for the background thread to invoke done.
  ASSERT_TRUE(done.WaitForRun());
  EXPECT_EQ(done.run_count(), 1);
  EXPECT_EQ(static_cast<BRStatusCode>(response.status().status_code()), BRStatusCode::OK);
  // The receiver should have recorded exactly one chunk for sender_id=7.
  EXPECT_EQ(recvr->GetTotalChunks(7), 1);
}

// Verifies two edge cases handled inside TransmitChunkInternal:
//   1. Attachment size mismatch: when the actual request attachment length is
//      smaller than the declared data_size in the chunk descriptor, the method
//      must return INTERNAL_ERROR and still invoke done.
//   2. Null done callback: when done is nullptr, the method must complete
//      successfully (returning OK and delivering the chunk) without crashing.
TEST_F(BrInternalServiceTest, TransmitChunkInternalHandlesAttachmentsAndNullCallbacks) {
  auto* stream_mgr = BrMgr::GetInstance().GetDataStreamMgr();
  ASSERT_NE(stream_mgr, nullptr);
  ASSERT_EQ(stream_mgr->PreparePassThroughChunkBuffer(6101), KStatus::SUCCESS);
  auto recvr = stream_mgr->CreateRecvr(6101, 62, 1, 64, false, false);
  ASSERT_NE(recvr, nullptr);

  // Case 1: attachment size (3 bytes) is less than the declared data_size (5),
  // which should trigger an INTERNAL_ERROR response.
  PTransmitChunkParams attachment_error_request;
  attachment_error_request.set_query_id(6101);
  attachment_error_request.set_dest_processor(62);
  attachment_error_request.set_sender_id(4);
  attachment_error_request.set_be_number(9);
  attachment_error_request.set_sequence(0);
  auto* error_chunk = attachment_error_request.add_chunks();
  error_chunk->set_data_size(5);
  error_chunk->set_serialized_size(5);

  PTransmitChunkResult attachment_error_response;
  CountingClosure attachment_error_done;
  brpc::Controller attachment_error_controller;
  // Append only 3 bytes, creating a mismatch with data_size=5.
  attachment_error_controller.request_attachment().append("123");

  service_.TransmitChunkInternal(&attachment_error_controller, &attachment_error_done,
                                 &attachment_error_request, &attachment_error_response);

  EXPECT_EQ(static_cast<BRStatusCode>(attachment_error_response.status().status_code()),
            BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(attachment_error_done.run_count.load(), 1);

  // Case 2: passing nullptr as done should not crash; the service must handle
  // it gracefully and still deliver the chunk to the receiver.
  auto pass_through_chunk = std::make_unique<DataChunk>();
  recvr->GetPassThroughContext().AppendChunk(4, pass_through_chunk, 12, 5);
  PTransmitChunkParams request = MakePassThroughRequest(6101, 62, 4, 9, 0);
  PTransmitChunkResult response;
  brpc::Controller controller;

  service_.TransmitChunkInternal(&controller, nullptr, &request, &response);

  EXPECT_EQ(static_cast<BRStatusCode>(response.status().status_code()), BRStatusCode::OK);
  // Confirm the chunk was successfully delivered to the receiver.
  EXPECT_EQ(recvr->GetTotalChunks(4), 1);
}

}  // namespace kwdbts
