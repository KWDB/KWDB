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
#include <limits>
#include <memory>

#include "brpc/controller.h"

// Expose private members of the recoverable stub and manager for white-box testing.
#define private public
#include "br_internal_service_recoverable_stub.h"
#include "br_mgr.h"
#undef private

namespace kwdbts {

namespace {

// Resets all internal state of the BrMgr singleton to a clean initial state.
// This ensures test isolation by clearing stub caches, stream managers, RPC
// pools, the BRPC server, authenticator, cluster ID, and bound address.
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

// A simple Closure implementation that counts the number of times Run() is
// called. Used to verify that RPC completion callbacks are invoked exactly once.
class CountingClosure : public google::protobuf::Closure {
 public:
  // Increments run_count atomically each time the closure is executed.
  void Run() override { ++run_count; }

  // Tracks how many times Run() has been called; must be read with load().
  std::atomic<int> run_count{0};
};

// Parses a "host:port" string into a butil::EndPoint.
// Fails the current test via EXPECT_EQ if parsing is unsuccessful.
butil::EndPoint ParseEndpoint(const char* address) {
  butil::EndPoint endpoint;
  EXPECT_EQ(str2endpoint(address, &endpoint), 0);
  return endpoint;
}

// Installing a fake session lets tests exercise retry/throttle logic without
// invoking brpc::Channel::Init(), which starts global bthread infrastructure.
void InstallFakeSession(const std::shared_ptr<BoxServiceRetryableClosureStub>& stub) {
  auto session = std::make_shared<BoxServiceRetryableClosureStub::Session>();
  session->channel = std::make_shared<brpc::Channel>();
  session->stub = std::make_shared<BoxService_Stub>(
      session->channel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
  stub->session_ = std::move(session);
}

// Test fixture for BoxServiceRetryableClosureStub internal behaviour.
//
// Each test case gets a freshly reset BrMgr singleton with a dummy
// BoxAuthenticator pre-installed so that channel creation can succeed without
// a real cluster.
class BrRecoverableStubInternalTest : public ::testing::Test {
 protected:
  // Resets the BrMgr singleton and installs a test-only authenticator before
  // every test.
  void SetUp() override {
    ResetBrMgrSingletonState();
    BrMgr::GetInstance().auth_ = std::make_unique<BoxAuthenticator>("retryable-cluster");
  }

  // Restores the BrMgr singleton to a clean state after every test.
  void TearDown() override { ResetBrMgrSingletonState(); }
};

}  // namespace

// Verifies that RetryableClosure::Run() does NOT trigger a channel reset in
// three situations:
//   1. The RPC succeeded (no error on the controller).
//   2. The RPC failed with a non-EHOSTDOWN error (e.g. EPERM).
//   3. The host-down reset is throttled (last_host_down_reset_ns_ is at its
//      maximum value, indicating a reset was performed very recently).
//
// In all three cases the closure's done callback must still be invoked exactly
// once, and the existing session must remain unchanged.
TEST_F(BrRecoverableStubInternalTest, RetryableClosureRunSkipsResetForNonHostDownAndThrottle) {
  auto stub = std::make_shared<BoxServiceRetryableClosureStub>(ParseEndpoint("127.0.0.1:8124"));
  InstallFakeSession(stub);
  auto session_before = stub->GetSessionSnapshot();
  ASSERT_NE(session_before, nullptr);

  // Case 1: successful RPC – session must be preserved and callback fired once.
  CountingClosure non_failed_done;
  brpc::Controller non_failed_controller;
  (new RetryableClosure(stub, &non_failed_controller, &non_failed_done))->Run();
  EXPECT_EQ(non_failed_done.run_count.load(), 1);
  EXPECT_EQ(stub->GetSessionSnapshot().get(), session_before.get());

  // Case 2: non-EHOSTDOWN failure (EPERM) – last_host_down_reset_ns_ must be
  // left untouched and no new session should be created.
  CountingClosure other_error_done;
  brpc::Controller other_error_controller;
  other_error_controller.SetFailed(EPERM, "permission denied");
  stub->last_host_down_reset_ns_ = 12345;
  (new RetryableClosure(stub, &other_error_controller, &other_error_done))->Run();
  EXPECT_EQ(other_error_done.run_count.load(), 1);
  EXPECT_EQ(stub->last_host_down_reset_ns_, 12345);

  // Case 3: EHOSTDOWN but throttled (timestamp at max) – neither the timestamp
  // nor the session pointer should change.
  CountingClosure throttled_done;
  brpc::Controller throttled_controller;
  throttled_controller.SetFailed(EHOSTDOWN, "host down");
  stub->last_host_down_reset_ns_ = std::numeric_limits<k_int64>::max();
  (new RetryableClosure(stub, &throttled_controller, &throttled_done))->Run();
  EXPECT_EQ(throttled_done.run_count.load(), 1);
  EXPECT_EQ(stub->last_host_down_reset_ns_, std::numeric_limits<k_int64>::max());
  EXPECT_EQ(stub->GetSessionSnapshot().get(), session_before.get());
}

}  // namespace kwdbts
