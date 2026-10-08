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
#include <cerrno>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#define private public
#include "br_stub_cache.h"
#undef private

#include "br_auth.h"
#include "br_disposable_closure.h"
#include "br_global.h"
#include "br_internal_service_recoverable_stub.h"
#include "br_mgr.h"
#include "brpc/controller.h"
#include "settings.h"

namespace kwdbts {

namespace {

// Test helper that counts how many times Run() has been invoked.
// Used to verify that RPC callbacks are properly executed.
class CountingClosure : public google::protobuf::Closure {
 public:
  void Run() override { ++run_count; }

  std::atomic<int> run_count{0};
};

// Test fixture for DisposableClosure that tracks destructor invocations.
// Verifies self-delete semantics: the closure deletes itself after Run().
class DisposableClosureForTest : public DisposableClosure<int, int> {
 public:
  // Constructs a closure with the given context value.
  // @param ctx User-defined context passed to success/failure handlers.
  explicit DisposableClosureForTest(int ctx) : DisposableClosure<int, int>(ctx) {}

  // Increments destructor_count on destruction to verify self-delete behavior.
  ~DisposableClosureForTest() override { ++destructor_count; }

  // Tracks total number of closures that have been destroyed.
  // Reset before each test that uses DisposableClosure.
  static std::atomic<int> destructor_count;
};

std::atomic<int> DisposableClosureForTest::destructor_count{0};

// Base test fixture for CoreTypesTest test suite.
// Provides common setup/teardown for test context initialization.
class CoreTypesTest : public ::testing::Test {};

}  // namespace

// Tests BRStatus lifecycle: construction, copy/move semantics, and Protobuf
// serialization round-trip. Verifies that status objects correctly preserve
// error codes and messages through all operations.
TEST_F(CoreTypesTest, BrStatusCoversCopyMoveAndProtobufRoundTrip) {
  // Default construction yields OK status with empty message.
  BRStatus ok;
  EXPECT_EQ(ok.code(), BRStatusCode::OK);
  EXPECT_TRUE(ok.message().empty());

  // Factory method creates error status with message.
  BRStatus internal = BRStatus::InternalError("internal error");
  EXPECT_EQ(internal.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(internal.message(), "internal error");

  // Copy construction preserves code and message.
  BRStatus copy(internal);
  EXPECT_EQ(copy.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(copy.message(), "internal error");

  // Copy assignment preserves code and message.
  BRStatus assigned;
  assigned = internal;
  EXPECT_EQ(assigned.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(assigned.message(), "internal error");

  // Move construction transfers state efficiently without copying.
  BRStatus moved(std::move(internal));
  EXPECT_EQ(moved.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(moved.message(), "internal error");

  // Move assignment transfers state efficiently without copying.
  BRStatus moved_assigned;
  moved_assigned = std::move(copy);
  EXPECT_EQ(moved_assigned.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(moved_assigned.message(), "internal error");

  // Serialize to Protobuf and restore: verifies wire format compatibility.
  StatusPB status_pb;
  moved_assigned.toProtobuf(&status_pb);
  BRStatus restored(status_pb);
  EXPECT_EQ(restored.code(), BRStatusCode::INTERNAL_ERROR);
  EXPECT_EQ(restored.message(), "internal error");

  // Empty message is preserved during round-trip.
  StatusPB empty_status_pb;
  empty_status_pb.set_status_code(static_cast<int>(BRStatusCode::NOT_READY));
  BRStatus empty_msg_status(empty_status_pb);
  EXPECT_EQ(empty_msg_status.code(), BRStatusCode::NOT_READY);
  EXPECT_TRUE(empty_msg_status.message().empty());

  // Messages exceeding 65535 bytes are truncated to the maximum allowed size.
  std::string oversized(70000, 'x');
  BRStatus truncated = BRStatus::Unknown(oversized);
  EXPECT_EQ(truncated.code(), BRStatusCode::UNKNOWN);
  EXPECT_EQ(truncated.message().size(), 65535u);
}

// Tests BoxAuthenticator credential generation and validation.
// Verifies that the authenticator correctly generates HMAC-based credentials
// and rejects tampered, malformed, or expired credentials.
TEST_F(CoreTypesTest, BoxAuthenticatorValidatesCredentialAndRejectsTampering) {
  // Create authenticator with cluster token and generate a valid credential.
  BoxAuthenticator auth("cluster-token");
  KString credential;
  ASSERT_EQ(auth.GenerateCredential(&credential), 0);
  EXPECT_FALSE(credential.empty());

  // Valid credential passes verification.
  butil::EndPoint endpoint;
  brpc::AuthContext auth_ctx;
  EXPECT_EQ(auth.VerifyCredential(credential, endpoint, &auth_ctx), 0);

  // Malformed credentials (wrong format) are rejected.
  EXPECT_EQ(auth.VerifyCredential("invalid-format", endpoint, &auth_ctx), -1);

  // Tampering with any byte invalidates the credential.
  // Flips the last hex digit to ensure integrity check fails.
  KString tampered = credential;
  tampered.back() = tampered.back() == '0' ? '1' : '0';
  EXPECT_EQ(auth.VerifyCredential(tampered, endpoint, &auth_ctx), -1);

  // Parse credential components: timestamp, nonce, and signature.
  // Verify that credentials with expired timestamps are rejected.
  unsigned int timestamp = 0;
  unsigned int nonce = 0;
  unsigned long long signature = 0;
  ASSERT_EQ(std::sscanf(credential.c_str(), "%u,%x,%llx", &timestamp, &nonce, &signature), 3);

  // Construct expired credential by backdating timestamp beyond expiration window.
  char expired[64];
  std::snprintf(expired, sizeof(expired), "%u,%08x,%016llx",
                timestamp - BOX_AUTH_TIMESTAMP_EXPIRE_SECONDS - 1, nonce, signature);
  EXPECT_EQ(auth.VerifyCredential(expired, endpoint, &auth_ctx), -1);
}

// Tests DisposableClosure success path: verifies that the success handler
// receives correct context and result values, and that the closure self-deletes
// after Run() completes successfully.
TEST_F(CoreTypesTest, DisposableClosureRunsSuccessAndSelfDeletes) {
  DisposableClosureForTest::destructor_count = 0;
  int success_ctx = 0;
  int success_result = 0;

  // Create closure with context value 7 and set result to 42.
  auto* closure = new DisposableClosureForTest(7);
  closure->result = 42;

  // Register success handler that captures context and result.
  closure->AddSuccessHandler([&](const int& ctx, const int& result) {
    success_ctx = ctx;
    success_result = result;
  });
  closure->AddFailedHandler([](const int&, std::string_view) { FAIL() << "unexpected failure"; });

  // Trigger the closure execution.
  closure->Run();

  // Verify success handler received correct values.
  EXPECT_EQ(success_ctx, 7);
  EXPECT_EQ(success_result, 42);

  // Verify closure self-deleted after Run().
  EXPECT_EQ(DisposableClosureForTest::destructor_count.load(), 1);
}

// Tests DisposableClosure failure path: verifies that the failure handler
// is invoked when the controller indicates failure, and that exceptions
// thrown by handlers are caught and not propagated.
TEST_F(CoreTypesTest, DisposableClosureRunsFailureAndSwallowsHandlerExceptions) {
  DisposableClosureForTest::destructor_count = 0;
  int failed_ctx = 0;
  std::string failure_message;

  // First failure test: controller reports EPERM error.
  auto* failed = new DisposableClosureForTest(11);
  failed->AddSuccessHandler([](const int&, const int&) { FAIL() << "unexpected success"; });
  failed->AddFailedHandler([&](const int& ctx, std::string_view msg) {
    failed_ctx = ctx;
    failure_message.assign(msg.data(), msg.size());
  });
  failed->cntl.SetFailed(EPERM, "permission denied");
  failed->Run();

  // Verify failure handler received correct context and error message.
  EXPECT_EQ(failed_ctx, 11);
  EXPECT_NE(failure_message.find("permission denied"), std::string::npos);

  // Second exception test: success handler throws but exception is swallowed.
  // The closure should still self-delete without propagating the exception.
  auto* throwing = new DisposableClosureForTest(13);
  throwing->AddSuccessHandler([](const int&, const int&) { throw std::runtime_error("boom"); });
  throwing->AddFailedHandler([](const int&, std::string_view) {});
  throwing->Run();

  // Both closures should be destroyed (self-delete worked despite exception).
  EXPECT_EQ(DisposableClosureForTest::destructor_count.load(), 2);
}

// Tests BoxServiceRetryableClosureStub behavior when no session is established.
// Verifies that all RPC entrypoints fail fast with appropriate error indication
// and that callbacks are executed (though with failure status).
TEST_F(CoreTypesTest, RetryableStubFailsFastWithoutSessionForAllRpcEntrypoints) {
  // Create stub with arbitrary endpoint (no actual server listening).
  auto stub =
      std::make_shared<BoxServiceRetryableClosureStub>(butil::EndPoint{butil::IP_ANY, 12345});

  // Prepare request/response objects for three RPC methods.
  PDialDataRecvr dial_request;
  PDialDataRecvrResult dial_response;
  PTransmitChunkParams transmit_request;
  PTransmitChunkResult transmit_response;
  PSendExecStatus exec_request;
  PSendExecStatusResult exec_response;

  // Create individual controllers and callbacks for each RPC.
  brpc::Controller dial_controller;
  brpc::Controller transmit_controller;
  brpc::Controller exec_controller;
  CountingClosure dial_done;
  CountingClosure transmit_done;
  CountingClosure exec_done;

  // Issue all three RPCs without waiting.
  stub->DialDataRecvr(&dial_controller, &dial_request, &dial_response, &dial_done);
  stub->TransmitChunk(&transmit_controller, &transmit_request, &transmit_response, &transmit_done);
  stub->SendExecStatus(&exec_controller, &exec_request, &exec_response, &exec_done);

  // All controllers should indicate failure (no session established).
  EXPECT_TRUE(dial_controller.Failed());
  EXPECT_TRUE(transmit_controller.Failed());
  EXPECT_TRUE(exec_controller.Failed());

  // All callbacks should have been invoked exactly once.
  EXPECT_EQ(dial_done.run_count.load(), 1);
  EXPECT_EQ(transmit_done.run_count.load(), 1);
  EXPECT_EQ(exec_done.run_count.load(), 1);
}

// Tests BrpcStubCache lookup behaviour without triggering a real channel reset.
// Invalid hostnames should still fail, and pre-populated entries should be
// returned consistently through the hostname and TNetworkAddress overloads.
TEST_F(CoreTypesTest, StubCacheReturnsNullForInvalidHostAndCachesValidEndpoint) {
  BrpcStubCache cache;

  // Invalid hostnames should return nullptr immediately (no network lookup).
  EXPECT_EQ(cache.GetStub("host.invalid.test", 8123), nullptr);

  // Seed the cache with a ready-made stub so this test exercises lookup and
  // reuse semantics without starting brpc global worker/timer threads.
  butil::EndPoint endpoint;
  ASSERT_EQ(str2endpoint("127.0.0.1:8123", &endpoint), 0);
  auto* pool = new BrpcStubCache::StubPool();
  pool->stubs_.push_back(std::make_shared<BoxServiceRetryableClosureStub>(endpoint));
  ASSERT_TRUE(cache.stub_map_.insert(endpoint, pool));

  // Valid host/port should return the pre-populated cached stub instance.
  auto first = cache.GetStub("127.0.0.1", 8123);
  auto second = cache.GetStub("127.0.0.1", 8123);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  // Second call should return the same instance (cache singleton).
  EXPECT_EQ(first.get(), second.get());

  // Cache should also support lookup via TNetworkAddress struct.
  TNetworkAddress addr;
  addr.hostname_ = "127.0.0.1";
  addr.port_ = 8123;
  auto from_addr = cache.GetStub(addr);
  ASSERT_NE(from_addr, nullptr);

  // Address-based lookup should also return the cached instance.
  EXPECT_EQ(first.get(), from_addr.get());
}

}  // namespace kwdbts
