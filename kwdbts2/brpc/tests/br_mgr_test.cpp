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

#include <gtest/gtest.h>

#include <memory>
#include <string>

// Expose private members of BrMgr for test-only state injection and singleton
// reset. The macro is undefined immediately after the include to limit its
// scope to this translation unit.
#define private public
#include "br_mgr.h"
#undef private

#include "br_data_stream_mgr.h"
#include "br_priority_thread_pool.h"
#include "br_stub_cache.h"

namespace kwdbts {

namespace {

// Resets all member fields of the BrMgr singleton to their default state.
//
// Invoked in SetUp() and TearDown() of every test fixture to guarantee that
// each test case starts and ends with a clean, isolated singleton, preventing
// cross-test state leakage caused by the singleton's persistent lifetime.
void ResetBrMgrSingletonState() {
  auto& mgr = BrMgr::GetInstance();
  // Release resources managed by Cleanup() (stub cache, stream mgr, RPC pool).
  mgr.Cleanup();
  // Explicitly reset fields that Cleanup() does not clear under WITH_TESTS.
  mgr.brpc_stub_cache_.reset();
  mgr.data_stream_mgr_.reset();
  mgr.query_rpc_pool_.reset();
  mgr.brpc_server_.reset();
  mgr.auth_.reset();
  mgr.cluster_id_.clear();
  mgr.address_.SetHostname("");
  mgr.address_.SetPort(0);
}

// Test fixture for BrMgr unit tests.
//
// Provides a freshly reset BrMgr singleton for every test case by calling
// ResetBrMgrSingletonState() in both SetUp() and TearDown(). Tests inject
// state directly into the singleton's private fields (made accessible via the
// #define private public hack above) to exercise getters and lifecycle
// behavior without performing a full BrMgr::Init().
class BrMgrTest : public ::testing::Test {
 protected:
  // Resets the BrMgr singleton before each test to ensure a clean slate.
  void SetUp() override { ResetBrMgrSingletonState(); }

  // Resets the BrMgr singleton after each test to avoid polluting later tests.
  void TearDown() override { ResetBrMgrSingletonState(); }
};

}  // namespace

// Verifies that BrMgr::GetInstance() satisfies the singleton contract by
// returning the same object address on repeated calls.
//
// No special setup is required; the test relies on the fresh singleton supplied
// by BrMgrTest::SetUp().
TEST_F(BrMgrTest, TestSingleton) {
  BrMgr& instance1 = BrMgr::GetInstance();
  BrMgr& instance2 = BrMgr::GetInstance();
  // Both references must point to the identical singleton instance.
  EXPECT_EQ(&instance1, &instance2);
}

// Verifies that all BrMgr getters return values that reflect state injected
// directly into the singleton's private fields, without calling BrMgr::Init().
//
// Expected results:
//   - GetBrpcStubCache() returns non-null after injecting a BrpcStubCache.
//   - GetDataStreamMgr() returns non-null after injecting a DataStreamMgr.
//   - GetPriorityThreadPool() returns non-null after injecting a thread pool.
//   - GetAuth() returns non-null after injecting a BoxAuthenticator.
//   - GetClusterID() returns the injected cluster ID string.
//   - GetAddr() reflects the injected hostname and port.
TEST_F(BrMgrTest, TestGettersReflectInjectedStateWithoutInit) {
  auto& mgr = BrMgr::GetInstance();

  // Inject each managed resource and configuration value directly.
  mgr.brpc_stub_cache_ = std::make_unique<BrpcStubCache>();
  mgr.data_stream_mgr_ = std::make_unique<DataStreamMgr>();
  mgr.query_rpc_pool_ = std::make_unique<PriorityThreadPool>("br-mgr", 0, 8);
  mgr.auth_ = std::make_unique<BoxAuthenticator>("cluster-token");
  mgr.cluster_id_ = "cluster-token";
  mgr.address_.SetHostname("127.0.0.1");
  mgr.address_.SetPort(8123);

  // All resource getters must return non-null pointers.
  EXPECT_NE(mgr.GetBrpcStubCache(), nullptr);
  EXPECT_NE(mgr.GetDataStreamMgr(), nullptr);
  EXPECT_NE(mgr.GetPriorityThreadPool(), nullptr);
  EXPECT_NE(mgr.GetAuth(), nullptr);
  // Configuration getters must reflect the injected values exactly.
  EXPECT_EQ(mgr.GetClusterID(), "cluster-token");
  EXPECT_EQ(mgr.GetAddr().hostname_, "127.0.0.1");
  EXPECT_EQ(mgr.GetAddr().port_, 8123);
}

// Verifies that BrMgr::Cleanup() releases managed resources (stub cache,
// stream manager, and thread pool) while leaving configuration fields
// (cluster ID and address) untouched.
//
// Expected results:
//   - After Cleanup(), all resource getters return nullptr.
//   - cluster_id_ and address_ retain the values set before Cleanup().
//   - Calling Cleanup() a second time on an already-cleaned instance is safe
//     and does not crash or alter the already-null resource pointers.
TEST_F(BrMgrTest, TestCleanupReleasesManagedResourcesWithoutDestroy) {
  auto& mgr = BrMgr::GetInstance();

  // Inject resources and configuration to establish a non-trivial initial state.
  mgr.brpc_stub_cache_ = std::make_unique<BrpcStubCache>();
  mgr.data_stream_mgr_ = std::make_unique<DataStreamMgr>();
  mgr.query_rpc_pool_ = std::make_unique<PriorityThreadPool>("br-cleanup", 0, 4);
  mgr.cluster_id_ = "cleanup-cluster";
  mgr.address_.SetHostname("localhost");
  mgr.address_.SetPort(9000);

  mgr.Cleanup();

  // Managed resources must have been released.
  EXPECT_EQ(mgr.GetBrpcStubCache(), nullptr);
  EXPECT_EQ(mgr.GetDataStreamMgr(), nullptr);
  EXPECT_EQ(mgr.GetPriorityThreadPool(), nullptr);
  // Configuration values must remain intact after Cleanup().
  EXPECT_EQ(mgr.GetClusterID(), "cleanup-cluster");
  EXPECT_EQ(mgr.GetAddr().hostname_, "localhost");
  EXPECT_EQ(mgr.GetAddr().port_, 9000);

  // A second Cleanup() on an already-cleaned instance must be idempotent.
  mgr.Cleanup();
  EXPECT_EQ(mgr.GetBrpcStubCache(), nullptr);
  EXPECT_EQ(mgr.GetDataStreamMgr(), nullptr);
  EXPECT_EQ(mgr.GetPriorityThreadPool(), nullptr);
}

}  // namespace kwdbts
