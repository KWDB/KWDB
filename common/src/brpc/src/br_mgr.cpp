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

#include "br_mgr.h"

#include <brpc/server.h>
#include <brpc/socket.h>

#include "br_internal_service.h"
#include "ee_cpuinfo.h"

namespace kwdbts {

// Global Internal Service Instance
static std::unique_ptr<BoxServiceBase<BoxService>> s_internal_service;

KStatus BrMgr::Init(kwdbContext_p ctx, const EngineOptions& options) {
  EnterFunc();

  CpuInfo::Init();
  k_int32 num_cores = CpuInfo::Get_Num_Cores();

  // Initialize Thread Pool
  try {
    query_rpc_pool_ = std::make_unique<PriorityThreadPool>("query_rpc", num_cores,
                                                           std::numeric_limits<uint32_t>::max());
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to initialize PriorityThreadPool:%s", e.what());
    return KStatus::FAIL;
  }

  // Initialize RPC Stub Cache
  try {
    brpc_stub_cache_ = std::make_unique<BrpcStubCache>();
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to initialize BrpcStubCache:%s", e.what());
    return KStatus::FAIL;
  }

  // Initialize Data Stream Manager
  try {
    data_stream_mgr_ = std::make_unique<DataStreamMgr>();
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to initialize DataStreamMgr:%s", e.what());
    return KStatus::FAIL;
  }

  // Initialize Internal Service
  try {
    s_internal_service = std::make_unique<BoxServiceBase<BoxService>>(this);
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to initialize BoxServiceBase:%s", e.what());
    return KStatus::FAIL;
  }

#ifndef WITH_TESTS
  // Maximum size of a single message body in all protocols
  brpc::FLAGS_max_body_size = 2147483648;

  // Create and Configure BRPC Server
  try {
    brpc_server_ = std::make_unique<brpc::Server>();
    brpc_server_->AddService(s_internal_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE);

    // Parse and Set Server Endpoint
    butil::EndPoint point;
    if (butil::str2endpoint(options.brpc_addr.c_str(), &point) < 0) {
      LOG_ERROR("Failed to parse endpoint address:%s", options.brpc_addr.c_str());
      return KStatus::FAIL;
    }
    address_.SetHostname(butil::ip2str(point.ip).c_str());
    address_.SetPort(point.port);

    cluster_id_ = options.cluster_id;
    auth_ = std::make_unique<BoxAuthenticator>(cluster_id_);

    // Start Server
    brpc::ServerOptions brpc_options;
    brpc_options.num_threads = num_cores;
    brpc_options.auth = auth_.get();
    brpc_options.has_builtin_services = false;
    if (auto ret = brpc_server_->Start(point, &brpc_options); ret != 0) {
      LOG_ERROR("Failed to start BRPC server, brpc_addr:%s, error code:%d",
                options.brpc_addr.c_str(), ret);
      return KStatus::FAIL;
    }

    LOG_INFO("BRPC server started successfully on %s", options.brpc_addr.c_str());
  } catch (const std::exception& e) {
    LOG_ERROR("BRPC server initialization failed:%s", e.what());
    return KStatus::FAIL;
  }
#endif

  return KStatus::SUCCESS;
}

void BrMgr::Cleanup() {
#ifndef WITH_TESTS
  // Gracefully Shut Down BRPC Server
  if (brpc_server_) {
    brpc_server_->Stop(0);
    brpc_server_->Join();
    brpc_server_.reset();
  }

  auth_.reset();
#endif
  // Clean Up Resources
  s_internal_service.reset();

  if (data_stream_mgr_) {
    data_stream_mgr_->Close();
    data_stream_mgr_.reset();
  }

  brpc_stub_cache_.reset();
  query_rpc_pool_.reset();
}

KStatus BrMgr::Destroy() {
  Cleanup();
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
