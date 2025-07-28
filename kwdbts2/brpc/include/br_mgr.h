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

#include <memory>
#include <string>

#include "br_auth.h"
#include "br_data_stream_mgr.h"
#include "br_priority_thread_pool.h"
#include "br_stub_cache.h"
#include "cm_func.h"
#include "cm_kwdb_context.h"
#include "kwdb_type.h"
#include "settings.h"

namespace kwdbts {

// BRPC Service Manager Class
// Responsible for managing the lifecycle and resources of the BRPC service
class BrMgr {
 public:
  // Singleton Access Interface
  static BrMgr& GetInstance() {
    static BrMgr instance;
    return instance;
  }

  // Disable Copy and Move
  BrMgr(const BrMgr&) = delete;
  BrMgr& operator=(const BrMgr&) = delete;
  BrMgr(BrMgr&&) = delete;
  BrMgr& operator=(BrMgr&&) = delete;

  // Initialize BRPC Service
  // @param ctx: KWDB Context
  // @param options: Engine Options
  // @return: Operation Status
  KStatus Init(kwdbContext_p ctx, const EngineOptions& options);
  KStatus Destroy();

  // Get RPC Stub Cache
  BrpcStubCache* GetBrpcStubCache() const { return brpc_stub_cache_.get(); }

  // Get Data Stream Manager
  DataStreamMgr* GetDataStreamMgr() const { return data_stream_mgr_.get(); }

  // Get Priority Thread Pool
  PriorityThreadPool* GetPriorityThreadPool() const { return query_rpc_pool_.get(); }

  TNetworkAddress& GetAddr() { return address_; }

  std::string GetClusterID() const { return cluster_id_; }

  BoxAuthenticator* GetAuth() const { return auth_.get(); }

 private:
  BrMgr() = default;
  ~BrMgr() = default;

  // Resource Cleanup
  void Cleanup();

  // Member Variables
  std::unique_ptr<BrpcStubCache> brpc_stub_cache_;
  std::unique_ptr<DataStreamMgr> data_stream_mgr_;
  std::unique_ptr<PriorityThreadPool> query_rpc_pool_;
  std::unique_ptr<brpc::Server> brpc_server_;
  TNetworkAddress address_;
  std::string cluster_id_;
  std::unique_ptr<BoxAuthenticator> auth_;
};

}  // namespace kwdbts
