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
// BrMgr: BRPC Service Manager Class
// This class is responsible for managing the lifecycle, initialization, and resources of the BRPC
// service. It provides interfaces for accessing core components such as the stub cache, data stream
// manager, thread pool, and authentication. The class is implemented as a singleton to ensure a
// single instance throughout the application.
class BrMgr {
 public:
  // Disable copy constructor
  BrMgr(const BrMgr&) = delete;
  // Disable copy assignment operator
  BrMgr& operator=(const BrMgr&) = delete;
  // Disable move constructor
  BrMgr(BrMgr&&) = delete;
  // Disable move assignment operator
  BrMgr& operator=(BrMgr&&) = delete;

  // Initialize the BRPC service.
  // @param ctx: KWDB context pointer.
  // @param options: Engine options for initialization.
  // @return: Operation status (KStatus).
  KStatus Init(kwdbContext_p ctx, const EngineOptions& options);

  // Destroy the BRPC service and release resources.
  // @return: Operation status (KStatus).
  KStatus Destroy();

  // Get the RPC stub cache.
  // @return: Pointer to BrpcStubCache.
  BrpcStubCache* GetBrpcStubCache() const { return brpc_stub_cache_.get(); }

  // Get the data stream manager.
  // @return: Pointer to DataStreamMgr.
  DataStreamMgr* GetDataStreamMgr() const { return data_stream_mgr_.get(); }

  // Get the priority thread pool for query RPC.
  // @return: Pointer to PriorityThreadPool.
  PriorityThreadPool* GetPriorityThreadPool() const { return query_rpc_pool_.get(); }

  // Get the network address of the BRPC service.
  // @return: Reference to TNetworkAddress.
  TNetworkAddress& GetAddr() { return address_; }

  // Get the cluster ID.
  // @return: Cluster ID as a string.
  std::string GetClusterID() const { return cluster_id_; }

  // Get the authenticator for the service.
  // @return: Pointer to BoxAuthenticator.
  BoxAuthenticator* GetAuth() const { return auth_.get(); }

  // Get the singleton instance of BrMgr.
  // @return: Reference to the singleton BrMgr instance.
  static BrMgr& GetInstance() {
    static BrMgr instance;
    return instance;
  }

 private:
  // Private constructor to enforce singleton pattern.
  BrMgr() = default;
  // Private destructor.
  ~BrMgr() = default;

  // Cleanup resources used by the BRPC service.
  void Cleanup();

  // Member variables

  // Unique pointer to the RPC stub cache.
  std::unique_ptr<BrpcStubCache> brpc_stub_cache_;

  // Unique pointer to the data stream manager.
  std::unique_ptr<DataStreamMgr> data_stream_mgr_;

  // Unique pointer to the priority thread pool for query RPC.
  std::unique_ptr<PriorityThreadPool> query_rpc_pool_;

  // Unique pointer to the BRPC server instance.
  std::unique_ptr<brpc::Server> brpc_server_;

  // Network address of the BRPC service.
  TNetworkAddress address_;

  // Cluster ID string.
  std::string cluster_id_;

  // Unique pointer to the authenticator.
  std::unique_ptr<BoxAuthenticator> auth_;
};

}  // namespace kwdbts
