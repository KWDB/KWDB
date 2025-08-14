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
#include <mutex>
#include <string>
#include <vector>

#include "br_internal_service_recoverable_stub.h"
#include "br_network_util.h"
#include "br_spinlock.h"
#include "lg_api.h"

namespace kwdbts {

class BrpcStubCache {
 private:
  struct StubPool {
    StubPool() { stubs_.reserve(1); }

    std::shared_ptr<BoxServiceRetryableClosureStub> GetOrCreate(const butil::EndPoint& endpoint) {
      if (stubs_.empty()) {
        auto stub = std::make_shared<BoxServiceRetryableClosureStub>(endpoint);
        if (stub->ResetChannel() != KStatus::SUCCESS) {
          return nullptr;
        }
        stubs_.push_back(stub);
        idx_ = 0;
        return stub;
      }
      if (++idx_ >= static_cast<k_int64>(stubs_.size())) {
        idx_ = 0;
      }
      return stubs_[idx_];
    }

    std::vector<std::shared_ptr<BoxServiceRetryableClosureStub>> stubs_;
    k_int64 idx_ = -1;
  };

  SpinLock lock_;
  butil::FlatMap<butil::EndPoint, StubPool*> stub_map_;

 public:
  BrpcStubCache() { stub_map_.init(239); }

  BrpcStubCache(const BrpcStubCache&) = delete;
  BrpcStubCache& operator=(const BrpcStubCache&) = delete;

  ~BrpcStubCache() {
    for (auto& entry : stub_map_) {
      delete entry.second;
    }
  }

  std::shared_ptr<BoxServiceRetryableClosureStub> GetStub(const TNetworkAddress& taddr) {
    return GetStub(taddr.hostname_, taddr.port_);
  }

  std::shared_ptr<BoxServiceRetryableClosureStub> GetStub(const std::string& host, k_int32 port) {
    butil::EndPoint endpoint;
    std::string real_host = host;
    if (!IsValidIp(host)) {
      KStatus status = HostnameToIp(host, real_host);
      if (status != KStatus::SUCCESS) {
        return nullptr;
      }
    }
    std::string brpc_url = GetHostPort(real_host, port);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
      LOG_ERROR("unknown endpoint, host=%s, port=%d", host.c_str(), port);
      return nullptr;
    }
    return GetStub(endpoint);
  }

  std::shared_ptr<BoxServiceRetryableClosureStub> GetStub(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> lock(lock_);
    auto stub_pool_ptr = stub_map_.seek(endpoint);
    if (stub_pool_ptr == nullptr) {
      auto* pool = new StubPool();
      stub_map_.insert(endpoint, pool);
      return pool->GetOrCreate(endpoint);
    }
    return (*stub_pool_ptr)->GetOrCreate(endpoint);
  }
};

}  // namespace kwdbts
