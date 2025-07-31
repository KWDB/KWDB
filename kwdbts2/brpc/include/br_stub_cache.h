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
 public:
  BrpcStubCache() { stub_map_.init(239); }

  ~BrpcStubCache() {
    for (auto& stub : stub_map_) {
      delete stub.second;
    }
  }

  std::shared_ptr<PInternalServiceRecoverableStub> GetStub(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(lock_);
    auto stub_pool = stub_map_.seek(endpoint);
    if (stub_pool == nullptr) {
      StubPool* pool = new StubPool();
      stub_map_.insert(endpoint, pool);
      return pool->GetOrCreate(endpoint);
    }
    return (*stub_pool)->GetOrCreate(endpoint);
  }

  std::shared_ptr<PInternalServiceRecoverableStub> GetStub(const TNetworkAddress& taddr) {
    return GetStub(taddr.hostname_, taddr.port_);
  }

  std::shared_ptr<PInternalServiceRecoverableStub> GetStub(const std::string& host, k_int32 port) {
    butil::EndPoint endpoint;
    std::string realhost;
    std::string brpc_url;
    realhost = host;
    if (!IsValidIp(host)) {
      KStatus status = HostnameToIp(host, realhost);
      if (status != KStatus::SUCCESS) {
        return nullptr;
      }
    }
    brpc_url = GetHostPort(realhost, port);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
      LOG_ERROR("unknown endpoint, host=%s, port=%d", host.c_str(), port);
      return nullptr;
    }
    return GetStub(endpoint);
  }

 private:
  struct StubPool {
    // StubPool() { stubs_.reserve(config::brpc_max_connections_per_server); }
    StubPool() { stubs_.reserve(1); }

    std::shared_ptr<PInternalServiceRecoverableStub> GetOrCreate(const butil::EndPoint& endpoint) {
      // if (UNLIKELY(stubs_.size() < config::brpc_max_connections_per_server))
      // {
      if (stubs_.size() < 1) {
        auto stub = std::make_shared<PInternalServiceRecoverableStub>(endpoint);
        if (stub->ResetChannel() != KStatus::SUCCESS) {
          return nullptr;
        }
        stubs_.push_back(stub);
        return stub;
      }
      // if (++idx_ >= config::brpc_max_connections_per_server) {
      if (++idx_ >= 1) {
        idx_ = 0;
      }
      return stubs_[idx_];
    }

    std::vector<std::shared_ptr<PInternalServiceRecoverableStub>> stubs_;
    k_int64 idx_ = -1;
  };

  SpinLock lock_;
  butil::FlatMap<butil::EndPoint, StubPool*> stub_map_;
};

class HttpBrpcStubCache {
 public:
  static HttpBrpcStubCache* GetInstance() {
    static HttpBrpcStubCache cache;
    return &cache;
  }

  std::shared_ptr<PInternalServiceRecoverableStub> GetHttpStub(const TNetworkAddress& taddr) {
    butil::EndPoint endpoint;
    std::string realhost;
    std::string brpc_url;
    realhost = taddr.hostname_;
    if (!IsValidIp(taddr.hostname_)) {
      KStatus status = HostnameToIp(taddr.hostname_, realhost);
      if (status != KStatus::SUCCESS) {
        return nullptr;
      }
    }
    brpc_url = GetHostPort(realhost, taddr.port_);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
      return nullptr;
    }
    // get is exist
    std::lock_guard<SpinLock> l(lock_);
    auto stub_ptr = stub_map_.seek(endpoint);

    if (stub_ptr != nullptr) {
      return *stub_ptr;
    }
    // create
    auto stub = std::make_shared<PInternalServiceRecoverableStub>(endpoint);
    if (stub->ResetChannel("http") != KStatus::SUCCESS) {
      return nullptr;
    }
    stub_map_.insert(endpoint, stub);
    return stub;
  }

 private:
  HttpBrpcStubCache() { stub_map_.init(500); }
  HttpBrpcStubCache(const HttpBrpcStubCache& cache) = delete;
  HttpBrpcStubCache& operator=(const HttpBrpcStubCache& cache) = delete;

  SpinLock lock_;
  butil::FlatMap<butil::EndPoint, std::shared_ptr<PInternalServiceRecoverableStub>> stub_map_;
};

}  // namespace kwdbts
