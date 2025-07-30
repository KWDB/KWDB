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

#include <brpc/channel.h>
#include <brpc/server.h>

#include <memory>
#include <mutex>
#include <string>

#include "br_internal_service.pb.h"
#include "kwdb_type.h"

namespace kwdbts {

class PInternalServiceRecoverableStub
    : public PInternalService,
      public std::enable_shared_from_this<PInternalServiceRecoverableStub> {
 public:
  explicit PInternalServiceRecoverableStub(const butil::EndPoint& endpoint);
  ~PInternalServiceRecoverableStub();

  KStatus ResetChannel(const std::string& protocol = "");

  // implements PInternalService ------------------------------------------
  void DialDataRecvr(::google::protobuf::RpcController* controller,
                     const ::kwdbts::PDialDataRecvr* request,
                     ::kwdbts::PDialDataRecvrResult* response,
                     ::google::protobuf::Closure* done) override;

  void TransmitChunk(::google::protobuf::RpcController* controller,
                     const ::kwdbts::PTransmitChunkParams* request,
                     ::kwdbts::PTransmitChunkResult* response,
                     ::google::protobuf::Closure* done) override;

  void SendExecStatus(::google::protobuf::RpcController* controller,
                      const ::kwdbts::PSendExecStatus* request,
                      ::kwdbts::PSendExecStatusResult* response,
                      ::google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<kwdbts::PInternalService_Stub> stub_;
  const butil::EndPoint endpoint_;
  k_int64 connection_group_ = 0;
  std::mutex mutex_;

  PInternalServiceRecoverableStub(const PInternalServiceRecoverableStub&) = delete;
  PInternalServiceRecoverableStub& operator=(const PInternalServiceRecoverableStub&) = delete;
};

}  // namespace kwdbts
