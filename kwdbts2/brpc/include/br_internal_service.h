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

#include <brpc/controller.h>
#include <google/protobuf/service.h>

#include "br_internal_service.pb.h"
#include "br_mgr.h"
#include "kwdb_type.h"

namespace brpc {
class Controller;
}

namespace kwdbts {

template <typename T>
class PInternalServiceBase : public T {
 public:
  explicit PInternalServiceBase(BrMgr* br_mgr) : br_mgr_(br_mgr) {}
  ~PInternalServiceBase() override = default;

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
  void DialDataRecvrInternal(::google::protobuf::RpcController* controller,
                             const ::kwdbts::PDialDataRecvr* request,
                             ::kwdbts::PDialDataRecvrResult* response,
                             ::google::protobuf::Closure* done);

  void TransmitChunkInternal(::google::protobuf::RpcController* controller,
                             const ::kwdbts::PTransmitChunkParams* request,
                             ::kwdbts::PTransmitChunkResult* response,
                             ::google::protobuf::Closure* done);

  void SendExecStatusInternal(::google::protobuf::RpcController* controller,
                              const ::kwdbts::PSendExecStatus* request,
                              ::kwdbts::PSendExecStatusResult* response,
                              ::google::protobuf::Closure* done);

 protected:
  BrMgr* br_mgr_;
};

}  // namespace kwdbts
