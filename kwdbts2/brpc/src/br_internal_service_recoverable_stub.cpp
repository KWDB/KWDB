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

#include "br_internal_service_recoverable_stub.h"

#include <utility>

#include "br_mgr.h"
#include "lg_api.h"

namespace kwdbts {

class RecoverableClosure : public ::google::protobuf::Closure {
 public:
  RecoverableClosure(std::shared_ptr<kwdbts::PInternalServiceRecoverableStub> stub,
                     ::google::protobuf::RpcController* controller,
                     ::google::protobuf::Closure* done)
      : stub_(std::move(stub)), controller_(controller), done_(done) {}

  void Run() override {
    auto* cntl = static_cast<brpc::Controller*>(controller_);
    if (cntl->Failed() && cntl->ErrorCode() == EHOSTDOWN) {
      auto st = stub_->ResetChannel();
      if (st != KStatus::SUCCESS) {
        LOG_WARN("Fail to reset channel: %s", cntl->ErrorText().c_str());
      }
    }
    done_->Run();
    delete this;
  }

 private:
  std::shared_ptr<kwdbts::PInternalServiceRecoverableStub> stub_;
  ::google::protobuf::RpcController* controller_;
  ::google::protobuf::Closure* done_;
};

PInternalServiceRecoverableStub::PInternalServiceRecoverableStub(const butil::EndPoint& endpoint)
    : endpoint_(endpoint) {}

PInternalServiceRecoverableStub::~PInternalServiceRecoverableStub() = default;

KStatus PInternalServiceRecoverableStub::ResetChannel(const std::string& protocol) {
  std::lock_guard<std::mutex> l(mutex_);

  brpc::ChannelOptions options;
  options.connect_timeout_ms = 30000;
  if (protocol == "http") {
    options.protocol = protocol;
  } else {
    // http does not support these.
    options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    options.connection_group = std::to_string(connection_group_++);
  }
  options.max_retry = 3;
  options.auth = BrMgr::GetInstance().GetAuth();
  auto channel = std::make_unique<brpc::Channel>();
  if (channel->Init(endpoint_, &options) != 0) {
    LOG_ERROR("Fail to init channel.\n");
    return KStatus::FAIL;
  }
  stub_ = std::make_shared<PInternalService_Stub>(channel.release(),
                                                  google::protobuf::Service::STUB_OWNS_CHANNEL);
  return KStatus::SUCCESS;
}

void PInternalServiceRecoverableStub::DialDataRecvr(::google::protobuf::RpcController* controller,
                                                    const ::kwdbts::PDialDataRecvr* request,
                                                    ::kwdbts::PDialDataRecvrResult* response,
                                                    ::google::protobuf::Closure* done) {
  auto closure = new RecoverableClosure(shared_from_this(), controller, done);
  stub_->DialDataRecvr(controller, request, response, closure);
}

void PInternalServiceRecoverableStub::TransmitChunk(::google::protobuf::RpcController* controller,
                                                    const ::kwdbts::PTransmitChunkParams* request,
                                                    ::kwdbts::PTransmitChunkResult* response,
                                                    ::google::protobuf::Closure* done) {
  auto closure = new RecoverableClosure(shared_from_this(), controller, done);
  stub_->TransmitChunk(controller, request, response, closure);
}

void PInternalServiceRecoverableStub::SendExecStatus(::google::protobuf::RpcController* controller,
                                                     const ::kwdbts::PSendExecStatus* request,
                                                     ::kwdbts::PSendExecStatusResult* response,
                                                     ::google::protobuf::Closure* done) {
  auto closure = new RecoverableClosure(shared_from_this(), controller, done);
  stub_->SendExecStatus(controller, request, response, closure);
}

}  // namespace kwdbts
