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

#include <chrono>
#include <utility>

#include "br_mgr.h"
#include "lg_api.h"

namespace kwdbts {

namespace {

// Throttle channel resets so a burst of concurrent EHOSTDOWN callbacks does
// not repeatedly rebuild the same endpoint connection.
constexpr k_int64 kMinHostDownResetIntervalNs = 100 * 1000 * 1000LL;

k_int64 SteadyClockNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace

std::shared_ptr<BoxServiceRetryableClosureStub::Session>
BoxServiceRetryableClosureStub::GetSessionSnapshot() {
  std::lock_guard<std::mutex> l(mutex_);
  return session_;
}

void BoxServiceRetryableClosureStub::DialDataRecvr(::google::protobuf::RpcController* controller,
                                                   const ::kwdbts::PDialDataRecvr* request,
                                                   ::kwdbts::PDialDataRecvrResult* response,
                                                   ::google::protobuf::Closure* done) {
  auto session = GetSessionSnapshot();
  if (!session || !session->stub) {
    if (controller != nullptr) {
      controller->SetFailed("BRPC session is not initialized.");
    }
    if (done != nullptr) {
      done->Run();
    }
    return;
  }
  auto closure = new RetryableClosure(shared_from_this(), controller, done);
  session->stub->DialDataRecvr(controller, request, response, closure);
}

RetryableClosure::RetryableClosure(std::shared_ptr<kwdbts::BoxServiceRetryableClosureStub> stub,
                                   ::google::protobuf::RpcController* controller,
                                   ::google::protobuf::Closure* done)
    : stub_(std::move(stub)), controller_(controller), done_(done) {}

void RetryableClosure::Run() {
  auto* cntl = static_cast<brpc::Controller*>(controller_);
  if (cntl->Failed() && cntl->ErrorCode() == EHOSTDOWN) {
    auto st = stub_->MaybeResetChannelOnHostDown();
    if (st != KStatus::SUCCESS) {
      LOG_WARN("Fail to reset channel: %s", cntl->ErrorText().c_str());
    }
  }
  if (done_ != nullptr) {
    done_->Run();
  }
  delete this;
}

void BoxServiceRetryableClosureStub::TransmitChunk(::google::protobuf::RpcController* controller,
                                                   const ::kwdbts::PTransmitChunkParams* request,
                                                   ::kwdbts::PTransmitChunkResult* response,
                                                   ::google::protobuf::Closure* done) {
  auto session = GetSessionSnapshot();
  if (!session || !session->stub) {
    if (controller != nullptr) {
      controller->SetFailed("BRPC session is not initialized.");
    }
    if (done != nullptr) {
      done->Run();
    }
    return;
  }
  auto closure = new RetryableClosure(shared_from_this(), controller, done);
  session->stub->TransmitChunk(controller, request, response, closure);
}

KStatus BoxServiceRetryableClosureStub::ResetChannel(const std::string& protocol) {
  brpc::ChannelOptions options;
  options.connect_timeout_ms = 30000;
  if (protocol == "http") {
    options.protocol = protocol;
  } else {
    // http does not support these.
    options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    k_int64 connection_group = 0;
    {
      std::lock_guard<std::mutex> l(mutex_);
      connection_group = connection_group_++;
    }
    options.connection_group = std::to_string(connection_group);
  }
  options.max_retry = 3;
  options.auth = BrMgr::GetInstance().GetAuth();

  auto* raw_channel = new brpc::Channel();
  std::shared_ptr<brpc::Channel> channel(raw_channel);
  if (channel->Init(endpoint_, &options) != 0) {
    LOG_ERROR("Fail to init channel.\n");
    return KStatus::FAIL;
  }

  auto session = std::make_shared<Session>();
  session->channel = std::move(channel);
  session->stub = std::make_shared<BoxService_Stub>(
      session->channel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);

  {
    std::lock_guard<std::mutex> l(mutex_);
    session_ = std::move(session);
  }
  return KStatus::SUCCESS;
}

KStatus BoxServiceRetryableClosureStub::MaybeResetChannelOnHostDown() {
  const k_int64 now_ns = SteadyClockNowNs();
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (now_ns - last_host_down_reset_ns_ < kMinHostDownResetIntervalNs) {
      return KStatus::SUCCESS;
    }
    last_host_down_reset_ns_ = now_ns;
  }

  auto st = ResetChannel();
  if (st != KStatus::SUCCESS) {
    std::lock_guard<std::mutex> l(mutex_);
    last_host_down_reset_ns_ = 0;
  }
  return st;
}

void BoxServiceRetryableClosureStub::SendExecStatus(::google::protobuf::RpcController* controller,
                                                    const ::kwdbts::PSendExecStatus* request,
                                                    ::kwdbts::PSendExecStatusResult* response,
                                                    ::google::protobuf::Closure* done) {
  auto session = GetSessionSnapshot();
  if (!session || !session->stub) {
    if (controller != nullptr) {
      controller->SetFailed("BRPC session is not initialized.");
    }
    if (done != nullptr) {
      done->Run();
    }
    return;
  }
  auto closure = new RetryableClosure(shared_from_this(), controller, done);
  session->stub->SendExecStatus(controller, request, response, closure);
}

}  // namespace kwdbts
