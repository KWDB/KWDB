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

#include "br_internal_service.h"

#include <brpc/closure_guard.h>

#include "br_global.h"
#include "br_mgr.h"
#include "br_priority_thread_pool.h"
#include "ee_defer.h"
namespace kwdbts {

template <typename T>
void BoxServiceBase<T>::DialDataRecvrInternal(::google::protobuf::RpcController* controller,
                                              ::google::protobuf::Closure* done,
                                              const ::kwdbts::PDialDataRecvr* request,
                                              ::kwdbts::PDialDataRecvrResult* response) {
  brpc::ClosureGuard closure_guard(done);

  BRStatus st = BrMgr::GetInstance().GetDataStreamMgr()->DialDataRecvr(*request);
  st.toProtobuf(response->mutable_status());
}

template <typename T>
void BoxServiceBase<T>::TransmitChunkInternal(google::protobuf::RpcController* controller,
                                              ::google::protobuf::Closure* done,
                                              const PTransmitChunkParams* request,
                                              PTransmitChunkResult* response) {
  class WrapClosure : public google::protobuf::Closure {
   public:
    WrapClosure(google::protobuf::Closure* done, PTransmitChunkResult* response)
        : done_(done), response_(response) {}
    ~WrapClosure() override = default;
    void Run() override {
      std::unique_ptr<WrapClosure> self_guard(this);
      if (done_ != nullptr) {
        done_->Run();
      }
    }

   private:
    google::protobuf::Closure* done_;
    PTransmitChunkResult* response_;
  };

  google::protobuf::Closure* wrapped_done = KNEW WrapClosure(done, response);
  if (wrapped_done == nullptr) {
    LOG_ERROR("new WrapClosure failed");
    return;
  }

  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto* req = const_cast<PTransmitChunkParams*>(request);
  BRStatus st;
  st.toProtobuf(response->mutable_status());

  DeferOp defer([&]() {
    if (st.code() != BRStatusCode::OK) {
      LOG_ERROR("TransmitChunk failed, status: %s", std::string(st.message()).c_str());
    }
    if (wrapped_done != nullptr) {
      st.toProtobuf(response->mutable_status());
      wrapped_done->Run();
    }
  });

  if (cntl->request_attachment().size() > 0) {
    butil::IOBuf& io_buf = cntl->request_attachment();
    for (k_size_t i = 0; i < req->chunks().size(); ++i) {
      auto chunk = req->mutable_chunks(i);
      if (UNLIKELY(io_buf.size() < chunk->data_size())) {
        std::ostringstream oss;
        oss << "iobuf's size " << io_buf.size() << " < " << chunk->data_size();
        auto msg = oss.str();
        LOG_ERROR("%s", msg.c_str());
        st = BRStatus::InternalError(msg);
        return;
      }
      // also with copying due to the discontinuous memory in chunk
      auto size = io_buf.cutn(chunk->mutable_data(), chunk->data_size());
      if (UNLIKELY(size != chunk->data_size())) {
        std::ostringstream oss;
        oss << "size(" << size << ") != chunk->data_size()(" << chunk->data_size() << ")";
        auto msg = oss.str();
        LOG_ERROR("%s", msg.c_str());
        st = BRStatus::InternalError(msg);
        return;
      }
    }
  }

  st = BrMgr::GetInstance().GetDataStreamMgr()->TransmitChunk(*request, &wrapped_done);
}

template <typename T>
void BoxServiceBase<T>::SendExecStatusInternal(::google::protobuf::RpcController* controller,
                                               ::google::protobuf::Closure* done,
                                               const ::kwdbts::PSendExecStatus* request,
                                               ::kwdbts::PSendExecStatusResult* response) {
  brpc::ClosureGuard closure_guard(done);

  BRStatus st = BrMgr::GetInstance().GetDataStreamMgr()->SendExecStatus(*request);
  st.toProtobuf(response->mutable_status());
}

template <typename T>
void BoxServiceBase<T>::DialDataRecvr(::google::protobuf::RpcController* controller,
                                      const ::kwdbts::PDialDataRecvr* request,
                                      ::kwdbts::PDialDataRecvrResult* response,
                                      ::google::protobuf::Closure* done) {
  this->DialDataRecvrInternal(controller, done, request, response);
}

template <typename T>
void BoxServiceBase<T>::TransmitChunk(google::protobuf::RpcController* controller,
                                      const PTransmitChunkParams* request,
                                      PTransmitChunkResult* response,
                                      google::protobuf::Closure* done) {
  auto task = [=]() { this->TransmitChunkInternal(controller, done, request, response); };

  if (!BrMgr::GetInstance().GetPriorityThreadPool()->TryOffer(std::move(task))) {
    brpc::ClosureGuard closure_guard(done);
    LOG_ERROR("submit transmit_chunk task failed");
    BRStatus::ServiceUnavailable("submit transmit_chunk task failed")
        .toProtobuf(response->mutable_status());
    return;
  }
}

template <typename T>
void BoxServiceBase<T>::SendExecStatus(::google::protobuf::RpcController* controller,
                                       const ::kwdbts::PSendExecStatus* request,
                                       ::kwdbts::PSendExecStatusResult* response,
                                       ::google::protobuf::Closure* done) {
  this->SendExecStatusInternal(controller, done, request, response);
}

template class BoxServiceBase<BoxService>;

}  // namespace kwdbts
