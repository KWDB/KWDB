// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.
#include "ee_sink_buffer.h"

#include <time.h>

#include <chrono>
#include <mutex>
#include <string_view>

#include "butil/logging.h"
#include "ee_defer.h"
#include "ee_global.h"
#include "lg_api.h"

namespace kwdbts {

#define PIPELINE_SINK_BUFFER_SIZE 64

OutboundBuffer::OutboundBuffer(const std::vector<FragmentDestination>& destinations,
                       bool is_dest_merge)
    : audit_stats_send_frequency_max_(16),
      is_dest_merge_(is_dest_merge) {
  for (const auto& dest : destinations) {
    const auto& target_node_id = dest.target_node_id;
    // instance_id == -1 indicates that the destination is pseudo for bucket
    // shuffle join.
    if (target_node_id == -1) {
      continue;
    }

    if (outbound_ctxs_.count(target_node_id) == 0) {
      outbound_ctxs_[target_node_id] = std::make_unique<OutboundContext>();
      auto& ctx = OutboundCtx(target_node_id);
      ctx.count_outbounder = 0;
      ctx.request_seq = -1;
      ctx.max_in_contiguous_ack_seq = -1;
      ctx.num_finished_rpcs = 0;
      ctx.num_in_flight_rpcs = 0;
      ctx.brpc_dest_addrs = dest.brpc_addr;
      ctx.query_id = dest.query_id;
      // LOG_ERROR("SinkBuffer::AddRequest [End], query_id: %d", dest.query_id);
    }
  }
}

OutboundBuffer::~OutboundBuffer() {
  // In some extreme cases, the pipeline driver has not been created yet, and
  // the query is over At this time, sink_buffer also needs to be able to be
  // destructed correctly
  is_completing_ = true;

  // DCHECK(IsFinished());

  outbound_ctxs_.clear();
}

void OutboundBuffer::IncrSinker() {
  uncancelled_sinker_count_++;
  for (auto& [nodeid, outbound_ctx] : outbound_ctxs_) {
    outbound_ctx->count_outbounder++;
    // LOG_ERROR("send target_id:%ld", nodeid);
  }
  unprocessed_eos_num_ += outbound_ctxs_.size();
}

KStatus OutboundBuffer::AddSendRequest(ChunkTransmitContext& request) {
  if (is_completing_) {
    LOG_ERROR("AddRequest is_completing_");
    return KStatus::SUCCESS;
  }

  {
    auto request_sequence = request_sequence_id_++;
    if (!request.transmit_params->eos() &&
        (request_sequence & (audit_stats_send_frequency_ - 1)) == 0) {
      if (audit_stats_send_frequency_ <
          audit_stats_send_frequency_max_) {
        audit_stats_send_frequency_ = audit_stats_send_frequency_ << 1;
      }
    }

    auto& context = OutboundCtx(request.target_node_id);

    if (KStatus::SUCCESS != AttemptSendRpc(request.target_node_id, [&]() {
          context.buffer.push(request);
        })) {
      LOG_ERROR("TrySendRpc fail");
      return KStatus::FAIL;
    }
    // LOG_ERROR(
    //     "SinkBuffer::AddRequest [End], query_id: %ld, "
    //     "record_processor_id: %d, sender_id: %d,request.params->eos() is %d",
    //     request.params->query_id(), request.params->dest_processor(),
    //     request.params->sender_id(), request.params->eos());
    // sleep(1);
  }

  return KStatus::SUCCESS;
}

k_bool OutboundBuffer::IsFull() const {
  // std::queue' read is concurrent safe without mutex
  // Judgement may not that accurate because we do not known in advance which
  // instance the data to be sent corresponds to
  size_t max_buffer_size = PIPELINE_SINK_BUFFER_SIZE * outbound_ctxs_.size();
  size_t buffer_size = 0;
  for (auto& [_, context] : outbound_ctxs_) {
    buffer_size += context->buffer.size();
  }
  return buffer_size > max_buffer_size;
}

k_bool OutboundBuffer::IsFinished() const {
  if (!is_completing_) {
    return false;
  }

  return sending_rpc_count_ == 0 && pending_rpc_total_ == 0;
}

k_bool OutboundBuffer::IsFinishedExtra() const {
  return (sending_rpc_count_ == 0 &&
          pending_rpc_total_ == 0) /*|| is_completing_*/;
}
void OutboundBuffer::CancelOneSinker() {
  // auto notify = this->defer_notify();
  if (--uncancelled_sinker_count_ == 0) {
    is_completing_ = true;
  }
}

void OutboundBuffer::UpdateSendWindow(const k_int64& instance_id,
                                   const k_int64 sequence) {
  // Both sender side and receiver side can tolerate disorder of tranmission
  // if receiver side is not ExchangeMergeSortSourceOperator
  if (!is_dest_merge_) {
    return;
  }
  auto& context = OutboundCtx(instance_id);
  auto& seqs = context.non_contiguous_acked_seqs;
  seqs.insert(sequence);
  auto& max_continuous_acked_seq = context.max_in_contiguous_ack_seq;
  std::unordered_set<k_int64>::iterator it;
  while ((it = seqs.find(max_continuous_acked_seq + 1)) != seqs.end()) {
    seqs.erase(it);
    ++max_continuous_acked_seq;
  }
}

KStatus OutboundBuffer::AttemptSendRpc(const k_int64& instance_id,
                               const std::function<void()>& pre_works) {
  auto& context = OutboundCtx(instance_id);
  std::lock_guard guard(context.mutex);
  pre_works();

  DeferOp decrease_defer([this]() { --sending_rpc_count_; });
  ++sending_rpc_count_;

  for (;;) {
    if (is_completing_) {
      return KStatus::SUCCESS;
    }

    auto& buffer = context.buffer;

    bool too_much_brpc_process = false;
    if (is_dest_merge_) {
      k_int64 discontinuous_acked_window_size =
          context.request_seq - context.max_in_contiguous_ack_seq;
      too_much_brpc_process = discontinuous_acked_window_size >= 64;
    } else {
      too_much_brpc_process = context.num_in_flight_rpcs >= 64;
    }
    if (buffer.empty() || too_much_brpc_process) {
      return KStatus::SUCCESS;
    }

    ChunkTransmitContext& request = buffer.front();
    bool need_wait = false;
    DeferOp pop_defer([&need_wait, &buffer]() {
      if (need_wait) {
        return;
      }
      buffer.pop();
    });

    // The order of data transmiting in IO level may not be strictly the same as
    // the order of submitting data packets
    // But we must guarantee that first packet must be received first
    if (context.num_finished_rpcs == 0 && context.num_in_flight_rpcs > 0) {
      need_wait = true;
      return KStatus::SUCCESS;
    }
    if (request.transmit_params->eos()) {
      DeferOp eos_defer([this, &instance_id, &need_wait]() {
        if (need_wait) {
          return;
        }
        if (--unprocessed_eos_num_ == 0) {
          is_completing_ = true;
        }
        OutboundCtx(instance_id).count_outbounder--;
      });
      if (context.count_outbounder > 1) {
        // if (request.params->chunks_size() == 0) {
        //     continue;
        // } else {
        //     request.params->set_eos(false);
        // }
      } else {
        // The order of data transmiting in IO level may not be strictly the
        // same as the order of submitting data packets But we must guarantee
        // that eos packent must be the last packet
        if (context.num_in_flight_rpcs > 0) {
          need_wait = true;
          return KStatus::SUCCESS;
        }
      }
    }

    request.transmit_params->set_query_id(context.query_id);
    request.transmit_params->set_sequence(++context.request_seq);
    auto* closure = new DisposableClosure<PTransmitChunkResult, ClosureContext>(
        {instance_id, request.transmit_params->sequence(), MonotonicNanos()});

    closure->AddFailedHandler([this](const ClosureContext& ctx, std::string_view rpc_error_msg) noexcept {
      auto defer = DeferOp([this, ctx, rpc_error_msg]() {
        if (notify_rpc_callback_) {
          notify_rpc_callback_(ctx.instance_id, ERRCODE_INTERNAL_ERROR, std::string(rpc_error_msg));
        }
        --pending_rpc_total_;
        if (notify_callback_) {
          notify_callback_();
        }
      });
      is_completing_ = true;
      auto& context = OutboundCtx(ctx.instance_id);
      ++context.num_finished_rpcs;
      --context.num_in_flight_rpcs;
      const auto& dest_addr = context.brpc_dest_addrs;
      LOG_ERROR(
          "transmit chunk rpc failed [target_node_id={%ld}] "
          "[dest={%s}:{%d}] detail:{%s}",
          ctx.instance_id, dest_addr.hostname_.c_str(), dest_addr.port_,
          rpc_error_msg.data());
    });
    closure->AddSuccessHandler(
        [this](const ClosureContext& ctx,
               const PTransmitChunkResult& result) noexcept {
          // auto notify = this->defer_notify();
          StatusPB status(result.status());
          auto defer = DeferOp([this, ctx, status]() {
            if (0 != status.status_code() && notify_rpc_callback_) {
              std::string msg = status.error_msgs(0);
              notify_rpc_callback_(ctx.instance_id, status.status_code(), msg);
            }
            --pending_rpc_total_;
            if (notify_callback_) {
              notify_callback_();
            }
          });

          auto& context = OutboundCtx(ctx.instance_id);
          ++context.num_finished_rpcs;
          --context.num_in_flight_rpcs;

          if (0 != status.status_code()) {
            is_completing_ = true;
            const auto& dest_addr = context.brpc_dest_addrs;
            LOG_ERROR(
                "transmit chunk rpc failed [target_node_id={%ld}] "
                "[dest={%s}:{%d}] detail:{%s}",
                ctx.instance_id, dest_addr.hostname_.c_str(), dest_addr.port_,
                status.error_msgs(0).c_str());
          } else {
            static_cast<void>(AttemptSendRpc(ctx.instance_id, [&]() {
              UpdateSendWindow(ctx.instance_id, ctx.seq);
            }));
          }
        });

    ++pending_rpc_total_;
    ++context.num_in_flight_rpcs;

    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(3000 * 1000);

    KStatus st;
    if (bthread_self()) {
      st = SendDataViaRpc(closure, request);
    } else {
      st = SendDataViaRpc(closure, request);
    }
    return st;
  }
  return KStatus::SUCCESS;
}

KStatus OutboundBuffer::SendDataViaRpc(
    DisposableClosure<PTransmitChunkResult, ClosureContext>* closure,
    const ChunkTransmitContext& request) {
  closure->cntl.request_attachment().append(request.attachment_data);
  request.brpc_service_stub->TransmitChunk(&closure->cntl, request.transmit_params.get(), &closure->result, closure);

  return KStatus::SUCCESS;
}
KStatus OutboundBuffer::SendErrMsg(TransmitSimpleInfo& request) {
  auto& context = OutboundCtx(request.target_node_id);
  std::lock_guard guard(context.mutex);

  DeferOp decrease_defer([this]() { --sending_rpc_count_; });
  ++sending_rpc_count_;

  for (;;) {
    auto* closure =
        new DisposableClosure<PSendExecStatusResult, ClosureContext>(
            {request.target_node_id, 0, MonotonicNanos()});

    closure->AddFailedHandler([this](const ClosureContext& ctx, std::string_view rpc_error_msg) noexcept {
      auto defer = DeferOp([this, ctx, rpc_error_msg]() {
        if (notify_rpc_callback_) {
          notify_rpc_callback_(ctx.instance_id, ERRCODE_INTERNAL_ERROR, std::string(rpc_error_msg));
        }
        --pending_rpc_total_;
        if (notify_callback_) {
          notify_callback_();
        }
      });
    });
    closure->AddSuccessHandler(
        [this](const ClosureContext& ctx,
               const PSendExecStatusResult& result) noexcept {
          StatusPB status(result.status());
          auto defer = DeferOp([this, ctx, status]() {
            if (0 != status.status_code() && notify_rpc_callback_) {
              std::string msg = status.error_msgs(0);
              notify_rpc_callback_(ctx.instance_id, status.status_code(), msg);
            }
            --pending_rpc_total_;
            if (notify_callback_) {
              notify_callback_();
            }
          });
        });

    ++pending_rpc_total_;
    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(300 * 1000);
    // closure->cntl.request_attachment().append(request.attachment);
    request.brpc_stub->SendExecStatus(&closure->cntl, request.exec_status.get(),
                                      &closure->result, closure);

    return KStatus::SUCCESS;
  }
  return KStatus::SUCCESS;
}

KStatus OutboundBuffer::SendSimpleMsg(TransmitSimpleInfo& request) {
  auto& context = OutboundCtx(request.target_node_id);
  std::lock_guard guard(context.mutex);

  DeferOp decrease_defer([this]() { --sending_rpc_count_; });
  ++sending_rpc_count_;

  for (;;) {
    auto* closure = new DisposableClosure<PDialDataRecvrResult, ClosureContext>(
        {request.target_node_id, 0, MonotonicNanos()});

    closure->AddFailedHandler([this](const ClosureContext& ctx, std::string_view rpc_error_msg) noexcept {
      auto defer = DeferOp([this, ctx, rpc_error_msg]() {
        if (notify_rpc_callback_) {
          notify_rpc_callback_(ctx.instance_id, ERRCODE_INTERNAL_ERROR, std::string(rpc_error_msg));
        }
        --pending_rpc_total_;
        if (notify_callback_) {
          notify_callback_();
        }
      });
    });
    closure->AddSuccessHandler([this](const ClosureContext& ctx, const PDialDataRecvrResult& result) noexcept {
      // auto notify = this->defer_notify();
      StatusPB status(result.status());
      auto defer = DeferOp([this, ctx, status]() {
        if (0 != status.status_code() && notify_rpc_callback_) {
          std::string msg = status.error_msgs(0);
          notify_rpc_callback_(ctx.instance_id, status.status_code(), msg);
        }
        --pending_rpc_total_;
        if (notify_callback_) {
          notify_callback_();
        }
      });
    });

    ++pending_rpc_total_;
    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(300 * 1000);
    // closure->cntl.request_attachment().append(request.attachment);

    request.brpc_stub->DialDataRecvr(&closure->cntl, request.params.get(),
                                     &closure->result, closure);

    return KStatus::SUCCESS;
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
