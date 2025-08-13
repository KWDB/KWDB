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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "br_internal_service.pb.h"
#include "br_mgr.h"
#include "br_network_util.h"
#include "br_stub_cache.h"
#include "ee_block_compress.h"
#include "ee_operator_type.h"
#include "ee_outbound_op.h"
#include "ee_pb_plan.pb.h"
#include "ee_sink_buffer.h"
#include "ee_global.h"
namespace kwdbts {

class OutboundBuffer;

class RouterOutboundOperator : public OutboundOperator {
 public:
  // using OutboundOperator::OutboundOperator;
  class Channel {
   public:
    Channel(RouterOutboundOperator* parent, const TNetworkAddress& brpc_dest, const k_int64& query_id,
            k_int32 dest_processor_id, k_int32 target_node_id, k_int32 stream_id,
            PassThroughChunkBuffer* pass_through_chunk_buffer)
        : parent_(parent),
          rpc_dest_addr_(brpc_dest),
          query_id_(query_id),
          dest_processor_id_(dest_processor_id),
          target_node_id_(target_node_id),
          stream_id_(stream_id),
          pass_through_context_(pass_through_chunk_buffer, query_id, dest_processor_id_) {
    }
    // ~Channel() = default;
    KStatus Init();
    KStatus SendOneChunk(DataChunkPtr& chunk, k_int32 driver_sequence,
                           k_bool eos);
    KStatus SendOneChunk(DataChunkPtr& chunk, k_int32 driver_sequence,
                           k_bool eos, k_bool* is_real_sent);
    KStatus SendChunkRequest(PTransmitChunkParamsPtr chunk_request,
                             const butil::IOBuf& attachment,
                             k_int64 attachment_physical_bytes);
    KStatus CheckRecvrReady();
    k_bool CheckPassThrough();
    k_bool IsLocal();
    k_int32 GetQueryId() { return query_id_; }
    KStatus AddRowsSelective(DataChunk* chunk, k_int32 driver_sequence,
                             const k_uint32* row_indexes, k_uint32 size);
    k_bool UsePassThrougth() { return use_pass_through_; }
    KStatus CloseInternal();
    KStatus SendFinish();
    KStatus Close();
    void SetType(::kwdbts::StreamEndpointType type) { type_ = type; }
    void SetStatus(k_int32 code, const std::string& msg) {
      status_.set_status_code(code);
      status_.set_error_msgs(0, msg);
    }
    void SetSendLastChunk(k_bool send_last_chunk) {
      is_send_last_chunk_ = send_last_chunk;
    }

    k_int32 GetDestProcessorid() { return dest_processor_id_; }

   public:
    KStatus SendError(k_int32 error_code, const std::string& error_msg);

   private:
    RouterOutboundOperator* parent_;
    k_bool is_inited_{false};
    k_int32 dest_processor_id_{-1};
    k_int64 query_id_{-1};
    k_int32 target_node_id_{-1};
    k_int32 stream_id_{-1};
    TNetworkAddress rpc_dest_addr_;
    k_bool use_pass_through_ = false;
    std::vector<std::unique_ptr<DataChunk>> chunks_;
    PTransmitChunkParamsPtr chunk_request_;
    size_t current_request_bytes_ = 0;
    std::shared_ptr<BoxServiceRetryableClosureStub> brpc_stub_ = nullptr;
    ::kwdbts::StreamEndpointType type_ = ::kwdbts::StreamEndpointType::LOCAL;
    PassThroughContext pass_through_context_;
    StatusPB status_;
    k_int32 send_count_ = 0;
    k_bool is_first_chunk_{true};
    k_bool is_send_last_chunk_{false};
  };

  RouterOutboundOperator(TsFetcherCollection* collection,
                         TSOutputRouterSpec* spec, TABLE* table);
  ~RouterOutboundOperator();

  KStatus PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                    EEIteratorErrCode code = EEIteratorErrCode::EE_OK) override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  BaseOperator* Clone() override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  void PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                  const EEPgErrorInfo& pgInfo) override;

  int64_t ConstrucBrpcAttachment(const PTransmitChunkParamsPtr& _chunk_request,
                                 butil::IOBuf& attachment);
  KStatus SerializeChunk(DataChunk* src, ChunkPB* dst, k_bool* is_first_chunk);
  enum OperatorType Type() override {
    return OperatorType::OPERATOR_REMOTR_OUT_BOUND;
  }
  KStatus SetFinishing() override;

  k_bool HasOutput() override { return true; };
  k_bool NeedInput() override { return true; };
  k_bool IsFinished() override { return true; }

  // BaseOperator* Clone() override;
  void ReceiveNotify(k_int32 nodeid = 0, k_int32 code = 0, const std::string& msg = "") {
    {
      std::unique_lock l(lock_);
      if (code > 0) {
        status_.set_status_code(code);
        status_.set_error_msgs(0, msg);
      }
    }
  }

  void ReceiveNotifyEx() {
    wait_cond_.notify_one();
  }

 private:
  k_bool CheckReady(kwdbContext_p ctx);
  k_bool SendErrorMessage(k_int32 error_code, const std::string& error_msg);
  KStatus SendLastChunk();

 private:
  std::string host_name_;
  k_int32 port_{8060};
  std::vector<k_int32> channel_indices_;
  static const int32_t DEFAULT_DRIVER_SEQUENCE = 0;
  static const int32_t max_transmit_batched_bytes_ = 262144;
  size_t current_request_bytes_{0};
  int32_t curr_channel_idx_{0};
  k_bool is_first_chunk_{true};
  PTransmitChunkParamsPtr chunk_request_;
  std::shared_ptr<OutboundBuffer> buffer_;
  k_int32 sender_id_{0};
  k_int32 be_number_ = 0;
  BaseOperator* input_{nullptr};  // input iterator
  std::map<k_int32, std::unique_ptr<Channel>> target_id2channel_;
  std::map<k_int32, StatusPB> channel_status_;
  std::vector<Channel*> channels_;
  mutable std::mutex lock_;
  std::condition_variable wait_cond_;
  EEPgErrorInfo pg_info_;
  StatusPB status_;
  k_bool is_real_finished_ = false;
  // k_bool is_ready_ = false;
  k_bool is_tp_stop_{false};
  CompressionTypePB compress_type_ = CompressionTypePB::NO_COMPRESSION;
  const BlockCompressor* compress_codec_ = nullptr;
  std::string compression_scratch_;
  //   std::atomic<StatusPB*> status_{nullptr};
  // StatusPB status_1_;
};

}  // namespace kwdbts
