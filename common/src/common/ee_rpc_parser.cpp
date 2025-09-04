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
// Created by liguoliang on 2022/07/18.

#include "ee_rpc_parser.h"


namespace kwdbts {

bool BrpcMessage::InitBrpcSpec(const TSProcessorSpec &tsProcessorSpec) {
  InitInputSyncSpec(tsProcessorSpec);
  InitOutputRouterSpec(tsProcessorSpec);

  return true;
}

void BrpcMessage::InitInputSyncSpec(const TSProcessorSpec &tsProcessorSpec) {
  k_uint32 input_size = tsProcessorSpec.input_size();
  for (k_uint32 i = 0; i < input_size; ++i) {
    const TSInputSyncSpec& input = tsProcessorSpec.input(i);
    InputSyncSpec input_spec;
    input_spec.type_ = input.type();
    k_uint32 stream_size = input.streams_size();
    for (k_uint32 j = 0; j < stream_size; ++j) {
      const TSStreamEndpointSpec& stream = input.streams(j);
      if (kwdbts::StreamEndpointType::REMOTE == stream.type()) {
        is_remote_input_ = true;
        StreamInfo info;
        info.endpoint_type_ = stream.type();
        info.stream_id_ = stream.stream_id();
        info.target_node_id_ = stream.target_node_id();
        info.dest_processor_id_ = stream.dest_processor();
        input_spec.stream_info_vec_.push_back(info);
      } else {
        is_local_input_ = true;
        continue;
      }
    }

    if (input_spec.stream_info_vec_.empty()) {
      continue;
    }

    if (input.has_ordering()) {
      const kwdbts::TSOrdering &order = input.ordering();
      k_uint32 order_size_ = order.columns_size();
      for (k_uint32 i = 0; i < order_size_; i++) {
        if (kwdbts::TSOrdering_Column_Direction::
              TSOrdering_Column_Direction_ASC == order.columns(i).direction()) {
          input_spec.order_info_.asc_order_.push_back(true);
        } else {
          input_spec.order_info_.asc_order_.push_back(false);
        }
        input_spec.order_info_.null_first_.push_back(true);
        input_spec.order_info_.order_column_ids_.push_back(order.columns(i).col_idx());
      }
    }

    remote_spec_vec_.push_back(input_spec);
  }
}

void BrpcMessage::InitOutputRouterSpec(const TSProcessorSpec &tsProcessorSpec) {
  k_uint32 output_size = tsProcessorSpec.output_size();
  for (k_uint32 i = 0; i < output_size; ++i) {
    const TSOutputRouterSpec& output = tsProcessorSpec.output(i);
    OutputRouterSpec output_spec;
    output_spec.type_ = output.type();
    k_uint32 stream_size = output.streams_size();
    for (k_uint32 j = 0; j < stream_size; ++j) {
      const TSStreamEndpointSpec& stream = output.streams(j);
      if (kwdbts::StreamEndpointType::REMOTE == stream.type()) {
        is_routor_output_ = true;
        StreamInfo info;
        info.endpoint_type_ = stream.type();
        info.stream_id_ = stream.stream_id();
        info.target_node_id_ = stream.target_node_id();
        info.dest_processor_id_ = stream.dest_processor();
        output_spec.stream_info_vec_.push_back(info);
      } else {
        is_local_output_ = false;
        continue;
      }
    }

    if (output_spec.stream_info_vec_.empty()) {
      continue;
    }

    for (k_uint32 i = 0; i < output.hash_columns_size(); ++i) {
      output_spec.group_cols_.push_back(output.hash_columns(i));
    }

    routor_spec_vec_.push_back(output_spec);
  }
}

// bool BrpcMessage::InitStreamRecvr() {
//   BrpcInfo brpc_info;
//   query_id_ = brpc_info.query_id_;
//   if (KStatus::SUCCESS !=
//       BrMgr::GetInstance().GetDataStreamMgr()->PreparePassThroughChunkBuffer(query_id_)) {
//     return false;
//   }
//   stream_recvr_ = BrMgr::GetInstance().GetDataStreamMgr()->CreateRecvr(
//       query_id_, dest_processor_id_, stream_size_, EXCHG_NODE_BUFFER_SIZE_BYTES,
//       asc_order_.empty() ? false : true, asc_order_.empty() ? false : true);
//   if (stream_recvr_ == nullptr) {
//     LOG_ERROR("CreateRecvr faild");
//     Return(EEIteratorErrCode::EE_ERROR);
//   }
//   stream_recvr_->SetReceiveNotify(
//       [this](k_int32 nodeid, k_int32 code, const std::string& msg) {
//         this->ReceiveChunkNotify(nodeid, code, msg);
//       });

//   status_.set_status_code(0);
//   status_.add_error_msgs("");
// }


void RpcSpecResolve::BuildRpcSpec(const TSProcessorSpec &tsProcessorSpec) {
  k_uint32 input_size = tsProcessorSpec.input_size();
  input_specs_.reserve(input_size);
  for (k_uint32 i = 0; i < input_size; ++i) {
    const TSInputSyncSpec& input = tsProcessorSpec.input(i);
    input_specs_.push_back(const_cast<TSInputSyncSpec*>(&input));
    k_uint32 stream_size = input.streams_size();
    for (k_uint32 j = 0; j < stream_size; ++j) {
      const TSStreamEndpointSpec& stream = input.streams(j);
      input_map_.emplace(stream.stream_id(), const_cast<TSInputSyncSpec*>(&input));
      input_ids_.emplace(stream.stream_id());
    }
  }

  k_uint32 output_size = tsProcessorSpec.output_size();
  for (k_uint32 i = 0; i < output_size; ++i) {
    const TSOutputRouterSpec& output = tsProcessorSpec.output(i);
    output_specs_.push_back(const_cast<TSOutputRouterSpec*>(&output));
    k_uint32 stream_size = output.streams_size();
    for (k_uint32 j = 0; j < stream_size; ++j) {
      const TSStreamEndpointSpec& stream = output.streams(j);
      output_map_.emplace(stream.stream_id(), const_cast<TSOutputRouterSpec*>(&output));
      output_ids_.emplace(stream.stream_id());
    }
  }
}

}  // namespace kwdbts
