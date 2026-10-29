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
