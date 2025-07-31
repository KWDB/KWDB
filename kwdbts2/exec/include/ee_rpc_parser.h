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

#include <vector>
#include <unordered_map>
#include <set>

#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"

/**
 * @brief  Operator parameter
 *
 * @author liguoliang
 */

namespace kwdbts {


class RpcSpecResolve {
 public:
  void BuildRpcSpec(const TSProcessorSpec &tsProcessorSpec);

  std::set<k_int32> input_ids_;
  std::vector<TSInputSyncSpec *>    input_specs_;
  std::unordered_map<k_int32, TSInputSyncSpec*> input_map_;  // key stream_id

  std::set<k_int32> output_ids_;
  std::vector<TSOutputRouterSpec *> output_specs_;
  std::unordered_map<k_int32, TSOutputRouterSpec *> output_map_;  // key stream_id
};


}  // namespace kwdbts

