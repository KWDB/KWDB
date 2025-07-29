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

#include "kwdb_type.h"

namespace kwdbts {

enum class OperatorType : k_uint8 {
  OPERATOR_INVALID      = 0,

  // scan
  OPERATOR_TAG_SCAN,
  OPERATOR_HASH_TAG_SCAN,
  OPERATOR_TABLE_SCAN,
  OPERATOR_AGG_SCAN,
  OPERATOR_SORT_SCAN,
  OPERATOR_STATISTIC_SCAN,

  // agg
  OPERATOR_HASH_GROUP_BY,
  OPERATOR_SORT_GROUP_BY,
  OPERATOR_POST_AGG_SCAN,

  // srot
  OPERATOR_ORDER_BY,

  // distinct
  OPERATOR_DISTINCT,

  // noop
  OPERATOR_NOOP,
  OPERATOR_PASSTHROUGH_NOOP,

  // window
  OPERATOR_WINDOWS,

  // sampler
  OPERATOR_SAMPLER,

  // result collector
  OPERATOR_RESULT_COLLECTOR,

  // inbound
  OPERATOR_LOCAL_IN_BOUND,
  OPERATOR_REMOTE_IN_BOUND,

  // outbound
  OPERATOR_LOCAL_OUT_BOUND,
  OPERATOR_REMOTR_OUT_BOUND,

  OPERATOR_SYNCHRONIZER,
  // merge
  OPERATOR_REMOTE_MERGE_SORT_IN_BOUND,
  OPERATOR_LOCAL_MERGE_IN_BOUND
};

}   // namespace kwdbts
