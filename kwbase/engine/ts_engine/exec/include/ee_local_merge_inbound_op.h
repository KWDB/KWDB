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

#include <deque>
#include <vector>

#include "ee_local_inbound_op.h"
#include "ee_operator_type.h"

namespace kwdbts {

class LocalMergeInboundOperator : public LocalInboundOperator {
 public:
  using LocalInboundOperator::LocalInboundOperator;
  virtual ~LocalMergeInboundOperator() = default;
  enum OperatorType Type() override {return OperatorType::OPERATOR_LOCAL_MERGE_IN_BOUND;}

 private:
  KStatus PullChunk(kwdbContext_p ctx, DataChunkPtr& chunk);
  KStatus CreateSortContainer(k_uint32 size, std::deque<DataChunkPtr>& buffer,
                              ColumnInfo* col_info, k_int32 col_num,
                              k_bool is_mem);
  // sort container
 private:
  DataContainerPtr container_;
};

}  // namespace kwdbts
