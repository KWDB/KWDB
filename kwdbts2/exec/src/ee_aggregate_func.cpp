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

#include "ee_aggregate_func.h"

namespace kwdbts {

int AggregateFunc::isDistinct(IChunk* chunk, k_uint32 line,
                                 std::vector<roachpb::DataType>& col_types,
                                 std::vector<k_uint32>& col_lens,
                                 std::vector<k_uint32>& group_cols,
                                 k_bool *is_distinct,
                                 std::vector<bool>& group_allow_null) {
  // group col + agg col
  std::vector<k_uint32> all_cols(group_cols);
  all_cols.insert(all_cols.end(), arg_idx_.begin(), arg_idx_.end());

  if (seen_ == nullptr) {
    std::vector<roachpb::DataType> distinct_types;
    std::vector<k_uint32> distinct_lens;
    std::vector<bool> distinct_allow_null;
    for (auto& col : all_cols) {
      distinct_types.push_back(col_types[col]);
      distinct_lens.push_back(col_lens[col]);
      distinct_allow_null.push_back(group_allow_null[col]);
    }
    seen_ = KNEW LinearProbingHashTable(distinct_types, distinct_lens, 0, distinct_allow_null);
    if (seen_->Resize() < 0) {
      return -1;
    }
  }

  k_uint64 loc;
  if (seen_->FindOrCreateGroups(chunk, line, all_cols, &loc) < 0) {
    return -1;
  }

  if (seen_->IsUsed(loc)) {
    *is_distinct = false;
    return 0;
  }

  seen_->SetUsed(loc);
  seen_->CopyGroups(chunk, line, all_cols, loc);

  *is_distinct = true;
  return 0;
}

}   // namespace kwdbts
