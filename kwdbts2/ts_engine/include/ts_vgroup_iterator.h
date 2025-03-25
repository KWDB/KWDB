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

#include <map>
#include <string>
#include <cstdio>
#include <vector>

#include "ts_common.h"
#include "ts_block_segment.h"
#include "ts_iterator.h"


namespace kwdbts {

struct TsBlockIteratorParams {
  TSTableID table_id = 0;
  uint32_t table_version = 0;
  std::vector<TSEntityID> entity_ids;
  std::vector<KwTsSpan> ts_spans;  // if empty, means scan all blocks.
  std::vector<uint32_t> kw_scan_cols;  // used for resulst_type is not BLOCK_ITEM_REFER
  bool is_reversed = false;
  TsIterResultSetType resulst_type = BLOCK_ITEM_REFER;
};

class TsBlockSegmentIterator {
 private:
  const TsBlockSegment& segment_;
  const TsBlockIteratorParams& params_;
 public:
  TsBlockSegmentIterator(const TsBlockIteratorParams& param, const TsBlockSegment& segment) :
    params_(param), segment_(segment) {}

  ~TsBlockSegmentIterator();

  KStatus Init();

  uint64_t TotalRowsNum();

  KStatus Next(IterResultSet* res, bool* is_finished, timestamp64 ts = INVALID_TS);
};


}  // namespace kwdbts
