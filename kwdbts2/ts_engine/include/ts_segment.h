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

#include <list>
#include <memory>
#include <stdexcept>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_block.h"

namespace kwdbts {

class TsSegmentBase;
// conditions used for flitering blockitem data.
struct TsBlockItemFilterParams {
  uint32_t db_id;
  TSTableID table_id;
  TSEntityID entity_id;
  const std::vector<KwTsSpan>& ts_spans_;
};

// base class for data segment
class TsSegmentBase {
 public:
  // filter blockspans that satisfied condition.
  virtual KStatus GetBlockSpans(const TsBlockItemFilterParams& filter,
                                std::list<shared_ptr<TsBlockSpan>>& block_spans) = 0;

  virtual bool MayExistEntity(TSEntityID entity_id) const { return true; }

  virtual ~TsSegmentBase() {}
};

}  // namespace kwdbts
