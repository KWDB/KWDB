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

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ee_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"

namespace kwdbts {

class HashTagRowBatch;
typedef std::shared_ptr<HashTagRowBatch> HashTagRowBatchPtr;

// wrap tag row batch result inside HashTagScan op for multiple model processing
class HashTagRowBatch : public TagRowBatch {
 private:
  k_uint32 rel_col_num_{0};

 public:
  void Init(TABLE *table) override;
  char *GetData(k_uint32 tagIndex, k_uint32 offset,
                roachpb::KWDBKTSColumn::ColumnType ctype,
                roachpb::DataType dt) override;
  k_uint16 GetDataLen(k_uint32 tagIndex, k_uint32 offset,
                      roachpb::KWDBKTSColumn::ColumnType ctype) override;
  bool IsNull(k_uint32 tagIndex, roachpb::KWDBKTSColumn::ColumnType ctype) override;
};

};  // namespace kwdbts
