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

#include "ee_hash_tag_row_batch.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_table.h"

namespace kwdbts {

void HashTagRowBatch::Init(TABLE *table) {
  TagRowBatch::Init(table);
  rel_col_num_ = table_->scan_rel_cols_.size();
}

char *HashTagRowBatch::GetData(k_uint32 tagIndex, k_uint32 offset,
                           roachpb::KWDBKTSColumn::ColumnType ctype,
                           roachpb::DataType dt) {
  tagIndex -= rel_col_num_;
  return TagRowBatch::GetData(tagIndex, offset, ctype, dt);
}
k_uint16 HashTagRowBatch::GetDataLen(k_uint32 tagIndex, k_uint32 offset,
                    roachpb::KWDBKTSColumn::ColumnType ctype) {
  tagIndex -= rel_col_num_;
  return TagRowBatch::GetDataLen(tagIndex, offset, ctype);
}

bool HashTagRowBatch::IsNull(k_uint32 tagIndex,
                         roachpb::KWDBKTSColumn::ColumnType ctype) {
  tagIndex -= rel_col_num_;
  return TagRowBatch::IsNull(tagIndex, ctype);
}

}  // namespace kwdbts
