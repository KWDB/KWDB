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

#include "ee_scan_row_batch.h"

namespace kwdbts {
char* ScanRowBatch::GetData(k_uint32 col, k_uint32 offset, roachpb::KWDBKTSColumn::ColumnType ctype,
                            roachpb::DataType dt) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return static_cast<char*>(tagdata_[col].tag_data);
  } else {
    if (dt == roachpb::VARCHAR || dt == roachpb::NVARCHAR || dt == roachpb::VARBINARY) {
      return static_cast<char *>(res_.data[col][current_batch_no_]->getVarColData(
          current_batch_line_));
    } else {
      return static_cast<char*>(res_.data[col][current_batch_no_]->mem) +
             current_batch_line_ * offset;
    }
  }
}

k_uint16 ScanRowBatch::GetDataLen(k_uint32 col, k_uint32 offset, roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return tagdata_[col].size;
  } else {
    return res_.data[col][current_batch_no_]->getVarColDataLen(
        current_batch_line_);
  }
}

k_bool ScanRowBatch::IsOverflow(k_uint32 col,
                                roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype ||
      roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return false;
  }
  return res_.data[col][current_batch_no_]->is_overflow;
}

void ScanRowBatch::Reset() {
  res_.clear();
  selection_.clear();
  is_filter_ = false;
  count_ = 0;
  effect_count_ = 0;
  current_line_ = 0;
  current_batch_line_ = 0;
  current_batch_no_ = 0;
  current_batch_count_ = -1;
}

/**
 * rows count
 */
k_uint32 ScanRowBatch::Count() {
  if (is_filter_) {
    return effect_count_;
  } else {
    return count_;
  }
}

/**
 *  nextline
 */
k_int32 ScanRowBatch::NextLine() {
  if (is_filter_) {
    if (current_line_ + 1 >= effect_count_) {
      return -1;
    }
    current_line_++;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
    return current_line_;
  } else {
    if (current_line_ + 1 >= count_) {
      return -1;
    }
    if (res_.col_num_ > 0) {
      if (current_batch_count_ == -1) {
        current_batch_count_ = res_.data[0][current_batch_no_]->count;
      }
      if (current_batch_line_ < (current_batch_count_ + 1)) {
        current_batch_line_++;
      } else {
        current_batch_no_++;
        current_batch_line_ = 0;
        current_batch_count_ = res_.data[0][current_batch_no_]->count;
      }
    }
    current_line_++;
    return current_line_;
  }
}

/**
 *  ResetLine
 */
void ScanRowBatch::ResetLine() {
  if (effect_count_ > 0) {
    is_filter_ = true;
    current_line_ = 0;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    current_batch_no_ = 0;
    current_batch_line_ = 0;
    current_line_ = 0;
  }
}

bool ScanRowBatch::IsNull(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    if (ctype == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      return false;
    }
    if (tagdata_[col].is_null) {
      return true;
    }
    return false;
  } else {
    bool is_null = false;
    res_.data[col][current_batch_no_]->isNull(current_batch_line_, &is_null);
    return is_null;
  }
}

void ScanRowBatch::CopyColumnData(k_uint32 col_idx, char* dest, k_uint32 data_len,
                                  roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    auto src = static_cast<char*>(tagdata_[col_idx].tag_data);
    if (src == nullptr) {
      return;
    }
    for (int row = 0; row < Count(); row++) {
      memcpy(dest + row * data_len, src, data_len);
    }
  } else {
    k_uint32 offset = 0;
    for (auto& batch : res_.data[col_idx]) {
      k_uint32 total_len = batch->count * data_len;
      if (batch->mem == nullptr) {
        offset += total_len;
        continue;
      }
      memcpy(dest + offset, batch->mem, total_len);
    }
  }
}

bool ScanRowBatch::SetCurrentLine(k_int32 line) {
  if (is_filter_) {
    if (line >= effect_count_) {
      return false;
    }
    current_line_ = line;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    if (line >= count_) {
      return false;
    }
    if (res_.col_num_ > 0) {
      k_int32 tmp = 0;
      for (k_int32 i = 0; i < res_.data[0].size(); i++) {
        auto batch = res_.data[0][i];
        tmp = tmp + batch->count;
        if (line < tmp) {
          current_batch_no_ = i;
          current_batch_line_ = line - (tmp - batch->count);
          current_batch_count_ = res_.data[0][current_batch_no_]->count;
          break;
        }
      }
    }
    current_line_ = line;
  }
  return true;
}

/**
 *  nextline
 */
k_int32 ReverseScanRowBatch::NextLine() {
  if (is_filter_) {
    if (current_line_ + 1 >= effect_count_) {
      return -1;
    }
    current_line_++;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
    return current_line_;
  } else {
    if (current_line_ + 1 >= count_) {
      return -1;
    }
    if (res_.col_num_ > 0) {
      if (current_batch_line_ > 0) {
        current_batch_line_--;
      } else {
        current_batch_no_++;
        current_batch_line_ = res_.data[0][current_batch_no_]->count - 1;
      }
    }
    current_line_++;
    return current_line_;
  }
}

/**
 *  ResetLine
 */
void ReverseScanRowBatch::ResetLine() {
  if (effect_count_ > 0) {
    is_filter_ = true;
    current_line_ = 0;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    current_batch_no_ = 0;
    current_batch_line_ = 0;
    if (res_.col_num_ > 0) {
      current_batch_line_ = res_.data[0][current_batch_no_]->count - 1;
    }
    current_line_ = 0;
  }
}

void ReverseScanRowBatch::CopyColumnData(k_uint32 col_idx, char* dest, k_uint32 data_len,
                                  roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    auto src = static_cast<char*>(tagdata_[col_idx].tag_data);
    if (src == nullptr) {
      return;
    }
    for (int row = 0; row < count_; row++) {
      memcpy(dest + row * data_len, src, data_len);
    }
  } else {
    k_uint32 offset = 0;
    for (auto& batch : res_.data[col_idx]) {
      k_uint32 total_len = batch->count * data_len;
      if (batch->mem == nullptr) {
        offset += total_len;
        continue;
      }
      char* mem = static_cast<char*>(batch->mem);
      for (k_int32 i = batch->count - 1; i >= 0; i--) {
        memcpy(dest + offset, mem + i * data_len, data_len);
        offset = offset + data_len;
      }
    }
  }
}

bool ReverseScanRowBatch::SetCurrentLine(k_int32 line) {
  if (is_filter_) {
    if (line >= effect_count_) {
      return false;
    }
    current_line_ = line;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    if (line >= count_) {
      return false;
    }
    if (res_.col_num_ > 0) {
      k_int32 tmp = 0;
      for (k_int32 i = 0; i < res_.data[0].size(); i++) {
        auto batch = res_.data[0][i];
        tmp = tmp + batch->count;
        if (line < tmp) {
          current_batch_no_ = i;
          current_batch_line_ = tmp - line - 1;
          current_batch_count_ = res_.data[0][current_batch_no_]->count;
          break;
        }
      }
    }
    current_line_ = line;
  }
  return true;
}

}  // namespace kwdbts
