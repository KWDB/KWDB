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

#include "ee_tag_row_batch.h"

#include "ee_field.h"
#include "ee_table.h"

namespace kwdbts {

#define PRIMARY_TAGS_EXTERN_STORAGE_LENGTH 8

void *TagRowBatch::GetData(k_uint32 tagIndex, k_uint32 offset,
                           roachpb::KWDBKTSColumn::ColumnType ctype,
                           roachpb::DataType dt) {
  if (res_.data[tagIndex].empty()) {
    return nullptr;
  }
  if (ctype == roachpb::KWDBKTSColumn::TYPE_PTAG) {
    return static_cast<char *>(res_.data[tagIndex][current_batch_no_]->mem) +
           current_batch_line_ * (tag_offsets_[table_->scan_tags_[tagIndex] + tag_col_offset_]);
  } else {
    if ((dt == roachpb::DataType::VARCHAR) ||
        (dt == roachpb::DataType::NVARCHAR) ||
        (dt == roachpb::DataType::VARBINARY)) {
      return static_cast<char *>(
          res_.data[tagIndex][current_batch_no_]->getVarColData(
              current_batch_line_));
    }

    return static_cast<char*>(res_.data[tagIndex][current_batch_no_]->mem) +
           current_batch_line_ * (tag_offsets_[table_->scan_tags_[tagIndex] + tag_col_offset_]) + 1;
  }
  return nullptr;
}

k_uint16 TagRowBatch::GetDataLen(k_uint32 tagIndex, k_uint32 offset,
                                 roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (res_.data[tagIndex].empty()) {
    return 0;
  }

  return res_.data[tagIndex][current_batch_no_]->getVarColDataLen(
      current_batch_line_);
}

void TagRowBatch::Reset() {
  entity_indexs_.clear();
  res_.clear();
  selection_.clear();
  isFilter_ = false;
  count_ = 0;
  effect_count_ = 0;
  current_line_ = 0;
  current_entity_ = 0;
  current_batch_line_ = 0;
  current_batch_no_ = 0;
}

bool TagRowBatch::IsNull(k_uint32 tagIndex,
                         roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (ctype == roachpb::KWDBKTSColumn::TYPE_PTAG) {
    return false;
  }
  if (res_.data[tagIndex].empty()) {
    return true;
  }
  char* bitmap =
      static_cast<char*>(res_.data[tagIndex][current_batch_no_]->mem) +
      current_batch_line_ * (tag_offsets_[table_->scan_tags_[tagIndex] + tag_col_offset_]);
  if (bitmap[0] != 1) {
    return true;
  }
  return false;
}

k_uint32 TagRowBatch::NextLine() {
  if (isFilter_) {
    if (current_line_ + 1 >= effect_count_) {
      current_line_++;
      return -1;
    }
    current_line_++;
    current_entity_ = selection_[current_line_].entity_;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
    return current_line_;
  } else {
    if (current_line_ + 1 >= count_) {
      current_line_++;
      return -1;
    }
    if (res_.data.size() > 0) {
      if (current_batch_line_ + 1 < (res_.data[0][current_batch_no_]->count)) {
        current_batch_line_++;
      } else {
        current_batch_no_++;
        current_batch_line_ = 0;
      }
    }
    current_entity_++;
    current_line_++;
    return current_line_;
  }
}

KStatus TagRowBatch::NextLine(k_uint32 *line) {
  if (*(line) + 1 >= Count()) {
    *(line) = 0;
    return FAIL;
  }
  k_int32 index = *(line);
  for (auto n : pipe_entity_num_) {
    if (index >= n) {
      index -= n;
    } else if (index + 1 >= n) {
      *(line) = 0;
      return FAIL;
    } else {
      *(line) += 1;
      return SUCCESS;
    }
  }
  return FAIL;
}

void TagRowBatch::ResetLine() {
  current_line_ = 0;
  if (effect_count_ > 0) {
    isFilter_ = true;
    current_entity_ = selection_[current_line_].entity_;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    current_entity_ = 0;
    current_batch_line_ = 0;
    current_batch_no_ = 0;
  }
}

KStatus TagRowBatch::GetTagData(TagData *tagData, void **bitmap,
                                k_uint32 line) {
  if (line >= Count()) {
    LOG_ERROR("failed to get the %uth line, "
      "exceeds the TagRowBatch total number of rows %u",
      line, Count());
    return KStatus::FAIL;
  }
  if (res_.data.size() == 0) {
    return SUCCESS;
  }

  tagData->clear();
  k_uint32 tag_num = table_->scan_tags_.size();
  tagData->resize(tag_num);

  k_uint32 batch_no = 0, batch_line = 0;
  if (isFilter_) {
    batch_no = selection_[line].batch_;
    batch_line = selection_[line].line_;
  } else {
    auto &colBatchs = res_.data[0];
    batch_line = line;
    for (auto &it : colBatchs) {
      if (batch_line >= it->count) {
        batch_line -= it->count;
        batch_no++;
      } else {
        break;
      }
    }
  }

  for (int idx = 0; idx < tag_num; idx++) {
    auto it = res_.data[idx];
    k_uint32 index = table_->scan_tags_[idx] + tag_col_offset_;  // index of column in raw table

    roachpb::DataType dt = table_->fields_[index]->get_sql_type();
    char *ptr = nullptr;
    TagRawData rawData;
    rawData.is_null = false;
    rawData.size = 0;

    roachpb::KWDBKTSColumn::ColumnType type =
        table_->fields_[index]->get_column_type();
    if (type == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      rawData.is_null = false;
    } else {
      char *bitmap = static_cast<char *>(it[batch_no]->mem) +
                     batch_line * tag_offsets_[index];

      if (bitmap[0] != 1) {
        rawData.is_null = true;
      }
    }

    if (rawData.is_null) {
      rawData.size = 0;
      rawData.tag_data = nullptr;
      (*tagData)[idx] = rawData;
      continue;
    }

    if ((type != roachpb::KWDBKTSColumn::TYPE_PTAG) &&
        ((dt == roachpb::DataType::VARCHAR) ||
         (dt == roachpb::DataType::NVARCHAR) ||
         (dt == roachpb::DataType::VARBINARY))) {
      if (dt != roachpb::DataType::VARCHAR) {
        rawData.size = it[batch_no]->getVarColDataLen(batch_line);
      }
      rawData.tag_data =
          static_cast<char *>(it[batch_no]->getVarColData(batch_line));
    } else {
      if (type != roachpb::KWDBKTSColumn::TYPE_PTAG) {
        rawData.tag_data = static_cast<char *>(it[batch_no]->mem) +
                           batch_line * tag_offsets_[index] + 1;
      } else {
        rawData.tag_data = static_cast<char *>(it[batch_no]->mem) +
                           batch_line * tag_offsets_[index];
      }
    }
    (*tagData)[idx] = rawData;
  }
  return SUCCESS;
}

void TagRowBatch::Init(TABLE *table) {
  table_ = table;
  SetTagToColOffset(table->GetMinTagId());
  k_uint32 boffset = 1 + (table->GetTagNum() + 7) / 8;
  SetBitmapOffset(boffset);
  k_uint32 primary_tags_len = PRIMARY_TAGS_EXTERN_STORAGE_LENGTH;
  for (k_int32 i = tag_col_offset_; i < table->field_num_; i++) {
    if (table->fields_[i]->get_column_type() ==
        roachpb::KWDBKTSColumn::TYPE_PTAG) {
      primary_tags_len += table->fields_[i]->get_storage_length();
    }
  }
  for (k_int32 i = tag_col_offset_; i < table->field_num_; i++) {
    if (table->fields_[i]->get_column_type() ==
        roachpb::KWDBKTSColumn::TYPE_PTAG) {
      tag_offsets_.emplace(std::make_pair(i, primary_tags_len));
    } else {
      roachpb::DataType dt = table_->fields_[i]->get_sql_type();
      if (((dt == roachpb::DataType::VARCHAR) ||
           (dt == roachpb::DataType::NVARCHAR) ||
           (dt == roachpb::DataType::VARBINARY))) {
        tag_offsets_.emplace(
            std::make_pair(i, sizeof(intptr_t) + 1));  // for varchar
      } else {
        tag_offsets_.emplace(
            std::make_pair(i, table->fields_[i]->get_storage_length() + 1));
      }
    }
  }
  res_.setColumnNum(table_->scan_tags_.size());
}

KStatus TagRowBatch::GetEntities(std::vector<EntityResultIndex> *entities,
                                 k_uint32 *start_tag_index) {
  k_uint32 entities_num_per_pipe, remainder;
  *(start_tag_index) = current_pipe_line_;
  if (current_pipe_no_ >= pipe_entity_num_.size()) {
    return FAIL;
  }
  if (isFilter_) {
    for (k_uint32 i = 0; i < pipe_entity_num_[current_pipe_no_]; i++) {
      entities->emplace_back(
          entity_indexs_[selection_[current_pipe_line_].entity_]);
      current_pipe_line_++;
    }
  } else {
    for (k_uint32 i = 0; i < pipe_entity_num_[current_pipe_no_]; i++) {
      entities->emplace_back(entity_indexs_[current_pipe_line_]);
      current_pipe_line_++;
    }
  }
  current_pipe_no_++;
  return SUCCESS;
}

bool TagRowBatch::isAllDistributed() {
  return current_pipe_no_ >= valid_pipe_no_;
}
void TagRowBatch::SetPipeEntityNum(k_uint32 pipe_degree) {
  current_pipe_no_ = 0;
  current_pipe_line_ = 0;
  k_int32 entities_num_per_pipe, remainder;
  if (isFilter_) {
    entities_num_per_pipe = selection_.size() / pipe_degree;
    remainder = selection_.size() % pipe_degree;
  } else {
    entities_num_per_pipe = entity_indexs_.size() / pipe_degree;
    remainder = entity_indexs_.size() % pipe_degree;
  }
  for (k_int32 i = 0; i < pipe_degree; i++) {
    int current_size = entities_num_per_pipe;
    if (remainder > 0) {
      current_size++;
      remainder--;
    }
    pipe_entity_num_.emplace_back(current_size);
    if (current_size > 0) {
      valid_pipe_no_++;
    }
  }
}

}  // namespace kwdbts