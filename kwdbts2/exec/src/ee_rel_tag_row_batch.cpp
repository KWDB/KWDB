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

#include "ee_rel_tag_row_batch.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_table.h"
#include "ee_common.h"

namespace kwdbts {

RelTagRowBatch::RelTagRowBatch() {
}
RelTagRowBatch::~RelTagRowBatch() {
  for (DatumPtr& rel_data_ptr : data_ptrs_) {
    free(rel_data_ptr);
  }
}

KStatus RelTagRowBatch::GetTagData(TagData *tagData, void **bitmap,
                                k_uint32 line) {
  if (line >= rel_tag_data_.size()) {
    return KStatus::FAIL;
  }
  *tagData = rel_tag_data_[line];
  return KStatus::SUCCESS;
}

void RelTagRowBatch::Init(TABLE *table) {
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
      tag_offsets_.emplace_back(primary_tags_len);
    } else {
      roachpb::DataType dt = table_->fields_[i]->get_sql_type();
      if (((dt == roachpb::DataType::VARCHAR) ||
           (dt == roachpb::DataType::NVARCHAR) ||
           (dt == roachpb::DataType::VARBINARY))) {
        tag_offsets_.emplace_back(sizeof(intptr_t) + 1);  // for varchar
      } else {
        tag_offsets_.emplace_back(table->fields_[i]->get_storage_length() + 1);
      }
    }
  }
  res_.setColumnNum(table_->scan_tags_.size());

  for (int i = 0; i < table_->scan_rel_cols_.size(); ++i) {
    Field* f = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_);
    if (IsVarStringType(f->get_storage_type())) {
      rel_data_len_ += f->get_storage_length() + sizeof(k_uint16) + 1;
    } else if (IsFixedStringType(f->get_storage_type())) {
      rel_data_len_ += f->get_storage_length() + 1;
    } else {
      rel_data_len_ += f->get_storage_length();
    }
  }

  for (int i = 0; i < table_->scan_tags_.size(); ++i) {
    Field* f = table_->GetFieldWithColNum(table_->scan_tags_[i] + tag_col_offset_);
    if (IsVarStringType(f->get_storage_type())) {
      tag_data_len_ += f->get_storage_length() + sizeof(k_uint16) + 1;
    } else if (IsFixedStringType(f->get_storage_type())) {
      tag_data_len_ += f->get_storage_length() + 1;
    } else {
      tag_data_len_ += f->get_storage_length();
    }
  }
}

KStatus RelTagRowBatch::AddRelTagJoinRecord(kwdbContext_p ctx, BatchDataContainerPtr batch_data_container,
                              DataChunkPtr &tag_data_chunk, k_uint32 rel_row, RowIndice& row_indice) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  void* bitmap = nullptr;
  k_uint32 col_idx_in_rs;
  while (row_indice.offset_in_batch != 0) {
    rel_tag_record.resize(rel_tag_col_num);
    // Add build side column data to rel_tag_record
    DatumPtr tag_data_ptr = (DatumPtr)malloc(tag_data_len_);
    if (nullptr == tag_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("tag_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(tag_data_ptr, 0, tag_data_len_);
    data_ptrs_.push_back(tag_data_ptr);
    for (int i = 0; i < tag_col_num; ++i) {
      TagRawData rel_col_raw_data;
      tag_data_chunk->ConvertToTagData(ctx, rel_row, i, rel_col_raw_data, tag_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[i] + table_->min_tag_id_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    // Add probe side column data to rel_tag_record
    DatumPtr rel_data_ptr = (DatumPtr)malloc(rel_data_len_);
    if (nullptr == rel_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("rel_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(rel_data_ptr, 0, rel_data_len_);
    data_ptrs_.push_back(rel_data_ptr);
    for (int i = 0; i < rel_col_num; ++i) {
      TagRawData rel_col_raw_data;
      batch_data_container->GetRelDataChunk(row_indice.batch_no - 1)->ConvertToTagData(ctx, row_indice.offset_in_batch - 1,
                                        table_->scan_rel_cols_[i], rel_col_raw_data, rel_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    rel_tag_data_.push_back(std::move(rel_tag_record));
    // Add the entity index int data chunk to entity_indexs_
    entity_indexs_.push_back(tag_data_chunk->GetEntityIndex(rel_row));
    row_indice = batch_data_container->rel_data_linked_lists_.GetLinkedList(row_indice.batch_no - 1)->
                                                             row_indice_list_[row_indice.offset_in_batch - 1];
  }
  Return(ret);
}

KStatus RelTagRowBatch::AddTagRelJoinRecord(kwdbContext_p ctx, BatchDataContainerPtr batch_data_container,
                              DataChunkPtr &rel_data_chunk, k_uint32 rel_row, RowIndice row_indice) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  void* bitmap = nullptr;
  k_uint32 col_idx_in_rs;
  while (row_indice.offset_in_batch != 0) {
    rel_tag_record.resize(rel_tag_col_num);
    // Add build side column data to rel_tag_record
    DatumPtr tag_data_ptr = (DatumPtr)malloc(tag_data_len_);
    if (nullptr == tag_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("tag_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(tag_data_ptr, 0, tag_data_len_);
    data_ptrs_.push_back(tag_data_ptr);
    for (int i = 0; i < tag_col_num; ++i) {
      TagRawData rel_col_raw_data;
      batch_data_container->GetRelDataChunk(row_indice.batch_no - 1)->ConvertToTagData(ctx, row_indice.offset_in_batch - 1,
                                                      i, rel_col_raw_data, tag_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[i] + table_->min_tag_id_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    // Add probe side column data to rel_tag_record
    DatumPtr rel_data_ptr = (DatumPtr)malloc(rel_data_len_);
    if (nullptr == rel_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("rel_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(rel_data_ptr, 0, rel_data_len_);
    data_ptrs_.push_back(rel_data_ptr);
    for (int i = 0; i < rel_col_num; ++i) {
      TagRawData rel_col_raw_data;
      rel_data_chunk->ConvertToTagData(ctx, rel_row, table_->scan_rel_cols_[i], rel_col_raw_data, rel_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    rel_tag_data_.push_back(std::move(rel_tag_record));
    // Add the entity index int data chunk to entity_indexs_
    entity_indexs_.push_back(batch_data_container->GetRelDataChunk(row_indice.batch_no - 1)->
                                                                  GetEntityIndex(row_indice.offset_in_batch - 1));
    row_indice = batch_data_container->rel_data_linked_lists_.GetLinkedList(row_indice.batch_no - 1)->
                                                             row_indice_list_[row_indice.offset_in_batch - 1];
  }
  Return(ret);
}

KStatus RelTagRowBatch::AddPrimaryTagRelJoinRecord(kwdbContext_p ctx, DataChunkPtr &rel_data_chunk,
                                            k_uint32 rel_row_index, DataChunkPtr &tag_data_chunk) {
  EnterFunc();
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  TagData tag_data(tag_col_num);
  void* bitmap = nullptr;
  k_uint32 current_line = 0;
  KStatus ret = KStatus::SUCCESS;
  k_uint32 col_idx_in_rs;
  k_uint32 count = tag_data_chunk->Count();
  for (int i = 0; i < count; ++i) {
    rel_tag_record.resize(rel_tag_col_num);
    // Add build side column data to rel_tag_record
    DatumPtr tag_data_ptr = (DatumPtr)malloc(tag_data_len_);
    if (nullptr == tag_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("tag_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(tag_data_ptr, 0, tag_data_len_);
    data_ptrs_.push_back(tag_data_ptr);
    for (int j = 0; j < tag_col_num; ++j) {
      TagRawData rel_col_raw_data;
      tag_data_chunk->ConvertToTagData(ctx, i, j, rel_col_raw_data, tag_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[j] + table_->min_tag_id_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    // Add rel column data to rel_tag_record
    DatumPtr rel_data_ptr = (DatumPtr)malloc(rel_data_len_);
    if (nullptr == rel_data_ptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("rel_data_ptr malloc failed\n");
      Return(KStatus::FAIL);
    }
    memset(rel_data_ptr, 0, rel_data_len_);
    data_ptrs_.push_back(rel_data_ptr);
    for (int j = 0; j < rel_col_num; ++j) {
      TagRawData rel_col_raw_data;
      rel_data_chunk->ConvertToTagData(ctx, rel_row_index, table_->scan_rel_cols_[j], rel_col_raw_data, rel_data_ptr);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[j] + table_->field_num_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    rel_tag_data_.push_back(std::move(rel_tag_record));
    entity_indexs_.push_back(tag_data_chunk->GetEntityIndex(i));
  }
  Return(ret);
}

}  // namespace kwdbts
