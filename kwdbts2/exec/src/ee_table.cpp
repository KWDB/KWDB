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
// Created by liguoliang on 2022/07/18.

#include "ee_table.h"

#include "ee_common.h"
#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "er_api.h"
#include "lg_api.h"

namespace kwdbts {

TABLE::~TABLE() {
  for (k_int32 i = 0; i < block_filters_.size(); i++) {
    if (IsStringType(fields_[block_filters_[i].colID]->get_storage_type()) ||
        fields_[block_filters_[i].colID]->get_storage_type() ==
            roachpb::DataType::BOOL) {
      block_filters_[i].Reset();
    }
  }
  if (fields_) {
    for (k_uint32 i = 0; i < field_num_; ++i) {
      SafeDeletePointer(fields_[i]);
    }

    free(fields_);
  }

  // clean up relational fields for multiple model processing
  for (k_uint32 i = 0; i < rel_fields_.size(); ++i) {
    SafeDeletePointer(rel_fields_[i]);
  }

  fields_ = nullptr;
  field_num_ = 0;
}


Field *TABLE::GetFieldWithColNum(k_uint32 num) {
  Field *table_field = nullptr;
  if (num < field_num_) {
    table_field = fields_[num];
  } else if (num < field_num_ + rel_fields_.size()) {
    // handle relational fields for multiple model processing
    table_field = rel_fields_[num - field_num_];
  }

  return table_field;
}

KStatus TABLE::Init(kwdbContext_p ctx, const TSTagReaderSpec *spec) {
  EnterFunc();
  if (KTRUE == init_) {
    Return(KStatus::SUCCESS);
  }

  KStatus ret = KStatus::SUCCESS;
  do {
    // field num
    field_num_ = spec->colmetas_size();
    fields_ = static_cast<Field **>(malloc(field_num_ * sizeof(Field *)));
    if (nullptr == fields_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc error\n");
      break;
    }
    memset(fields_, 0, field_num_ * sizeof(Field *));
    // resolve col
    for (k_int32 i = 0; i < field_num_; ++i) {
      Field *field = nullptr;
      const TSCol &col = spec->colmetas(i);
      ret = InitField(ctx, col, i, &field);
      if (ret != SUCCESS) break;
      fields_[i] = field;
    }

    // resolve hashpoints
    k_uint32 hps_num = spec->rangespans_size();
    for (k_int32 i = 0; i < hps_num; ++i) {
      const HashpointSpan &hps = spec->rangespans(i);
      k_uint32 from = hps.from();
      k_uint32 to = hps.to();
      for (k_uint32 i = from; i <= to; i++) {
        hash_points_.insert(i);
      }
    }

    if (KStatus::FAIL == ret) {
      break;
    }

    // initialize relational fields for multiple model processing
    rel_fields_.reserve(spec->relationalcols_size());
    if (nullptr == fields_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("malloc error\n");
      break;
    }
    // resolve relational cols for multiple model processing
    for (k_int32 i = 0; i < spec->relationalcols_size(); ++i) {
      Field *rel_field = nullptr;
      const TSCol &col = spec->relationalcols(i);
      ret = InitField(ctx, col, i + field_num_, &rel_field);
      if (ret != SUCCESS) break;
      rel_fields_.push_back(rel_field);
    }

    if (KStatus::FAIL == ret) {
      break;
    }

    // initialize join columns for multiple model processing
    if (spec->probecolids_size() != spec->hashcolids_size()) {
      ret = KStatus::FAIL;
      break;
    }

    rel_tag_join_column_indexes_.reserve(spec->probecolids_size());
    // resolve probe and hash colids for multiple model processing
    for (k_int32 i = 0; i < spec->probecolids_size(); ++i) {
      rel_tag_join_column_indexes_.push_back({spec->probecolids(i), spec->hashcolids(i)});
    }
  } while (0);

  Return(ret);
}

vector<Field*>& TABLE::GetRelFields() {
  return rel_fields_;
}

std::vector<pair<k_uint32, k_uint32>>& TABLE::GetRelTagJoinColumnIndexes() {
  return rel_tag_join_column_indexes_;
}

std::vector<k_uint32>& TABLE::GetScanTags() {
  return scan_tags_;
}

KStatus TABLE::InitField(kwdbContext_p ctx, const TSCol &col, k_uint32 index,
                         Field **field) {
  EnterFunc();
  KStatus ret = SUCCESS;
  roachpb::DataType sql_type = col.storage_type();
  switch (sql_type) {
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO: {
      *field = new FieldTimestampTZ();
      break;
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO: {
      *field = new FieldTimestamp();
      break;
    }
    case roachpb::DataType::SMALLINT: {
      *field = new FieldShort();
      break;
    }
    case roachpb::DataType::INT: {
      *field = new FieldInt();
      break;
    }
    case roachpb::DataType::BIGINT: {
      *field = new FieldLonglong();
      break;
    }
    case roachpb::DataType::FLOAT: {
      *field = new FieldFloat();
      break;
    }
    case roachpb::DataType::DOUBLE: {
      *field = new FieldDouble();
      break;
    }
    case roachpb::DataType::BOOL: {
      *field = new FieldBool();
      break;
    }
    case roachpb::DataType::CHAR: {
      *field = new FieldChar();
      break;
    }
    case roachpb::DataType::BINARY: {
      *field = new FieldBlob();
      break;
    }
    case roachpb::DataType::NCHAR: {
      *field = new FieldNchar();
      break;
    }
    case roachpb::DataType::VARCHAR: {
      *field = new FieldVarchar();
      break;
    }
    case roachpb::DataType::NVARCHAR: {
      *field = new FieldNvarchar();
      break;
    }
    case roachpb::DataType::VARBINARY: {
      *field = new FieldVarBlob();
      break;
    }
    default: {
      LOG_ERROR("unknow column type %d\n", sql_type);
      break;
    }
  }

  if (nullptr == *field) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("malloc error\n");
    ret = KStatus::FAIL;
    Return(ret);
  }

  (*field)->table_ = this;
  (*field)->set_num(index);
  (*field)->set_sql_type(sql_type);
  (*field)->set_storage_type(sql_type);
  (*field)->set_allow_null(col.nullable());
  if (col.has_storage_len()) {
    (*field)->set_storage_length(col.storage_len());
  }
  if (col.has_col_offset()) (*field)->set_column_offset(col.col_offset());
  if (col.has_variable_length_type())
    (*field)->set_variable_length_type(col.variable_length_type());
  if (col.has_column_type()) {
    (*field)->set_column_type(col.column_type());
    if (col.column_type() != roachpb::KWDBKTSColumn::TYPE_DATA) {
      if (min_tag_id_ == 0) {
        min_tag_id_ = index;
      }
      tag_num_++;
    }
  }
  Return(ret);
}

void *TABLE::GetData(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {
  auto row_batch = current_thd->GetRowBatch();
  return row_batch->GetData(col, offset, ctype, dt);
}

k_uint32 TABLE::PTagCount() const {
  k_uint32 ptag_count = 0;
  for (k_uint32 i = 0; i < field_num_; ++i) {
    if (fields_[i]->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      ++ptag_count;
    }
  }
  return ptag_count;
}

k_uint16 TABLE::GetDataLen(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype) {
  auto row_batch = current_thd->GetRowBatch();
  return row_batch->GetDataLen(col, offset, ctype);
}

bool TABLE::is_nullable(int col, roachpb::KWDBKTSColumn::ColumnType ctype) {
//  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype) {
//    return false;
//  }
  auto row_batch = current_thd->GetRowBatch();

  return row_batch->IsNull(col, ctype);
}

void *TABLE::GetData2(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {
  IChunk* data_chunk = current_thd->GetDataChunk();
  return data_chunk->GetData(col);
}

bool TABLE::is_nullable2(int col, roachpb::KWDBKTSColumn::ColumnType ctype) {
  IChunk* data_chunk = current_thd->GetDataChunk();
  return data_chunk->IsNull(col);
}

k_bool TABLE::IsOverflow(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype) {
  auto row_batch = current_thd->GetRowBatch();
  return row_batch->IsOverflow(col, ctype);
}

}  // namespace kwdbts
