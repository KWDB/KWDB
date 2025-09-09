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

#include <string.h>
#include "column_utils.h"
#include "mmap/mmap_tag_column_table.h"


bool ParseToAttributeInfo(const roachpb::KWDBKTSColumn& col, AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      attr_info.type = DATATYPE::TIMESTAMP64;
      attr_info.max_len = 3;
      break;
    case roachpb::SMALLINT:
      attr_info.type = DATATYPE::INT16;
      break;
    case roachpb::INT:
      attr_info.type = DATATYPE::INT32;
      break;
    case roachpb::BIGINT:
      attr_info.type = DATATYPE::INT64;
      break;
    case roachpb::FLOAT:
      attr_info.type = DATATYPE::FLOAT;
      break;
    case roachpb::DOUBLE:
      attr_info.type = DATATYPE::DOUBLE;
      break;
    case roachpb::BOOL:
      attr_info.type = DATATYPE::BYTE;
      break;
    case roachpb::CHAR:
      attr_info.type = DATATYPE::CHAR;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::BINARY:
    case roachpb::NCHAR:
      attr_info.type = DATATYPE::BINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::VARCHAR:
      attr_info.type = DATATYPE::VARSTRING;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return false;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN);
  attr_info.length = col.storage_len();
  if (!col.nullable()) {
    attr_info.setFlag(AINFO_NOT_NULL);
  }
  if (col.dropped()) {
    attr_info.setFlag(AINFO_DROPPED);
  }
  attr_info.col_flag = (ColumnFlag) col.col_type();
  attr_info.version = 1;

  return true;
}

bool ParseToColumnInfo(struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  switch (attr_info.type) {
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::INVALID:
    default:
    return false;
  }

  col.set_column_id(attr_info.id);
  col.set_name(attr_info.name);
  col.set_nullable(true);
  col.set_dropped(false);
  if (!col.has_storage_len()) {
    col.set_storage_len(attr_info.length);
  }
  if (attr_info.isFlag(AINFO_NOT_NULL)) {
    col.set_nullable(false);
  }
  if (attr_info.isFlag(AINFO_DROPPED)) {
    col.set_dropped(true);
  }
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)(attr_info.col_flag));
  return true;
}

bool ParseTagColumnInfo(struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  col.set_storage_len(tag_info.m_length);
  col.set_column_id(tag_info.m_id);
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)((ColumnFlag)tag_info.m_tag_type));
  if (tag_info.isDropped()) {
    col.set_dropped(true);
  }
  switch (tag_info.m_data_type) {
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      break;
    case DATATYPE::INVALID:
    default:
      return false;
  }
  return true;
}
