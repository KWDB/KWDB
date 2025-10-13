// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_table_object.h"
#include "utils/big_table_utils.h"

TsTableObject::TsTableObject() {
  meta_data_length_ = 0;
  mem_data_ = nullptr;
  meta_data_ = nullptr;
}

TsTableObject::~TsTableObject() {
}

int TsTableObject::open(const string& table_path, const std::string& db_path, const string& tbl_sub_path, int cc,
                        int flags) {
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  int error_code = bt_file_.open(table_path, db_path_ + tbl_sub_path_ + table_path, flags);
  if (error_code < 0) {
    return error_code;
  }
  if (bt_file_.fileLen() >= (off_t) sizeof(TSTableFileMetadata)) {
    cols_info_include_dropped_.clear();
    // initialize all pointers first
    initSection();
    if (meta_data_->magic != cc) {
      return KWENOOBJ;
    }

    error_code = getColumnInfo(meta_data_->attribute_offset,
                               meta_data_->cols_num, cols_info_include_dropped_);
    if (error_code < 0) {
      return error_code;
    }

    cols_info_exclude_dropped_.clear();
    idx_for_valid_cols_.clear();
    int offset = 0;
    for (int i = 0; i < cols_info_include_dropped_.size(); ++i) {
      if(!cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
        cols_info_exclude_dropped_.emplace_back(cols_info_include_dropped_[i]);
        idx_for_valid_cols_.emplace_back(i);
        offset += getDataTypeSize(cols_info_include_dropped_[i].type);
      }
    }

    ErrorInfo err_info;

    meta_data_length_ = meta_data_->length;
  }
  if (meta_data_length_ == 0)
    meta_data_length_ = bt_file_.fileLen();

  obj_name_ = getTsObjectName(bt_file_.filePath());

  return 0;
}

int TsTableObject::close() {
  bt_file_.munmap();
  meta_data_length_ = 0;
  mem_data_ = nullptr;
  meta_data_ = nullptr;
  return 0;
}

int TsTableObject::initMetaData() {
  int err_code = 0;
  if (bt_file_.fileLen() < (off_t) sizeof(TSTableFileMetadata))
    err_code = memExtend(sizeof(TSTableFileMetadata));
  if (err_code < 0)
    return err_code;
  memset(meta_data_, 0, sizeof(TSTableFileMetadata));
  meta_data_->meta_data_length = sizeof(TSTableFileMetadata);
  meta_data_->schema_version = 1; // default: 1
  return 0;
}

void TsTableObject::initSection() {
  meta_data_ = reinterpret_cast<TSTableFileMetadata*>(bt_file_.memAddr());
}

int TsTableObject::memExtend(off_t offset, size_t ps) {
  off_t new_mem_len = meta_data_length_ + offset;
  if (bt_file_.fileLen() < new_mem_len) {
    int err_code = bt_file_.mremap(getPageOffset(new_mem_len, ps));
    if (err_code < 0)
      return err_code;
    initSection();
  } else {
    /// trim table
  }
  meta_data_length_ = new_mem_len;
  return 0;
}

int TsTableObject::getColumnInfo(off_t offset, int size, vector<AttributeInfo>& attr_info) {
  AttributeInfo* cols = reinterpret_cast<AttributeInfo*>(addr(offset));
  for (int i = 0 ; i < size ; ++i) {
    AttributeInfo ainfo;
    memcpy(&ainfo, &cols[i], sizeof(AttributeInfo));
    attr_info.push_back(ainfo);
  }
  return 0;
}

void TsTableObject::setColumnInfo(int i, const AttributeInfo& a_info) {
  AttributeInfo* col_attr = reinterpret_cast<AttributeInfo*>(addr(meta_data_->attribute_offset));
  col_attr[i] = a_info;
}

off_t TsTableObject::addColumnInfo(const vector<AttributeInfo>& attr_info,
                                   int& err_code) {
  off_t start_addr = metaDataLen();
  off_t len = attr_info.size() * sizeof(AttributeInfo);
  err_code = memExtend(len);
  if (err_code < 0) {
    metaDataLen() = bt_file_.fileLen();
    return err_code;
  }
  meta_data_->meta_data_length += len;

  AttributeInfo* col_attr = reinterpret_cast<AttributeInfo*>(addr(start_addr));
  for (size_t i = 0 ; i < attr_info.size() ; ++i) {
    col_attr[i] = attr_info[i];
  }

  return start_addr;
}

const vector<AttributeInfo>& TsTableObject::colsInfoWithHidden() const {
  return cols_info_include_dropped_;
}

int TsTableObject::getColumnIndex(const AttributeInfo& attr_info) {
  int col_no = -1;
  for (int i = 0; i < cols_info_include_dropped_.size(); ++i) {
    if ((cols_info_include_dropped_[i].id == attr_info.id) && (!cols_info_include_dropped_[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  return col_no;
}

int TsTableObject::getColumnIndex(const uint32_t& col_id) {
  int col_no = -1;
  for (int i = 0; i < cols_info_include_dropped_.size(); ++i) {
    if (cols_info_include_dropped_[i].id == col_id) {
      col_no = i;
      break;
    }
  }
  return col_no;
}
