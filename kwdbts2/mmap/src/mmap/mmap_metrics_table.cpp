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

#include <cstdio>
#include <algorithm>
#include <cstring>
#include <atomic>
#include <sys/mman.h>
#include "cm_func.h"
#include "dirent.h"
#include "mmap/mmap_metrics_table.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "engine.h"
#include "utils/compress_utils.h"

extern void markDeleted(char* delete_flags, size_t row_index);

MMapMetricsTable::~MMapMetricsTable() {
  if (entity_block_meta_) {
    delete entity_block_meta_;
  }
}

impl_latch_virtual_func(MMapMetricsTable, &rw_latch_)

int MMapMetricsTable::open(const string& table_path, const std::string& db_path, const string& tbl_sub_path,
                           int flags, ErrorInfo& err_info) {
  string file_path = getTsFilePath(table_path);
  if ((err_info.errcode = TsTableObject::open(file_path, db_path, tbl_sub_path,
                                              magic(), flags)) < 0) {
    err_info.setError(err_info.errcode, tbl_sub_path + getTsFilePath(table_path));
    return err_info.errcode;
  }
  name_ = getTsObjectName(path());
  if (metaDataLen() < (off_t) sizeof(TSTableFileMetadata)) {
    if (!(bt_file_.flags() & O_CREAT)) {
      err_info.setError(KWECORR, tbl_sub_path + getTsFilePath(table_path));
    }
    return err_info.errcode;
  }
  setObjectReady();
  return err_info.errcode;
}

int MMapMetricsTable::create(const vector<AttributeInfo>& schema, const uint32_t& table_version, const string& tbl_sub_path,
                             uint64_t partition_interval, int encoding, ErrorInfo& err_info, bool init_data,
                             uint64_t hash_number) {
  if (init(schema, err_info) < 0)
    return err_info.errcode;

  meta_data_->magic = magic();
  meta_data_->struct_type |= (ST_COLUMN_TABLE);
  meta_data_->schema_version = table_version;
  meta_data_->partition_interval = partition_interval;

  meta_data_->has_data = init_data;
  meta_data_->hash_num = hash_number;
  setObjectReady();

  return 0;
}

int MMapMetricsTable::init(const vector<AttributeInfo>& schema, ErrorInfo& err_info) {
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);

  name_ = getTsObjectName(path());

  for (size_t i = 0 ; i < schema.size() ; ++i) {
    cols_info_include_dropped_.push_back(schema[i]);
  }

  if ((meta_data_->record_size = setAttributeInfo(cols_info_include_dropped_)) < 0) {
    return err_info.errcode;
  }
  off_t col_off = 0;
  col_off = addColumnInfo(cols_info_include_dropped_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  meta_data_->cols_num = cols_info_include_dropped_.size();
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  for (int i = 0; i < cols_info_include_dropped_.size(); ++i) {
    if(!cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
      cols_info_exclude_dropped_.emplace_back(cols_info_include_dropped_[i]);
      idx_for_valid_cols_.emplace_back(i);
    }
  }

  return err_info.errcode;
}

string MMapMetricsTable::path() const {
  return filePath();
}

int MMapMetricsTable::remove() {
  int error_code = 0;
  if (entity_block_meta_ != nullptr) {
    error_code = entity_block_meta_->remove();
    if (error_code < 0) {
      return error_code;
    }
  }
  delete entity_block_meta_;
  entity_block_meta_ = nullptr;
  return bt_file_.remove();
}

void MMapMetricsTable::sync(int flags) {
  if (entity_block_meta_ != nullptr) {
    entity_block_meta_->sync(flags);
  }
  bt_file_.sync(flags);
}


int MMapMetricsTable::Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  if (entity_block_meta_ != nullptr) {
    entity_block_meta_->sync(MS_SYNC);
  }
  return 0;
}

int MMapMetricsTable::Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows,
                           ErrorInfo& err_info) {
  if (entity_block_meta_ != nullptr) {
    entity_block_meta_->sync(MS_SYNC);
  }
  return 0;
}

int MMapMetricsTable::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  return 0;
}

int MMapMetricsTable::rename(const string& new_fp, const string& file_path) {
  int err_code = 0;
  if (!realFilePath().empty()) {
    err_code = ::rename(bt_file_.realFilePath().c_str(), new_fp.c_str());
    if (err_code != 0) {
      err_code = errnoToErrorCode();
      return err_code;
    }
    bt_file_.realFilePath() = new_fp;
    bt_file_.filePath() = file_path;
    name_ = getTsObjectName(bt_file_.filePath());
  }
  return err_code;
}
