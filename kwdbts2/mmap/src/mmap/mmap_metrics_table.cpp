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

extern void markDeleted(char* delete_flags, size_t row_index);

MMapMetricsTable::~MMapMetricsTable() {
}

impl_latch_virtual_func(MMapMetricsTable, &rw_latch_)

int MMapMetricsTable::open(const std::string& table_name, const fs::path& table_path, int flags, ErrorInfo& err_info) {
  const fs::path absolute_path = table_path / table_name;
  if ((err_info.errcode = TsTableObject::open(table_name, absolute_path, magic(), flags)) < 0) {
    err_info.setError(err_info.errcode, absolute_path);
    return err_info.errcode;
  }
  name_ = getTsObjectName(path());
  if (metaDataLen() < (off_t) sizeof(TSTableFileMetadata)) {
    if (!(bt_file_.flags() & O_CREAT)) {
      err_info.setError(KWECORR, absolute_path);
    }
    return err_info.errcode;
  }
  setObjectReady();
  return err_info.errcode;
}

int MMapMetricsTable::create(const vector<AttributeInfo>& schema, const uint32_t& table_version,
                             uint64_t partition_interval, int encoding, ErrorInfo& err_info, uint64_t hash_number) {
  if (init(schema, err_info) < 0)
    return err_info.errcode;

  meta_data_->struct_type |= (ST_COLUMN_TABLE);
  meta_data_->schema_version = table_version;
  meta_data_->partition_interval = partition_interval;
  meta_data_->encoding = encoding;
  meta_data_->hash_num = hash_number;
  meta_data_->magic = magic();
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
  return bt_file_.remove();
}

void MMapMetricsTable::sync(int flags) {
  bt_file_.sync(flags);
}

int MMapMetricsTable::Sync(kwdbts::TS_OSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  return 0;
}

int MMapMetricsTable::Sync() {
  sync(MS_SYNC);
  return 0;
}
