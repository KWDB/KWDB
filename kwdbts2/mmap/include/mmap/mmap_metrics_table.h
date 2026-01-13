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

#pragma once

#include <string>
#include "utils/date_time_util.h"
#include "ts_table_object.h"
#include "ts_common.h"

class MMapMetricsTable : public TSObject, public TsTableObject {
 private:
  KRWLatch rw_latch_;

 protected:
  string name_;

 public:
  MMapMetricsTable() : rw_latch_(RWLATCH_ID_MMAP_METRICS_TABLE_RWLOCK) {}

  virtual ~MMapMetricsTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  int magic() { return *reinterpret_cast<const int *>("MMET"); }

  /**
   * @brief	open a big object.
   *
   * @param 	table_name			big object name to be opened.
   * @param 	flags		option to open a file; O_CREAT to create new file.
   * @return	0 succeed, otherwise -1.
   */
  int open(const std::string& table_name, const fs::path& table_path, int flags, ErrorInfo& err_info);

  int create(const vector<AttributeInfo>& schema, const uint32_t& table_version,
             uint64_t partition_interval, int encoding, ErrorInfo& err_info, uint64_t hash_number = 2000);

  int init(const vector<AttributeInfo>& schema, ErrorInfo& err_info);

  const vector<AttributeInfo>& getSchemaInfoIncludeDropped() const {
    return cols_info_include_dropped_;
  }

  const vector<AttributeInfo>& getSchemaInfoExcludeDropped() const {
    return cols_info_exclude_dropped_;
  }

  const vector<AttributeInfo>* getSchemaInfoIncludeDroppedPtr() const {
    return &cols_info_include_dropped_;
  }

  const vector<AttributeInfo>* getSchemaInfoExcludeDroppedPtr() const {
    return &cols_info_exclude_dropped_;
  }

  DATATYPE GetTsColDataType() {
    return static_cast<DATATYPE>(cols_info_exclude_dropped_[0].type);
  }

  inline uint32_t GetVersion() {
    return meta_data_->schema_version;
  }

  string name() const override { return name_; }

  string path() const override;

  uint64_t& partitionInterval() { return meta_data_->partition_interval; }

  void SetPartitionInterval(uint64_t partition_interval) { meta_data_->partition_interval = partition_interval; }

  uint64_t& hashNum() { return meta_data_->hash_num; }

  virtual int remove();

  void sync(int flags) override;

  LifeTime GetLifeTime() { return LifeTime{meta_data_->life_time, meta_data_->precision}; }

  void SetLifeTime(LifeTime life_time) {
    meta_data_->life_time = life_time.ts;
    meta_data_->precision = life_time.precision;
  }

  virtual int reserve(size_t size) {
    return KWEPERM;
  }

  int Sync(kwdbts::TS_OSN check_lsn, ErrorInfo& err_info);

  int Sync();
};
