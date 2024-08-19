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
#include "cm_kwdb_context.h"
#include "date_time_util.h"
#include "big_table.h"
#include "mmap_object.h"
#include "mmap_segment_table.h"
#include "mmap_entity_idx.h"
#include "payload.h"
#include "ts_common.h"

class MMapMetricsTable : public TSObject, public TsTableObject {
 private:
  KRWLatch rw_latch_;

 protected:
  string name_;
  MMapEntityIdx* entity_block_idx_{nullptr};

 public:
  MMapMetricsTable() : TSObject(), rw_latch_(RWLATCH_ID_MMAP_METRICS_TABLE_RWLOCK) {}

  virtual ~MMapMetricsTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  int magic() { return *reinterpret_cast<const int *>("MMET"); }

  /**
   * @brief	open a big object.
   *
   * @param 	url			big object URL to be opened.
   * @param 	flag		option to open a file; O_CREAT to create new file.
   * @return	0 succeed, otherwise -1.
   */
  int open(const string& url, const std::string& db_path, const string& tbl_sub_path,
           int flags, ErrorInfo& err_info) override;

  int openInitEntityMeta(const int flags);

  int create(const vector<AttributeInfo>& schema,
             const vector<string>& key, const string& key_order,
             const string& ns_url,
             const string& description,
             const string& tbl_sub_path,
             const string& source_url,
             uint64_t partition_interval,
             int encoding, ErrorInfo& err_info,
             bool init_data);

  int init(const vector<AttributeInfo>& schema, ErrorInfo& err_info);

  const vector<AttributeInfo>& getSchemaInfoWithHidden() const {
    return cols_info_with_hidden_;
  }

  const vector<AttributeInfo>& getSchemaInfoWithoutHidden() const {
    return cols_info_without_hidden_;
  }

  const vector<uint32_t>& getColsIdxForHidden() const {
    return cols_idx_for_hidden_;
  }

  virtual const string& tbl_sub_path() const { return tbl_sub_path_; }

  virtual string name() const override { return name_; }

  virtual string URL() const override;

  uint64_t& partitionInterval() { return meta_data_->partition_interval; }

  virtual int remove();

  int rename(const string& new_fp, const string& file_path);

  virtual void sync(int flags) override;

  timestamp64& minTimestamp() { return meta_data_->min_ts; }

  timestamp64& maxTimestamp() { return meta_data_->max_ts; }

  virtual int reserve(size_t size) {
    return KWEPERM;
  }

  int Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) ;

  int Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows, ErrorInfo& err_info) ;

  int UndoPut(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t hint_row, kwdbts::Payload* payload,
              ErrorInfo& err_info);

  int UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) ;

  int DropColumn(const AttributeInfo& attr_info, ErrorInfo& err_info);

  int AlterColumnType(int col_index, const AttributeInfo& attr_info);

  int UndoAddColumn(const uint32_t& col_id, ErrorInfo &err_info);

  int UndoDropColumn(const uint32_t& col_id, ErrorInfo &err_info);
};