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
#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <unordered_map>
#include <utility>
#include <string>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ts_common.h"
#include "mmap/mmap_tag_table.h"
#include "mmap/mmap_metrics_table.h"
#include "ts_metrics_table_version_manager.h"

namespace kwdbts {
class SchemaVersionConv {
 public:
  uint32_t scan_version_{0};
  std::vector<uint32_t> blk_cols_extended_{};
  const std::vector<AttributeInfo>* scan_attrs_ = nullptr;
  const std::vector<AttributeInfo>* blk_attrs_ = nullptr;

  SchemaVersionConv(uint32_t scan_version, const std::vector<uint32_t>& blk_cols_extended,
                    const std::vector<AttributeInfo>* scan_attrs, const std::vector<AttributeInfo>* blk_attrs) :
    scan_version_(scan_version), blk_cols_extended_(blk_cols_extended),
    scan_attrs_(scan_attrs), blk_attrs_(blk_attrs) { }
};

/**
 * table schema manager used for organizing table schema (including tag data, tag schema, and metric schema).
 */
class TsTableSchemaManager {
  TSTableID table_id_;
  // schema path of the database
  string table_path_;
  // schema path of the metric
  string metric_schema_path_;
  // schema path of the tag
  string tag_schema_path_;
  uint64_t hash_num_;

 protected:
  uint32_t cur_version_{0};
  std::shared_ptr<MetricsVersionManager> metric_mgr_{nullptr};
  std::shared_ptr<TagTable> tag_table_{nullptr};
  KRWLatch table_version_rw_lock_;

  std::unordered_map<uint64_t, shared_ptr<SchemaVersionConv>> version_conv_map;
  KRWLatch ver_conv_rw_lock_;

 public:
  TsTableSchemaManager() = delete;

  TsTableSchemaManager(const string& root_path, TSTableID tbl_id) : table_id_(tbl_id),
    table_version_rw_lock_(RWLATCH_ID_TABLE_VERSION_RWLOCK),
    ver_conv_rw_lock_(RWLATCH_ID_VERSION_CONV_RWLOCK) {
    table_path_ = root_path + "/" + std::to_string(tbl_id) + "/";
    metric_schema_path_ = "metric/";
    tag_schema_path_ = "tag/";
  }

  bool IsSchemaDirsExist();

  KStatus Init();

  uint32_t GetCurrentVersion() const {
    return cur_version_;
  }

  uint64_t GetHashNum() const {
    return hash_num_;
  }

  TSTableID GetTableId();

  std::shared_ptr<TagTable> GetTagTable() {
    return tag_table_;
  }

  KStatus CreateTable(kwdbContext_p ctx, roachpb::CreateTsTable* meta, uint64_t db_id,
                      uint32_t ts_version, ErrorInfo& err_info);

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                                const uint32_t cur_version, const uint32_t new_version,
                                                const std::vector<uint32_t/* tag column id*/>& tags);

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,  const uint32_t cur_version,
                                              const uint32_t new_version, const uint64_t index_id);

  KStatus UndoCreateHashIndex(uint32_t index_id, uint32_t old_version, uint32_t new_version, ErrorInfo& err_info);

  KStatus UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t old_version,
                            uint32_t new_version, ErrorInfo& err_info);

  vector<uint32_t> GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id);

  KStatus GetMeta(kwdbContext_p ctx, TSTableID table_id, uint32_t version, roachpb::CreateTsTable* meta);

  KStatus GetColumnsExcludeDropped(std::vector<AttributeInfo>& schema, uint32_t ts_version = 0);

  KStatus GetColumnsIncludeDropped(std::vector<AttributeInfo>& schema, uint32_t ts_version = 0);

  KStatus GetMetricMeta(uint32_t version, std::vector<AttributeInfo>& info);

  KStatus GetTagMeta(uint32_t version, std::vector<TagInfo>& info);

  KStatus GetMetricSchema(uint32_t version, std::shared_ptr<MMapMetricsTable>* schema);

  KStatus GetTagSchema(kwdbContext_p ctx, std::shared_ptr<TagTable>* schema);

  void GetAllVersions(std::vector<uint32_t>* table_versions);

  LifeTime GetLifeTime() const;

  void SetLifeTime(LifeTime life_time) const;

  uint64_t GetPartitionInterval() const;

  uint64_t GetDbID() const;

  int Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info);

  KStatus SetDropped();

  bool IsDropped();

  KStatus RemoveAll();

  KStatus AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus UndoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version);

  KStatus UndoAlterCol(uint32_t old_version, uint32_t new_version);

  KStatus UpdateMetricVersion(uint32_t cur_version, uint32_t new_version);

  /**
   * @brief Gets the data type of table first column.
   *
   * @return datatype in storage engine.
   */
  DATATYPE GetTsColDataType();

  /**
   * @brief Gets index information for the actual column (exclude dropped columns)
   *
   * @param table_version Version number of the table. The default is 0.
   * @return Returns index information for the actual column.
   */
  const vector<uint32_t>& GetIdxForValidCols(uint32_t table_version = 0);

  bool FindVersionConv(uint64_t key, std::shared_ptr<SchemaVersionConv>* version_conv);

  void InsertVersionConv(uint64_t key, const shared_ptr<SchemaVersionConv>& ver_conv);

  bool IsExistTableVersion(uint32_t version);

  std::shared_ptr<MMapMetricsTable> GetCurrentMetricsTable() {
    auto cur_metric_version = metric_mgr_->GetCurrentMetricsVersion();
    assert(cur_version_ == cur_metric_version);
    return metric_mgr_->GetCurrentMetricsTable();
  }

 private:
  std::shared_ptr<MMapMetricsTable> open(uint32_t version, ErrorInfo& err_info);

  int getColumnIndex(const AttributeInfo& attr_info);

  static KStatus parseAttrInfo(const roachpb::KWDBKTSColumn& col, AttributeInfo& attr_info, bool first_col);

  KStatus alterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                        uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus alterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                        uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus addMetricForAlter(vector<AttributeInfo>& schema, uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

  std::shared_ptr<MMapMetricsTable> getMetricsTable(uint32_t ts_version) {
    return metric_mgr_->GetMetricsTable(ts_version);
  }

  static KStatus parseMetaToSchema(roachpb::CreateTsTable* meta,
                                   std::vector<AttributeInfo>& metric_schema,
                                   std::vector<TagInfo>& tag_schema);
};

}  // namespace kwdbts
