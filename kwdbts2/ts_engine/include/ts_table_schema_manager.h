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
#include <filesystem>
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

namespace kwdbts {

#define Def_Column(col_var, col_id, pname, ptype, poffset, psize, plength, pencoding, \
       pflag, pmax_len, pversion, pattrtype)      \
    struct AttributeInfo col_var;          \
    {col_var.id = col_id; snprintf(col_var.name, sizeof(col_var.name), "%s", pname); \
    col_var.type = ptype; col_var.offset = poffset; }  \
    {col_var.size = psize; col_var.length = plength; }      \
    {col_var.encoding = pencoding; col_var.flag = pflag; }      \
    {col_var.max_len = pmax_len; col_var.version = pversion; }      \
    {col_var.col_flag = (ColumnFlag)pattrtype; }

#define Def_Tag(tag_var, id, data_type, data_length, tag_type)    \
    struct TagInfo tag_var;          \
    {tag_var.m_id = id; tag_var.m_data_type = data_type; tag_var.m_length = data_length; }  \
    {tag_var.m_size = data_length; tag_var.m_tag_type = tag_type; }

class SchemaVersionConv {
 public:
  uint32_t scan_version_{0};
  // std::vector<uint32_t> ts_scan_cols_{};
  std::vector<uint32_t> blk_cols_extended_{};
  std::vector<AttributeInfo> scan_attrs_{};
  std::vector<AttributeInfo> blk_attrs_{};

  SchemaVersionConv(uint32_t scan_version, const std::vector<uint32_t>& blk_cols_extended,
                    const std::vector<AttributeInfo>& scan_attrs, const std::vector<AttributeInfo>& blk_attrs) :
    scan_version_(scan_version), blk_cols_extended_(blk_cols_extended),
    scan_attrs_(scan_attrs), blk_attrs_(blk_attrs) { }
};

/**
 * table schema used for organizing table schema(including tag / tag schema and metric schema).
 */

class TsTableSchemaManager {
 private:
  TSTableID table_id_;

  struct Schema {
    std::vector<AttributeInfo> metric;
    std::vector<TagInfo> tag;
  };

  // schema path of the database
  string schema_root_path_;
  // schema path of the metric
  string metric_schema_path_;
  // schema path of the tag
  string tag_schema_path_;
  KRWLatch schema_rw_lock_;

  int getColumnIndex(const AttributeInfo& attr_info);

  KStatus alterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                        uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus alterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                        uint32_t cur_version, uint32_t new_version, string& msg);

 protected:
  uint32_t cur_schema_version_{0};
  // metric schema of current version
  std::shared_ptr<MMapMetricsTable> cur_metric_schema_;
  // schemas of all versions
  std::unordered_map<uint32_t, std::shared_ptr<MMapMetricsTable>> metric_schemas_;
  std::shared_ptr<TagTable> tag_table_{nullptr};
  // Partition interval.
  uint64_t partition_interval_;
  // Compression status.
  std::atomic<bool> is_compressing_{false};
  std::atomic<bool> is_vacuuming_{false};
  std::unordered_map<string, shared_ptr<SchemaVersionConv>> version_conv_map;
  KRWLatch* ver_conv_rw_lock_{nullptr};

  /**
   * @brief Open the schema file of the specified version
   * @param version schema version
   * @param err_info Error message
   * @return std::shared_ptr<MMapMetricsTable> Pointer
   */
  std::shared_ptr<MMapMetricsTable> open(uint32_t version, ErrorInfo& err_info);

 public:
  TsTableSchemaManager() = delete;

  TsTableSchemaManager(const string& root_path, TSTableID tbl_id) : table_id_(tbl_id), schema_root_path_(root_path + '/'),
      schema_rw_lock_(RWLATCH_ID_TABLE_SCHEMA_RWLOCK) {
    metric_schema_path_ = "metric_" + std::to_string(table_id_) + "/";
    tag_schema_path_ = "tag_" + std::to_string(table_id_) + "/";
    ver_conv_rw_lock_ = new KRWLatch(RWLATCH_ID_VERSION_CONV_RWLOCK);
  }

  virtual ~TsTableSchemaManager();

  bool IsSchemaDirsExist();

  KStatus Init(kwdbContext_p ctx);

  void put(uint32_t ts_version, const std::shared_ptr<MMapMetricsTable>& schema);

  std::shared_ptr<MMapMetricsTable> Get(uint32_t ts_version, bool lock = true);

  inline uint32_t GetCurrentVersion() {
    return cur_schema_version_;
  }

  TSTableID GetTableId();

  std::shared_ptr<TagTable> GetTagTable() {
    return tag_table_;
  }

  KStatus CreateTableSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta, uint32_t ts_version,
                            ErrorInfo& err_info, uint32_t cur_version = 0);
  KStatus CreateTable(kwdbContext_p ctx, roachpb::CreateTsTable* meta, uint64_t db_id,
                      uint32_t ts_version, ErrorInfo& err_info);

  KStatus CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                                const uint32_t cur_version, const uint32_t new_version,
                                                const std::vector<uint32_t/* tag column id*/>& tags);

  KStatus DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,  const uint32_t cur_version,
                                              const uint32_t new_version, const uint64_t index_id);

  KStatus UndoCreateHashIndex(uint32_t index_id, uint32_t cur_ts_version, uint32_t new_ts_version, ErrorInfo& err_info);

  KStatus UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t cur_ts_version,
                        uint32_t new_ts_version, ErrorInfo& err_info);

  vector<uint32_t> GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id);

  KStatus AddMetricSchema(vector<AttributeInfo>& schema, uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

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

  int rdLock();

  int wrLock();

  int unLock();

  int Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info);

  KStatus SetDropped();

  bool IsDropped();

  bool TrySetCompressStatus(bool desired);

  void SetCompressStatus(bool status);

  KStatus RemoveAll();

  KStatus RollBack(uint32_t old_version, uint32_t new_version);

  KStatus UpdateVersion(uint32_t cur_version, uint32_t new_version);

  KStatus UpdateTableVersionOfLastData(uint32_t version);

  uint32_t GetTableVersionOfLatestData();

  static KStatus GetColAttrInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                     AttributeInfo& attr_info, bool first_col);

  KStatus AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus UndoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                     uint32_t cur_version, uint32_t new_version);

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

  bool FindVersionConv(const string& key, std::shared_ptr<SchemaVersionConv>* version_conv);
  void InsertVersionConv(const string& key, const shared_ptr<SchemaVersionConv>& ver_conv);
};

}  // namespace kwdbts
