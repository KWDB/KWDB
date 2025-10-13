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

#include <mutex>
#include <shared_mutex>
#include <pthread.h>
#include "ts_object.h"
#include "mmap/mmap_file.h"
#include "utils/big_table_utils.h"
#include "data_type.h"
#include "settings.h"

using namespace kwdbts;

#define TSCOLUMNATTR_LEN      64  // MAX_COLUMNATTR_LEN + 2

typedef char col_a[TSCOLUMNATTR_LEN];


/**
 * meta data information in a TSTable object
 * DO NOT ALTER variable order within the structure!!!
 */
struct TSTableFileMetadata {
  int magic;                // Magic number for a big object.
  int struct_type;          // structure type
  uint32_t schema_version;       // data version number
  uint32_t db_id ;          // database id
  uint64_t partition_interval;  // unit: second
  int cols_num;                    // number of cols.
  time_t create_time;       // TSTable object (vtree) create time.
  off_t meta_data_length;   // length of meta data section.

  off_t attribute_offset;   // offset to tree attributes.
  off_t record_size;        // size of record (in bytes).
  int encoding;             // Encoding scheme.
  int32_t precision;        // precision of life time
  bool is_dropped;
  // Updatable data, start from 512 bytes for recoverability.
  size_t num_node;          // total number of nodes.
  off_t length;             // file data length.
  int status;               // status flag.
  size_t num_leaf_node;     // total number of leaf nodes;
  size_t actul_size;        // Actual table size.
  int64_t life_time;         // life time: second type
  // has valid row
  bool has_data;
  // entity hash range
  uint64_t hash_num;
  char user_defined[115]; ///< reserved for user-defined meta data information.
};

static_assert(sizeof(TSTableFileMetadata) == 264, "wrong size of TSTableFileMetadata, please check compatibility.");

class TsTableObject {
 protected:
  off_t meta_data_length_;
  TSTableFileMetadata* meta_data_;
  void* mem_data_;        ///< data section starting address.
  string obj_name_;
  string db_path_;  // file path
  string tbl_sub_path_;

  vector<AttributeInfo> cols_info_include_dropped_;
  vector<AttributeInfo> cols_info_exclude_dropped_;
  // Index for valid columns, and the index number is corresponding to cols_info_include_dropped_
  vector<uint32_t> idx_for_valid_cols_;

  inline size_t& _reservedSize() const { return meta_data_->num_leaf_node; }

  inline size_t& size_() const { return meta_data_->num_node; }

  void setColumnInfo(int idx, const AttributeInfo& a_info);

  int getColumnInfo(off_t offset, int size, vector<AttributeInfo>& attr_info);

  off_t addColumnInfo(const vector<AttributeInfo>& attr_info, int& err_code);


 public:
  TsTableObject();

  virtual ~TsTableObject();
  MMapFile bt_file_;
  off_t& metaDataLen() { return meta_data_length_; }

  /**
   * @brief	open a big object.
   *
   * @param 	table_path			big object path to be opened.
   * @param 	flag		option to open a file; O_CREAT to create new file
   * @param	tbl_sub_path		sub path.
   * @return	>= 0 if succeed, otherwise -1.
   */
  int open(const string& table_path, const std::string& db_path, const string& tbl_sub_path, int cc, int flags);

  int close();

  int initMetaData();

 const vector<uint32_t>& getIdxForValidCols() const {
  return idx_for_valid_cols_;
 }

  /**
   * @brief	obtain the physical address of based on the offset.
   *
   * @param 	offset	offset in the data section.
   * @return	memory address of the data
   */
  void* addr(off_t offset) const { return (void*) ((intptr_t) bt_file_.memAddr() + offset); }

  void initSection();

  int memExtend(off_t offset = 0, size_t ps = kwdbts::EngineOptions::pageSize());

  std::string filePath() const { return bt_file_.filePath(); }
  std::string realFilePath() const { return bt_file_.realFilePath(); }

  /*
   * --------------------------------------------------------------
   * functions related to meta data information.
   * --------------------------------------------------------------
   */
  void setStatus(int status) { meta_data_->status = status; }

  int status() const { return meta_data_->status; }

  /**
   * @brief	obtain the hierarchy attributes series.
   *
   * @param	attr		the hierarchy attribute.
   * @return	1 if succeeds; 0 otherwise.
   */
  const vector<AttributeInfo>& colsInfoWithHidden() const;

  /**
   * @brief	check if a big object's structure type.
   *
   * @return	structure type of a big object; available types are: ST_VTREE, ST_VTREE_LINK,
   * 					ST_DATA, ST_DATA_LINK, ST_SUPP, ST_SUPP_LINK
   */
  int structType() const { return (bt_file_.memAddr()) ? meta_data_->struct_type : 0; }

  /**
   * @brief	obtain version of the big object
   *
   * @return 	data object version.
   */
  int version() const { return (bt_file_.memAddr()) ? meta_data_->schema_version : 0; }

  /**
 * @brief	obtain level of attributes
 *
 * @return  total number of attributes level in a big object.
 */
  int level() const { return (bt_file_.memAddr()) ? meta_data_->cols_num : 0; }

  int encoding() const { return meta_data_->encoding; }

  TSTableFileMetadata* metaData() { return meta_data_; }

  void setDropped() { meta_data_->is_dropped = true; }

  void setNotDropped() { meta_data_->is_dropped = false; }

  void setDBid(uint32_t id) { meta_data_->db_id = id; }

  bool isDropped() { return meta_data_->is_dropped; }

  int getColumnIndex(const AttributeInfo& attr_info);

  int getColumnIndex(const uint32_t& col_id);
};
