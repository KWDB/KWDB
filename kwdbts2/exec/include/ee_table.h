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
#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "ee_encoding.h"
#include "ee_global.h"
#include "ee_pb_plan.pb.h"
#include "ee_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ts_common.h"

/**********************************
 * @brief    This file is a filter implementation of a virtual table, and
 *           various fields as well as projections
 *
 * @author   liguoliang
 *********************************/

namespace kwdbts {

class Handler;
class Field;
class TABLE {
 public:
  TABLE(KDatabaseId schemaID, KTableId objectID)
      : schema_id_(schemaID), object_id_(objectID) {}

  ~TABLE();

  TABLE(const TABLE &) = delete;
  TABLE &operator=(const TABLE &) = delete;
  TABLE(TABLE &&) = delete;
  TABLE &operator=(TABLE &&) = delete;

  /**
   * @brief  init
   *
   */
  KStatus Init(kwdbContext_p ctx, const TSTagReaderSpec *spec);

  /**
   * @brief get field count
   *
   * @return int
   */
  inline k_uint32 FieldCount() const { return field_num_; }

  inline k_uint32 DataCount() const { return field_num_ - tag_num_; }

  inline k_uint32 TagCount() const { return tag_num_; }

  inline void SetAccessMode(k_uint32 mode) { access_mode_ = mode; }
  k_uint32 GetAccessMode() const { return access_mode_; }

  void *GetData(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt);
  k_uint16 GetDataLen(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype);
  bool is_nullable(int col, roachpb::KWDBKTSColumn::ColumnType ctype);
  k_bool IsOverflow(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype);
  void *GetData2(int col, k_uint64 offset, roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt);
  bool is_nullable2(int col, roachpb::KWDBKTSColumn::ColumnType ctype);

  /**
   * @brief Get the Field With Col Num object
   *
   * @param num
   * @return Field*
   */
  Field *GetFieldWithColNum(k_uint32 num);
  k_uint32 GetMinTagId() { return min_tag_id_; }
  k_uint32 GetTagNum() { return tag_num_; }
  void SetTableVersion(k_uint32 version) { table_version_ = version; }
  // get relational fields for multiple model processing
  vector<Field*>& GetRelFields();
  // get relational and tag joining columns for multiple model processing
  std::vector<pair<k_uint32, k_uint32>>& GetRelTagJoinColumnIndexes();
  // get scan tags for multiple model processing
  std::vector<k_uint32>& GetScanTags();
  void SetOnlyTag(bool only_tag) { only_tag_ = only_tag; }

 private:
  KStatus InitField(kwdbContext_p ctx, const TSCol &col, k_uint32 index,
                    Field **field);

 public:
  KDatabaseId schema_id_;
  KTableId object_id_;
  k_uint32 field_num_{0};
  Field **fields_{nullptr};
  k_uint32 access_mode_{0};
  k_uint32 min_tag_id_{0};
  k_uint32 tag_num_{0};
  std::vector<k_uint32> scan_cols_;
  std::vector<k_uint32> scan_tags_;
  std::vector<k_int32> agg_extends_;
  // relational scan cols for multiple model processing
  std::vector<k_uint32> scan_rel_cols_;
  std::vector<Sumfunctype> scan_agg_types_;
  std::vector<Sumfunctype> scan_real_agg_types_;
  std::vector<k_int32> statistic_col_fix_idx_;  // for statistic
  std::vector<uint32_t> hash_points_;
  std::unordered_map<uint32_t, std::vector<KwTsSpan>> hash_points_spans_;
  std::vector<k_int64>
      scan_last_ts_points_;  // scan_last_ts_points_ and scan_real_agg_types_
                             // remain align
  std::vector<k_int64>
      scan_real_last_ts_points_;  // scan_last_ts_points_ and scan_real_agg_types_
                             // remain align
  k_uint32 table_version_{0};
  bool is_reverse_{0};
  bool only_tag_{false};
  k_int32 ptag_size_{0};
  bool ordered_scan_{false};
  bool optimize_offset_{false};
  k_uint32 offset_{0};
  k_uint32 limit_{0};
  bool contain_tag_for_statistic{false};  // for statistic last_row

 protected:
  // relational fields for multiple model processing
  vector<Field*> rel_fields_;
  // join columns for multiple model processing
  std::vector<pair<k_uint32, k_uint32>> rel_tag_join_column_indexes_;

 private:
  k_bool init_{KFALSE};
};

}  // namespace kwdbts
