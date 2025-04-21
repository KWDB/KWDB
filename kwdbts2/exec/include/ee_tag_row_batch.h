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

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ee_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"

namespace kwdbts {

class TABLE;
class TagRowBatch;
typedef std::shared_ptr<TagRowBatch> TagRowBatchPtr;
typedef std::vector<TagRawData> TagData;
struct TagSelection {
  k_uint32 entity_{0};
  k_uint32 batch_{0};
  k_uint32 line_{0};
};

#define PRIMARY_TAGS_EXTERN_STORAGE_LENGTH 8

class TagRowBatch : public RowBatch {
 public:
  ResultSet res_;
  k_uint32 count_{0};
  std::vector<EntityResultIndex> entity_indexs_;
  k_uint32 effect_count_{0};
  bool isFilter_{false};
  k_uint32 current_pipe_no_{0};  // Record the device index ID,-1 means that you
                                 // need to take it again from storage next time

 protected:
  k_uint32 current_entity_{0};
  k_uint32 current_batch_no_{0};
  k_uint32 current_batch_line_{0};
  k_uint32 current_line_{0};
  k_uint32 tag_col_offset_{0};
  std::vector<k_uint32> tag_offsets_;
  k_uint32 bitmap_offset_{0};
  k_uint32 current_pipe_line_{0};
  std::vector<k_uint32> pipe_entity_num_;  // Record the number of devices
                                           // allocated to each thread
  k_uint32 valid_pipe_no_{0};

  std::unordered_map<k_uint32, std::vector<k_uint32>> hash_entity_indexs_;

  std::vector<TagSelection> selection_;
  TABLE *table_{nullptr};

 public:
  TagRowBatch() {
    typ_ = RowBatchType::RowBatchTypeTag;
  }
  ~TagRowBatch() {}
  char *GetData(k_uint32 col, k_uint32 offset,
                roachpb::KWDBKTSColumn::ColumnType ctype,
                roachpb::DataType dt) override;
  // get current_entity_ for multiple model processing
  k_uint32 GetCurrentEntity() {
    return current_entity_;
  }
  k_uint16 GetDataLen(k_uint32 col, k_uint32 offset,
                      roachpb::KWDBKTSColumn::ColumnType ctype) override;
  void Reset();

  virtual bool IsNull(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype);
  /**
   * read count
   */
  k_uint32 Count() {
    if (isFilter_) {
      return effect_count_;
    } else {
      return count_;
    }
  }
  /**
   *  Move the cursor to the next line, default 0
   */
  k_int32 NextLine();
  /**
   *  Move the input cursor to the next line
   */
  KStatus NextLine(k_uint32 *line);
  /**
   *  Move the cursor to the first line
   */
  void ResetLine();

  /**
   *  get entityid by line
   */
  EntityResultIndex& GetEntityIndex(k_uint32 line) {
    // if (isFilter_) {
      // return entity_indexs_[selection_[line].entity_];
    // } else {
    return entity_indexs_[line];
    // }
  }

  /**
   *  get entityid by current line for multiple model processing
   */
  EntityResultIndex& GetCurrentEntityIndex() {
    return entity_indexs_[current_entity_];
  }

  void AddSelection() {
    // entity_indexs_per_pipe_.emplace_back(entity_indexs_[current_entity_]);
    selection_.push_back(
        {current_entity_, current_batch_no_, current_batch_line_});
    effect_count_++;
  }

  // remove selection for multiple model processing
  void RemoveSelection() {
    selection_.erase(selection_.begin() + current_line_);
    effect_count_--;
    if (current_line_ < selection_.size()) {
      current_entity_ = selection_[current_line_].entity_;
      current_batch_no_ = selection_[current_line_].batch_;
      current_batch_line_ = selection_[current_line_].line_;
    }
  }

  KStatus Sort(Field **renders, const std::vector<k_uint32> &cols,
               const std::vector<k_int32> &order_type) {
    return FAIL;
  }
  void SetTagToColOffset(k_uint32 offset) { tag_col_offset_ = offset; }
  void SetBitmapOffset(k_uint32 offset) { bitmap_offset_ = offset; }
  virtual KStatus GetTagData(TagData *tagData, void **bitmap, k_uint32 line);
  // get current tag data for multiple model processing
  virtual KStatus GetCurrentTagData(TagData *tagData, void **bitmap);
  virtual void Init(TABLE *table);
  void SetLimitOffset(k_uint32 limit, k_uint32 offset) {}
  void SetPipeEntityNum(kwdbContext_p ctx, k_uint32 pipe_degree);
  KStatus GetEntities(std::vector<EntityResultIndex> *entities);
  bool isAllDistributed();

  inline bool EntityLessThan(TagSelection &a, TagSelection &b) {
    EntityResultIndex &x = entity_indexs_[a.entity_];
    EntityResultIndex &y = entity_indexs_[b.entity_];
    if (x.entityGroupId == y.entityGroupId) {
      if (x.subGroupId == y.subGroupId) {
        return x.entityId < y.entityId;
      }
      return x.subGroupId < y.subGroupId;
    }
    return x.subGroupId < y.subGroupId;
  }
  KStatus SortByEntityIndex();
};

};  // namespace kwdbts
