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
#include "ee_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_dynamic_hash_index.h"
#include "ee_batch_data_container.h"

namespace kwdbts {

class RelTagRowBatch;
typedef std::shared_ptr<RelTagRowBatch> RelTagRowBatchPtr;

// wrap HashTagScan result as tag row batch for multiple model processing
class RelTagRowBatch : public TagRowBatch {
 public:
  RelTagRowBatch();
  ~RelTagRowBatch();

  /**
   * read count
   */
  k_uint32 Count() override {
    return rel_tag_data_.size();
  }
  KStatus GetTagData(TagData *tagData, void **bitmap, k_uint32 line) override;
  void Init(TABLE *table) override;
  /**
   * @brief Add new rows joining matched build side and probe side data to output for primaryHashTagScan mode
   * @param[in] ctx kwdb context
   * @param[in] rel_data_chunk  probe side relational data chunk
   * @param[in] rel_row_index row index in data chunk used to probe
   * @param[in] tag_data_chunk build side primary tag data
   */
  KStatus AddPrimaryTagRelJoinRecord(kwdbContext_p ctx, DataChunkPtr &rel_data_chunk,
                              k_uint32 rel_row_index, DataChunkPtr &tag_data_chunk);
  /**
   * @brief Add new rows joining matched build side (tag) and probe side (relational) data to output
   * @param[in] ctx kwdb context
   * @param[in] batch_data_container batch container keeping build side data
   * @param[in] rel_data_chunk  relational data chunk
   * @param[in] rel_row relation row index in data chunk
   * @param[in] row_indice  first row indice matched in build side
   */
  KStatus AddTagRelJoinRecord(kwdbContext_p ctx, BatchDataContainerPtr batch_data_container,
                              DataChunkPtr &rel_data_chunk, k_uint32 rel_row, RowIndice row_indice);
  /**
   * @brief Add new rows joining matched build side (relational) and probe side (tag) data to output
   * @param[in] ctx kwdb context
   * @param[in] batch_data_container batch container keeping build side data
   * @param[in] rel_data_chunk  relational data chunk
   * @param[in] rel_row relation row index in data chunk
   * @param[in] row_indice  first row indice matched in build side
   */
  KStatus AddRelTagJoinRecord(kwdbContext_p ctx, BatchDataContainerPtr batch_data_container,
                              DataChunkPtr &rel_data_chunk, k_uint32 rel_row, RowIndice& row_indice);

 private:
  // all tag data rows including relational data
  std::vector<TagData> rel_tag_data_;
  // all the allocated buffer to store relational and tag data
  std::vector<DatumPtr> data_ptrs_;
  // relational data len for each row
  k_uint32 rel_data_len_{0};
  // tag data len for each row
  k_uint32 tag_data_len_{0};
};

};  // namespace kwdbts
