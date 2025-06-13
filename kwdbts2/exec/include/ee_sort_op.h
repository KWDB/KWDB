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

#include <vector>
#include <memory>
#include <queue>

#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_sort_flow_spec.h"
#include "kwdb_type.h"
#include "ee_memory_data_container.h"
#include "ee_disk_data_container.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

class SortOperator : public BaseOperator {
 public:
  SortOperator(TsFetcherCollection* collection, BaseOperator* input, TSSorterSpec* spec,
                              TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  SortOperator(const SortOperator&, BaseOperator* input, int32_t processor_id);

  ~SortOperator() override;

  /**
   * Inherited from BaseIterator
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

 protected:
  KStatus ResolveSortCols(kwdbContext_p ctx);

  TSSorterSpec* spec_;
  TSPostProcessSpec* post_;
  SortSpecParam param_;
  BaseOperator* input_;

  k_uint32 limit_{0};
  k_uint32 offset_{0};
  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
  k_uint32 scanned_rows_{0};

  // sort info
  std::vector<ColumnOrderInfo> order_info_;

  // sort container
  DataContainerPtr container_;

  // input (FieldNum)
  std::vector<Field*>& input_fields_;

  bool is_done_{false};
  bool is_mem_container_{true};   // sort type

  ColumnInfo* input_col_info_{nullptr};
  k_int32 input_col_num_{0};

 private:
  static const k_uint64 SORT_MAX_MEM_BUFFER_SIZE = BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE;
};

}  // namespace kwdbts
