// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "ee_base_op.h"
#include "ee_data_chunk.h"
#include "ee_disk_data_container.h"
#include "ee_global.h"
#include "ee_heap_sort_container.h"
#include "ee_memory_data_container.h"
#include "ee_pb_plan.pb.h"
#include "ee_sort_parser.h"
#include "kwdb_type.h"

namespace kwdbts {

enum EESortType {
  EE_SORT_MEMORY = 0,  // Use in-memory sorting when the total memory of
                       // datachunk < SORT_MAX_MEM_BUFFER_SIZE
  EE_SORT_HEAP,        // Use heap sorting when (limit + offset) * row_size <
                       // SORT_MAX_MEM_BUFFER_SIZE
  EE_SORT_DISK  // Use external sorting when the total memory of datachunk >=
                // SORT_MAX_MEM_BUFFER_SIZE
};

class SortOperator : public BaseOperator {
 public:
  SortOperator(TsFetcherCollection* collection, TSSorterSpec* spec,
               TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  SortOperator(const SortOperator&, int32_t processor_id);

  ~SortOperator() override;

  /**
   * Inherited from BaseIterator
   */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

  enum OperatorType Type() override { return OperatorType::OPERATOR_ORDER_BY; }

 protected:
  KStatus ResolveSortCols(kwdbContext_p ctx);

  TSSorterSpec* spec_;
  TsSortParser param_;

  k_uint32 limit_{0};
  k_uint32 offset_{0};
  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
  k_uint32 scanned_rows_{0};

  // sort info
  std::vector<ColumnOrderInfo> order_info_;

  // sort container
  DataContainerPtr container_;

  EESortType sort_type_{EE_SORT_MEMORY};  // sort type

  ColumnInfo* input_col_info_{nullptr};
  k_int32 input_col_num_{0};

 private:
  static const k_uint64 SORT_MAX_MEM_BUFFER_SIZE = BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE;
};

}  // namespace kwdbts
