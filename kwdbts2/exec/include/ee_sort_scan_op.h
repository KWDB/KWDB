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
#include <queue>
#include <vector>

#include "ee_data_chunk.h"
#include "ee_global.h"
#include "ee_pb_plan.pb.h"
#include "ee_memory_data_container.h"
#include "ee_scan_op.h"
#include "ee_sort_flow_spec.h"
#include "kwdb_type.h"

namespace kwdbts {

struct Data {
  k_int64 ts_{0};
  k_int32 rowno_{-1};
};

class Greater {
 public:
  bool operator()(const Data *l, const Data *r) {
    return l->ts_ > r->ts_;
  }
};

class Less {
 public:
  bool operator()(const Data *l, const Data *r) {
    return l->ts_ < r->ts_;
  }
};

class SortScanOperator : public TableScanOperator {
 public:
  SortScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, TSPostProcessSpec* post, TABLE* table,
                   BaseOperator* input, int32_t processor_id);

  SortScanOperator(const SortScanOperator&, BaseOperator* input,
                   int32_t processor_id);

  ~SortScanOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

 private:
  EEIteratorErrCode mallocTempField(kwdbContext_p ctx);
  EEIteratorErrCode InitTmpOutputColInfo(kwdbContext_p ctx);
  EEIteratorErrCode initContainer(kwdbContext_p ctx, DataChunkPtr&chunk,
                        ColumnInfo* output_col_info, k_int32 output_col_num_, k_uint32 line);
  EEIteratorErrCode ResolveFilter(kwdbContext_p ctx, ScanRowBatch* row_batch);
  EEIteratorErrCode PrioritySort(kwdbContext_p ctx, ScanRowBatch* row_batch, k_uint32 limit);

 protected:
  Field* order_field_{nullptr};
  uint64_t ts_{INT64_MAX};
  std::priority_queue<Data*, std::vector<Data*>, Greater> data_desc_;
  std::priority_queue<Data*, std::vector<Data*>, Less> data_asc_;
  DataChunkPtr data_chunk_;
  Field **tmp_renders_{nullptr};
  std::vector<Field*> tmp_output_fields_;
  Field *new_field_{nullptr};
  ColumnInfo *tmp_output_col_info_{nullptr};
  bool is_offset_opt_{false};
};

}  // namespace kwdbts
