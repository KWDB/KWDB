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

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_scan_row_batch.h"
#include "ee_window_parser.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"

namespace kwdbts {

class WindowerSpec;
class TABLE;

class KWDBPostProcessSpec;

/**
 * @brief   window operator
 *
 * @author  lxp
 */
class WindowOperator : public BaseOperator {
 public:
  enum Rowmark { ROW_NEED_SKIP = -1, ROW_NO_NEED_SKIP = -2 };

  WindowOperator(TsFetcherCollection* collection,
                 WindowerSpec* spec, PostProcessSpec* post, TABLE* table,
                 int32_t processor_id);
  WindowOperator(const WindowOperator&, int32_t processor_id);
  ~WindowOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_WINDOWS;}

  BaseOperator* Clone() override;

 protected:
  k_bool CheckNewPartition(RowBatch* row_batch);
  void ResolvePartionByCols(kwdbContext_p ctx);
  void ResolveWindowsFuncs(kwdbContext_p ctx);
  void ProcessDataValue(kwdbContext_p ctx, RowBatch* row_batch, k_uint32 col,
                        Field* field, k_bool is_diff_func, k_bool diff_first);
  EEIteratorErrCode DoFilter(kwdbContext_p ctx);
  KStatus AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch);
  inline k_int32 SkipRowCount(k_int32 row);
  void ProcessFilterCols(k_bool ischunk);
  EEIteratorErrCode ResolveWinTempFields(kwdbContext_p ctx);
  k_int32 PreComputeRowIndex(RowBatch* row_batch);

 protected:
  k_int32 row_start_index_{ROW_NEED_SKIP};
  k_uint32 col_size_{0};
  k_uint32 count_index_{0};
  k_uint64 limit_{0};
  k_uint32 examined_rows_{0};
  k_bool is_new_partition_{false};  // for diff
  k_bool first_check_{false};
  WindowerSpec* spec_;
  TsWindowParser param_;
  // ScanRowBatch* row_batch_{nullptr};
  // partitionby col
  Field* filter_{nullptr};
  std::vector<k_uint32> partition_cols_;
  std::vector<GroupByColumnInfo> group_by_cols_;
  ColumnInfo* output_filter_col_info_{nullptr};
  k_int32 output_filter_col_num_{0};
  std::vector<Field*> filter_fields_;
  std::vector<k_uint32> filter_fields_num_;
  std::vector<Field*> win_func_fields_;
  std::vector<Field*> win_func_output_fields_;
};

}  // namespace kwdbts
